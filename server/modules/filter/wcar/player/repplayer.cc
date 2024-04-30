#include "repplayer.hh"
#include "../simtime.hh"
#include <maxbase/stopwatch.hh>
#include <maxbase/assert.hh>
#include <maxsimd/canonical.hh>
#include <iostream>
#include <unordered_map>

RepPlayer::RepPlayer(const RepConfig* pConfig)
    : m_config(*pConfig)
    , m_transform(&m_config)
    , m_front_trxn(begin(m_transform.transactions()))
    , m_recorder(std::make_unique<RecorderContext>(&m_transform.rep_event_storage()),
                 m_transform.max_parallel_sessions())
    , m_threadpool(m_transform.max_parallel_sessions())
{
}

RepPlayer::~RepPlayer()
{
}

void RepPlayer::replay()
{
    bool once = true;
    m_recorder.start();

    start_deadlock_monitor(m_transform.max_parallel_sessions(),
                           m_config.user, m_config.password,
                           m_config.host.address(), m_config.host.port());

    // TODO: add throttling. This loop will now schedule all events, i.e. everything
    // that cannot be executed now, will go to pending, potentially consuming all memory.
    for (auto&& qevent : m_transform.player_storage())
    {
        if (once)
        {
            once = false;
            SimTime::reset_sim_time(qevent.start_time, m_config.sim_speed);
            m_stopwatch.restart();
        }

        auto session_ite = m_sessions.find(qevent.session_id);

        if (session_ite == end(m_sessions))
        {
            auto ins = m_sessions.emplace(qevent.session_id,
                                          std::make_unique<RepSession>(&m_config, this,
                                                                       qevent.session_id,
                                                                       &m_recorder,
                                                                       m_threadpool));
            session_ite = ins.first;
        }

        timeline_add(*(session_ite->second), std::move(qevent));
    }

    MXB_SNOTICE("Main loop: " << mxb::to_string(m_stopwatch.restart()));
    wait_for_sessions_to_finish();
    MXB_SNOTICE("Final wait: " << mxb::to_string(m_stopwatch.restart()));

    m_recorder.stop();
    m_transform.finalize();
    stop_deadlock_monitor();
    MXB_SNOTICE("Transform finalize: " << mxb::to_string(m_stopwatch.restart()));

    for (auto str : mxb::strtok(mxb::get_collector_stats(), "\n"))
    {
        MXB_SNOTICE(str);
    }
}

RepPlayer::ExecutionInfo RepPlayer::get_execution_info(RepSession& session, const QueryEvent& qevent)
{
    ExecutionInfo exec {false, end(m_transform.transactions())};
    auto trx_start_ite = m_transform.trx_start_mapping(qevent.event_id);

    if (m_front_trxn == end(m_transform.transactions()))
    {
        exec.can_execute = true;
    }
    else if (session.in_trxn())
    {
        exec.can_execute = qevent.event_id <= session.commit_event_id();
    }
    else
    {
        if (trx_start_ite != end(m_transform.transactions()))
        {
            exec.can_execute = qevent.start_time < m_front_trxn->end_time;
        }
        else
        {
            exec.can_execute = true;
        }
    }

    if (exec.can_execute)
    {
        exec.trx_start_ite = trx_start_ite;
    }

    return exec;
}

void RepPlayer::trxn_finished(int64_t event_id)
{
    std::lock_guard lock(m_trxn_mutex);
    m_finished_trxns.insert(event_id);
    m_trxn_condition.notify_one();
}

void RepPlayer::session_finished(const RepSession& session)
{
    std::lock_guard lock(m_session_mutex);
    m_finished_sessions.insert(session.session_id());
    m_session_condition.notify_one();
}

void RepPlayer::timeline_add(RepSession& session, QueryEvent&& qevent)
{
    SimTime::sim_time().tick();

    mxb::Duration dur = qevent.start_time - SimTime::sim_time().now();
    std::unique_lock lock(m_trxn_mutex, std::defer_lock);

    if (m_config.sim_speed > 0 && dur > 0s)
    {
        mxb::TimePoint wait_until = mxb::Clock::now() + dur;
        bool at_least_once = true;

        while (mxb::Clock::now() < wait_until || at_least_once)
        {
            at_least_once = false;

            lock.lock();
            m_trxn_condition.wait_until(lock, wait_until, [this]{
                return !m_finished_trxns.empty();
            });

            schedule_pending_events(lock);
        }
    }
    else
    {
        lock.lock();
        schedule_pending_events(lock);
    }

    schedule_event(session, std::move(qevent));
}

void RepPlayer::schedule_event(RepSession& session, QueryEvent&& qevent)
{
    if (session.has_pending_events())
    {
        session.add_pending(std::move(qevent));
    }
    else if (auto exec = get_execution_info(session, qevent); exec.can_execute)
    {
        if (exec.trx_start_ite != m_transform.transactions().end())
        {
            session.queue_query(std::move(qevent), exec.trx_start_ite->end_event_id);
        }
        else
        {
            session.queue_query(std::move(qevent));
        }
    }
    else
    {
        session.add_pending(std::move(qevent));
    }
}

bool RepPlayer::schedule_pending_events(std::unique_lock<std::mutex>& lock)
{
    // m_trxn_mutex is locked at this point.
    std::unordered_set<int64_t> finished_trxns;
    finished_trxns.swap(m_finished_trxns);
    lock.unlock();

    mark_completed_trxns(finished_trxns);

    // Greedy scheduling. Not necessarily "fair".
    bool has_more = false;
    for (auto& p : m_sessions)
    {
        auto& session = *p.second;
        while (session.has_pending_events())
        {
            if (auto exec = get_execution_info(session, session.front_pending()); exec.can_execute)
            {
                if (exec.trx_start_ite != end(m_transform.transactions()))
                {
                    session.queue_front_pending(exec.trx_start_ite->end_event_id);
                }
                else
                {
                    session.queue_front_pending();
                }
            }
            else
            {
                break;
            }
        }

        has_more = has_more || session.has_pending_events();
    }

    return has_more;
}

void RepPlayer::wait_for_sessions_to_finish()
{
    bool more_pending = true;
    while (!m_sessions.empty())
    {
        if (more_pending)
        {
            std::unique_lock lock(m_trxn_mutex);
            more_pending = schedule_pending_events(lock);
            std::this_thread::yield();      // TODO no condition to wait on here
        }
        else
        {
            std::unique_lock lock(m_session_mutex);
            m_session_condition.wait(lock, [this]{
                return !m_finished_sessions.empty();
            });
        }

        remove_finished_sessions();
    }

    mxb_assert(m_front_trxn == end(m_transform.transactions()));
}

void RepPlayer::mark_completed_trxns(const std::unordered_set<int64_t>& finished_trxns)
{
    for (auto end_event_id : finished_trxns)
    {
        auto trx_ite = m_transform.trx_end_mapping(end_event_id);
        mxb_assert(trx_ite != end(m_transform.transactions()));
        trx_ite->completed = true;

        auto session_ite = m_sessions.find(trx_ite->session_id);
        mxb_assert(session_ite != end(m_sessions));
        session_ite->second->reset_commit_event_id();
    }

    // Move m_front_trxn forwards, until one is found that has not completed yet.
    while (m_front_trxn != end(m_transform.transactions()))
    {
        if (!m_front_trxn->completed)
        {
            break;
        }

        ++m_front_trxn;
    }
}

void RepPlayer::remove_finished_sessions()
{
    std::unordered_set<int64_t> finished_sessions;

    std::unique_lock lock(m_session_mutex);
    finished_sessions.swap(m_finished_sessions);
    lock.unlock();

    for (auto session_id : finished_sessions)
    {
        m_sessions.erase(session_id);
    }
}
