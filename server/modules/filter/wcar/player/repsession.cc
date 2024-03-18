#include "repsession.hh"
#include "repplayer.hh"
#include <maxbase/stopwatch.hh>
#include <maxbase/assert.hh>
#include <maxbase/string.hh>
#include <maxsimd/canonical.hh>
#include <iostream>
#include <thread>

namespace
{
namespace ThisUnit
{
std::atomic<int> next_thread_idx = 0;
thread_local int thread_idx = -1;
}
}

bool execute_stmt(MYSQL* pConn, const QueryEvent& qevent, RepRecorder* pRecorder)
{
    auto sql = maxsimd::canonical_args_to_sql(*qevent.sCanonical, qevent.canonical_args);

    RepEvent revent;
    revent.event_id = qevent.event_id;
    revent.start_time = mxb::Clock::now();
    revent.num_rows = 0;

    if (mysql_query(pConn, sql.c_str()))
    {
        std::cerr << "MariaDB: Error S " << qevent.session_id << " E " << qevent.event_id
                  << " SQL " << mxb::show_some(sql)
                  << " Error code " << mysql_error(pConn) << std::endl;
        return false;
    }

    while (MYSQL_RES* result = mysql_store_result(pConn))
    {
        revent.num_rows += mysql_num_rows(result);
        mysql_free_result(result);
    }

    revent.end_time = mxb::Clock::now();

    pRecorder->get_shared_data_by_index(ThisUnit::thread_idx)->send_update(std::move(revent));

    return true;
}

RepSession::RepSession(const RepConfig* pConfig,
                       RepPlayer* pPlayer,
                       int64_t session_id,
                       RepRecorder* pRecorder,
                       maxbase::ThreadPool& tpool)
    : m_config(*pConfig)
    , m_player(*pPlayer)
    , m_session_id(session_id)
    , m_pRecorder(pRecorder)
{
    auto name = "rep-" + std::to_string(session_id);
    m_future = tpool.async(name, [this]{
        run();
    });
}

void RepSession::stop()
{
    m_running.store(false, std::memory_order_relaxed);
}

RepSession::~RepSession()
{
    stop();
    m_future.get();
}

void RepSession::queue_query(QueryEvent&& qevent, int64_t commit_event_id)
{
    if (commit_event_id != -1)
    {
        mxb_assert(m_commit_event_id == -1);
        m_commit_event_id = commit_event_id;
    }

    std::lock_guard guard(m_mutex);
    m_queue.push_back(std::move(qevent));
    m_condition.notify_one();
}

void RepSession::run()
{
    if (ThisUnit::thread_idx == -1)
    {
        ThisUnit::thread_idx = ThisUnit::next_thread_idx.fetch_add(1, std::memory_order_relaxed);
    }

    m_pConn = mysql_init(nullptr);
    if (m_pConn == nullptr)
    {
        std::cerr << "Could not initialize connector-c " << mysql_error(m_pConn) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (mysql_real_connect(m_pConn, m_config.host.address().c_str(), m_config.user.c_str(),
                           m_config.password.c_str(), "", m_config.host.port(), nullptr, 0) == nullptr)
    {
        std::cerr << "Could not connect to " << m_config.host.address()
                  << ':' << std::to_string(m_config.host.port())
                  << " Error: " << mysql_error(m_pConn) << '\n';
        exit(EXIT_FAILURE);
    }

    while (m_running.load(std::memory_order_relaxed))
    {
        std::unique_lock lock(m_mutex);
        m_condition.wait(lock, [this]{
            return !m_queue.empty();
        });

        auto qevent = std::move(m_queue.front());
        m_queue.pop_front();
        lock.unlock();

        if (is_session_close(qevent))
        {
            m_player.session_finished(*this);
            break;
        }
        else
        {
            execute_stmt(m_pConn, qevent, m_pRecorder);
            if (qevent.event_id == m_commit_event_id)
            {
                auto rep = m_commit_event_id;
                m_player.trxn_finished(rep);
            }
        }
    }

    mysql_close(m_pConn);
    m_player.session_finished(*this);
}
