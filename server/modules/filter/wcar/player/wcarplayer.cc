#include "wcarplayer.hh"
#include <maxbase/stopwatch.hh>
#include <maxsimd/canonical.hh>
#include <iostream>
#include <unordered_map>

Player::Player(const PlayerConfig* pConfig)
    : m_config(*pConfig)
    , m_transform(&m_config)
    , m_front_trxn(begin(m_transform.transactions()))
{
}

maxbase::TimePoint Player::sim_time()
{
    return mxb::Clock::now() - m_timeline_delta;
}

void Player::replay()
{
    for (auto&& qevent : m_transform.player_storage())
    {
        if (m_timeline_delta == mxb::Duration::zero())
        {
            m_timeline_delta = mxb::Clock::now() - qevent.start_time;
        }

        auto session_ite = m_sessions.find(qevent.session_id);

        if (session_ite == end(m_sessions))
        {
            auto ins = m_sessions.emplace(qevent.session_id,
                                          std::make_unique<PlayerSession>(&m_config, this,
                                                                          qevent.session_id));
            session_ite = ins.first;
        }

        session_ite->second->queue_query(std::move(qevent), -1);
    }
}

void Player::trxn_finished(int64_t event_id)
{
    std::lock_guard lock(m_trxn_mutex);
    m_finished_trxns.insert(event_id);
    m_trxn_condition.notify_one();
}

void Player::session_finished(const PlayerSession& session)
{
    std::lock_guard lock(m_session_mutex);
    m_finished_sessions.insert(session.session_id());
}
