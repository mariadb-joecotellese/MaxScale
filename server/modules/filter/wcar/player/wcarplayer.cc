#include "wcarplayer.hh"
#include "wcarplayerconfig.hh"
#include "wcartransform.hh"
#include "wcarplayersession.hh"
#include <maxbase/stopwatch.hh>
#include <maxsimd/canonical.hh>
#include <iostream>
#include <unordered_map>

Player::Player(const PlayerConfig* pConfig)
    : m_config(*pConfig)
{
}

maxbase::TimePoint Player::sim_time()
{
    return mxb::Clock::now() - m_timeline_delta;
}

void Player::replay()
{
    mxb::StopWatch sw;
    Transform xform(&m_config);

    Storage& storage = xform.player_storage();

    int64_t count = 0;
    std::unordered_map<int64_t, std::unique_ptr<PlayerSession>> sessions;

    for (auto&& qevent : storage)
    {
        if (m_timeline_delta == mxb::Duration::zero())
        {
            m_timeline_delta = mxb::Clock::now() - qevent.start_time;
        }

        auto ite = sessions.find(qevent.session_id);

        if ((++count % 251) == 0)
        {
            std::cout << "\r" << count << std::flush;
        }

        if (ite == end(sessions))
        {
            auto ins = sessions.emplace(qevent.session_id,
                                        std::make_unique<PlayerSession>(&m_config, this, qevent.session_id));
            ite = ins.first;
        }

        ite->second->queue_query(std::move(qevent), -1);
    }

    std::cout << "\r" << count << std::endl;
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
