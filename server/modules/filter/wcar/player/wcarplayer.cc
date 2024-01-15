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

void Player::replay()
{
    mxb::StopWatch sw;
    Transform xform(&m_config);

    Storage& storage = xform.player_storage();

    int64_t count = 0;
    std::unordered_map<int64_t, std::unique_ptr<PlayerSession>> sessions;

    for (const auto& event : storage)
    {
        auto sql = maxsimd::canonical_args_to_sql(*event.sCanonical, event.canonical_args);

        auto ite = sessions.find(event.session_id);

        if (ite != end(sessions) && event.start_time == event.end_time)
        {
            ite->second->stop();
            continue;
        }

        if ((++count % 251) == 0)
        {
            std::cout << "\r" << count << std::flush;
        }

        if (ite == end(sessions))
        {
            auto ins = sessions.emplace(event.session_id,
                                        std::make_unique<PlayerSession>(&m_config, this, event.session_id));
            ite = ins.first;
        }

        ite->second->queue_query(sql);
    }

    std::cout << "\r" << count << std::endl;
}
