#include "wcarplayer.hh"
#include "wcarplayerconfig.hh"
#include "wcartransform.hh"
#include <maxbase/stopwatch.hh>
#include <maxsimd/canonical.hh>
#include <iostream>

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

    for (const auto& event : storage)
    {
        auto sql = maxsimd::canonical_args_to_sql(*event.sCanonical, event.canonical_args);
//        std::cout << "sql = " << sql << std::endl;
        if ((++count % 251) == 0)
        {
            std::cout << "\r" << count << std::flush;
        }

        // TODO: Ignoring the return value until erranous queries are handled.
        // If the sql was in fact wrong during capture, "selct 42", the
        // same "selct 42" will run here.
        execute_stmt(m_config.pConn, sql);
    }

    std::cout << "\r" << count << std::endl;
}
