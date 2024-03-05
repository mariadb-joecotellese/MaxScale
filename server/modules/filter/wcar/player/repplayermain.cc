/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "repplayer.hh"
#include <iostream>
#include <maxbase/maxbase.hh>

int main(int argc, char** argv)
try
{
    mxb::MaxBase mxb(MXB_LOG_TARGET_STDOUT);

    RepConfig config(argc, argv);

    if (config.mode == RepConfig::Mode::REPLAY)
    {
        RepPlayer player(&config);
        player.replay();
    }
    else if (config.mode == RepConfig::Mode::TRANSFORM)
    {
        RepTransform transform(&config);
    }
    else
    {
        // The code should never end up here: RepConfig::RepConfig exits on invalid options
        mxb_assert(!true);
    }

    return EXIT_SUCCESS;
}
catch (std::exception& ex)
{
    std::cerr << "Error: " << ex.what() << std::endl;
    return EXIT_FAILURE;
}
