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
    RepPlayer player(&config);

    if (config.mode == RepConfig::Mode::REPLAY)
    {
        player.replay();
    }

    return EXIT_SUCCESS;
}
catch (std::exception& ex)
{
    std::cerr << "Error: " << ex.what() << std::endl;
    return EXIT_FAILURE;
}
