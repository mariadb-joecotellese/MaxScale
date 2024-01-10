/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarplayer.hh"
#include <iostream>

int main(int argc, char** argv)
try
{
    PlayerConfig config(argc, argv);
    Player player(&config);

    player.replay();

    return EXIT_SUCCESS;
}
catch (std::exception& ex)
{
    std::cerr << "Error: " << ex.what() << std::endl;
    return EXIT_FAILURE;
}
