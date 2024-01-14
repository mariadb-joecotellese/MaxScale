/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../wcarstorage.hh"
#include <maxbase/host.hh>
#include <mysql.h>
#include <string>
#include <filesystem>
#include <memory>

struct PlayerConfig
{
    PlayerConfig(int argc, char** argv);

    std::string   user{"maxskysql"};
    std::string   password{"skysql"};
    maxbase::Host host{"127.1.1.0", 3306};

    std::string capture_dir = "/home/mariadb/maxscale/var/lib/maxscale/capture";
    std::string file_base_name;
    MYSQL*      pConn;


    void show_help();
};
