/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capdefs.hh"

#include <maxbase/host.hh>
#include <mysql.h>
#include <string>
#include <memory>

class RepStorage;

namespace cmd
{
static const char* REPLAY = "replay";
static const char* TRANSFORM = "transform";
static const char* CONVERT = "convert";
static const char* LIST_QUERIES = "list-queries";
static const char* DUMP_DATA = "dump-data";
}

struct RepConfig
{
    enum class CsvType
    {
        NONE,
        MINIMAL,
        FULL,
    };

    RepConfig(int argc, char** argv);

    std::string   user{"maxskysql"};
    std::string   password{"skysql"};
    maxbase::Host host{"127.1.1.0", 3306};
    int           verbosity = 0;
    CsvType       csv = CsvType::NONE;
    bool          row_counts = true;

    std::string capture_dir = "/home/mariadb/maxscale/var/lib/maxscale/capture";
    std::string file_name;      // full path, not necessarily in capture_dir
    std::string output_file;    // Output file, defaults to file_name
    std::string command = "replay";

    void show_help();

    std::unique_ptr<RepStorage> build_rep_storage() const;
};
