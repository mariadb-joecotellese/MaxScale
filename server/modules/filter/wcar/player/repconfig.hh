/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capdefs.hh"

#include <maxbase/host.hh>
#include <maxscale/paths.hh>
#include <mysql.h>
#include <string>
#include <memory>

class RepStorage;

namespace cmd
{
static const char* SUMMARY = "summary";
static const char* REPLAY = "replay";
static const char* CONVERT = "convert";
static const char* CANONICALS = "canonicals";
static const char* DUMP_DATA = "dump-data";
static const char* SHOW = "show";
}

struct RepConfig
{
    enum class CsvType
    {
        NONE,
        MINIMAL,
        FULL,
    };

    enum class CommitOrder
    {
        // No ordering of transactions
        NONE,

        // Optimistic ordering of transactions. Assumes that if a transaction was started before the latest
        // transaction was committed, it can be executed.
        OPTIMISTIC,

        // Serialized ordering of transactions. A transaction can only start if it's the next transaction in
        // line. This effectively serializes the execution of the workload for all transactions that cause
        // modifications.
        SERIALIZED,
    };

    RepConfig(int argc, char** argv);

    std::string   user{"maxskysql"};
    std::string   password{"skysql"};
    maxbase::Host host{"127.1.1.0", 3306};
    int           verbosity = 0;
    CsvType       csv = CsvType::NONE;
    CommitOrder   commit_order = CommitOrder::OPTIMISTIC;
    bool          analyze = false;

    std::string capture_dir = mxs::datadir() + std::string("/wcar");
    std::string file_name;      // full path, not necessarily in capture_dir
    std::string output_file;    // Output file, defaults to file_name
    std::string command = "replay";
    float       sim_speed = 1.0;
    uint64_t    chunk_size = 0; // A value of 0 uses system memory for sort buffer sizing

    std::vector<std::string> extra_args;

    void show_help();

    std::unique_ptr<RepStorage> build_rep_storage() const;
};
