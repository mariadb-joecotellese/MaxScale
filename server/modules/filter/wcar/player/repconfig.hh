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
    enum class OutputType
    {
        CSV,
        CSV_LARGE,
        BINARY
    };

    enum class QueryFilter
    {
        NONE,
        READ_ONLY,
        WRITE_ONLY
    };

    enum class CommitOrder
    {
        // No ordering of transactions
        NONE,

        // Orders transactions as per the transaction order during capture.
        // A transaction can be scheduled to run if all transactions that
        // ended before it during capture, have ended in replay.
        NORMAL,

        // Serialized ordering of transactions. A transaction can only start
        // if it's the next transaction in line. This effectively serializes
        // the execution of the workload for all transactions that cause modifications.
        SERIALIZED,
    };

    RepConfig(int argc, char** argv);

    std::string   user{"maxskysql"};
    std::string   password{"skysql"};
    maxbase::Host host{"127.1.1.0", 3306};
    int           verbosity = 0;
    OutputType    output_type = OutputType::CSV;
    CommitOrder   commit_order = CommitOrder::NORMAL;
    bool          analyze = false;
    mxb::Duration idle_wait{1s};

    std::string capture_dir = mxs::datadir() + std::string("/wcar");
    std::string file_name;      // full path, not necessarily in capture_dir
    std::string output_file;    // Output file, defaults to file_name
    std::string command = "replay";
    float       sim_speed = 1.0;
    uint64_t    chunk_size = 0; // A value of 0 uses system memory for sort buffer sizing
    QueryFilter query_filter = QueryFilter::NONE;


    std::vector<std::string> extra_args;

    void show_help();

    std::unique_ptr<RepStorage> build_rep_storage() const;
};

std::ostream& operator<<(std::ostream& os, RepConfig::QueryFilter query_filter);
std::ostream& operator<<(std::ostream& os, RepConfig::OutputType output_type);
std::ostream& operator<<(std::ostream& os, RepConfig::CommitOrder commit_order);
