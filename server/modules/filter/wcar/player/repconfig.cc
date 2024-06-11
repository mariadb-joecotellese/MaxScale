/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "repconfig.hh"
#include "reptransform.hh"
#include "../capbooststorage.hh"
#include "repbooststorage.hh"
#include "repcsvstorage.hh"
#include <getopt.h>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <map>


namespace
{
// TODO make this print out more information.
const std::string short_version_str = "0.1";
std::string display_version_info_and_exit()
{
    // TODO display distro and build time. Make a procedure so the version
    // number is incremented each time a new version is released.
    std::cout << "maxplayer: version " << short_version_str << '\n';
    std::cout << "Copyright (c) 2024 MariaDB plc\n";
    exit(EXIT_SUCCESS);
}

using Command = std::pair<std::string, std::string>;

std::vector<Command> s_commands{
    {cmd::SUMMARY, "Show a summary of the capture."},
    {cmd::REPLAY, "Replay the capture."},
    {cmd::CONVERT, "Converts the input file (either .cx or .rx) to a replay file (.rx or .csv)."},
    {cmd::CANONICALS, "List the canonical forms of the captured SQL as CSV."},
    {cmd::DUMP_DATA, "Dump capture data as SQL."},
    {cmd::SHOW, "Show the SQL of one or more events."}
};
}

std::ostream& operator<<(std::ostream& os, RepConfig::CsvType csv_type)
{
    switch (csv_type)
    {
    case RepConfig::CsvType::NONE:
        os << "none";
        break;

    case RepConfig::CsvType::MINIMAL:
        os << "minimal";
        break;

    case RepConfig::CsvType::FULL:
        os << "full";
        break;
    }

    return os;
}

std::ostream& operator<<(std::ostream& os, RepConfig::QueryFilter query_filter)
{
    switch (query_filter)
    {
    case RepConfig::QueryFilter::NONE:
        os << "none";
        break;

    case RepConfig::QueryFilter::WRITE_ONLY:
        os << "write-only";
        break;

    case RepConfig::QueryFilter::READ_ONLY:
        os << "read-only";
        break;
    }

    return os;
}

std::ostream& operator<<(std::ostream& os, RepConfig::CommitOrder commit_order)
{
    switch (commit_order)
    {
    case RepConfig::CommitOrder::NONE:
        os << "none";
        break;

    case RepConfig::CommitOrder::OPTIMISTIC:
        os << "optimistic";
        break;

    case RepConfig::CommitOrder::SERIALIZED:
        os << "serialized";
        break;
    }

    return os;
}

const struct option long_opts[] =
{
    {"help",         no_argument,       0, 'h'},
    {"user",         required_argument, 0, 'u'},
    {"password",     required_argument, 0, 'p'},
    {"host",         required_argument, 0, 'H'},
    {"speed",        required_argument, 0, 's'},
    {"csv",          required_argument, 0, 'c'},
    {"output",       required_argument, 0, 'o'},
    {"verbose",      no_argument,       0, 'v'},
    {"analyze",      no_argument,       0, 'A'},
    {"idle-wait",    required_argument, 0, 'i'},
    {"commit-order", required_argument, 0, 'C'},
    {"chunk-size",   required_argument, 0, 'B'},
    {"query-filter", required_argument, 0, 'f'},
    {"version",      no_argument,       0, 'V'},
    {0,              0,                 0, 0  }
};

// This is not separately checked, keep in sync with long_opts.
const char* short_opts = "hu:p:H:s:c:o:vAi:C:B:f:V";

// Creates a stream output overload for M, which is an ostream&
// manipulator usually a lambda returning std::ostream&. Participates
// in overloading only if m(os) is not an error (SFINAE).
template<class M>
auto operator<<(std::ostream& os, const M& m) -> decltype(m(os))
{
    return m(os);
}

// Output manipulator for help text.
template<typename H>
auto OPT(int optval, H help)
{
    size_t indent = 0;

    for (int i = 0; long_opts[i].val; ++i)
    {
        indent = std::max(strlen(long_opts[i].name), indent);
    }

    int64_t idx;
    for (idx = 0; long_opts[idx].val; ++idx)
    {
        if (long_opts[idx].val == optval)
        {
            break;
        }
    }

    std::ostringstream msg;
    mxb_assert(long_opts[idx].val != 0);
    msg << "\n-" << char(long_opts[idx].val) << " --"
        << std::setw(indent + 1) << std::left << long_opts[idx].name
        << std::boolalpha;

    if constexpr (std::is_convertible_v<H, mxb::Duration> )
    {
        msg << mxb::to_string(help);
    }
    else
    {
        msg << help;
    }

    return [msg = msg.str()](std::ostream& os) -> std::ostream& {
        return os << msg.c_str();
    };
}

std::string list_commands()
{
    size_t indent = 0;

    for (const auto& [cmd, _] : s_commands)
    {
        indent = std::max(cmd.size(), indent);
    }

    std::ostringstream msg;

    for (const auto& [cmd, desc] : s_commands)
    {
        msg << std::setw(indent + 1) << std::left << cmd << desc << "\n";
    }

    return msg.str();
}

void RepConfig::show_help()
{
    std::cout << "Usage: maxplayer [OPTION]... [COMMAND] FILE\n\n";
    std::cout << "Commands: (default: replay)\n" << list_commands() << '\n';
    std::cout << "Options:\n"
                 "--user          User name for login to the replay server.\n"
                 "-u              This version does not support using the actual user names\n"
                 "                that were used during capture.\n"
                 "\n"
                 "--password      Only clear text passwords are supported as of yet.\n"
                 "-p\n"
                 "\n"
                 "--host          The address of the replay server in <IP>:<port> format.\n"
                 "-h              E.g. 127.0.0.1:4006\n"
                 "\n"
                 "--csv           TODO\n"
                 "-c\n"
                 "\n"
                 "--output        The name of the csv output file: e.g. baseline.csv\n"
                 "-o\n"
                 "\n"
                 "--speed         The value is a multiplier. 2.5 is 2.5x speed and 0.5 is half speed.\n"
                 "-s              A value of zero means no limit, or replay as fast as possible.\n"
                 "                A multiplier of 2.5 might not have any effect as the actual time spent\n"
                 "                depends on many factors, such as the captured volume and replay server.\n"
                 "\n"
                 "--idle-wait     Relates to playback speed, and can be used together with --speed.\n"
                 "-i              During capture there can be long delays where there is no traffic.\n"
                 "                One hour of no capture traffic would mean replay waits for one hour.\n"
                 "                idle-wait allows to move simulation time forwards when such gaps\n"
                 "                occure. A 'gap' starts when all prior queries have fully executed.\n"
                 "                --idle-wait takes a duration value. A negative value turns the feature off,\n"
                 "                            i.e. the one hour wait would happen.\n"
                 "                --idle-wait 0s means time moves to the event start-time immediately\n"
                 "                            when a gap is detected, i.e., all gaps are skipped over.\n"
                 "                --idle-wait 10s means time moves to the event start-time 10 seconds\n"
                 "                            (wall time) after the gap was detected. Shorter\n"
                 "                            gaps than 10 seconds will thus be fully waited for.\n"
                 "                --idle-wait has a default value of 1 second.\n"
                 "                Examples: 1h, 60m, 3600s, 3600000ms, which all define the same duration.\n"
                 "\n"
                 "--query-filter  Options: none, write-only, read-only. Default: none.\n"
                 "-f              Replay can optionally apply only writes or only reads. This option is useful\n"
                 "                once the databases to be tested have been prepared (see full documentation)\n"
                 "                and optionally either a write-only run, or a full replay has been run.\n"
                 "                Now multiple read-only runs against the server(s) are simple as no further\n"
                 "                data syncronization is needed.\n"
                 "                Note that this mode has its limitations as the query results may\n"
                 "                be very different than what they were during capture.\n"
                 "\n"
                 "--commit-order  Options: none, optimistic, serialized. Default: optimistic\n"
                 "-C              none       - No ordering of transactions\n"
                 "                optimistic - If a transaction was started (in capture) before other\n"
                 "                             running transactions were commited, the transaction\n"
                 "                             can be scheduled to run.\n"
                 "                serialized - A transaction can only start when the previous transaction\n"
                 "                             has commited. This effectivdly serializes the workload\n"
                 "                             as far as transactions are concerned.\n"
                 "\n"
                 "--analyze       Enabling this option will track the server Rows_read statistic for each query.\n"
                 "-A              This will slow down the overall replay time. The query time measurements\n"
                 "                are still valid, but currently this option should only be used when\n"
                 "                it is of real value to know how many rows the server read for each query.\n"
                 "\n"
                 "--verbose       Verbose output. The option can be repeated for more verbosity: -vvv\n"
                 "-v\n"
                 "\n"
                 "--version       Display the version number and copyrights.\n"
                 "-V\n";

    if (!file_name.empty())
    {
        std::cout << "\nInput file: " << file_name << "\n";
    }
    std::cout << OPT('h', "this help text (with current option values)")
              << OPT('u', user)
              << OPT('p', password)
              << OPT('H', host)
              << OPT('c', csv)
              << OPT('o', output_file)
              << OPT('s', sim_speed)
              << OPT('i', idle_wait)
              << OPT('f', query_filter)
              << OPT('C', commit_order)
              << OPT('A', analyze)
              << OPT('v', verbosity)
              << OPT('V', short_version_str)
              << std::endl;
}

RepConfig::RepConfig(int argc, char** argv)
{
    bool help = false;
    bool error = false;

    int opt;
    while ((opt = getopt_long(argc, argv, short_opts, long_opts, nullptr)) != -1)
    {
        switch (opt)
        {
        case 'h':
            help = true;
            break;

        case 'u':
            user = optarg;
            break;

        case 'p':
            password = optarg;
            break;

        case 'H':
            host = maxbase::Host::from_string(optarg);
            if (!host.is_valid())
            {
                std::cerr << "Host string is invalid: " << optarg << std::endl;
                help = true;
                error = true;
            }
            break;

        case 'c':
            csv = CsvType::NONE;

            if (optarg == "minimal"s)
            {
                csv = CsvType::MINIMAL;
            }
            else if (optarg == "full"s)
            {
                csv = CsvType::FULL;
            }
            else if (optarg != "none"s)
            {
                std::cerr << "Invalid --csv value: " << optarg << std::endl;
                help = true;
                error = true;
            }
            break;

        case 'C':
            if (optarg == "none"s)
            {
                commit_order = CommitOrder::NONE;
            }
            else if (optarg == "optimistic"s)
            {
                commit_order = CommitOrder::OPTIMISTIC;
            }
            else if (optarg == "serialized"s)
            {
                commit_order = CommitOrder::SERIALIZED;
            }
            else
            {
                std::cerr << "Invalid --commit-order value: " << optarg << std::endl;
                help = true;
                error = true;
            }
            break;

        case 's':
            sim_speed = std::stof(optarg);
            // 10'000x overflows int64 nanos in ~ 10 days
            sim_speed = std::min(sim_speed, 10'000.0f);
            break;

        case 'v':
            verbosity++;
            break;

        case 'o':
            output_file = optarg;
            break;

        case 'A':
            analyze = true;
            break;

        case 'i':

            if (optarg[0] == '-')
            {
                idle_wait = -1s;
            }
            else
            {
                std::chrono::milliseconds ms;
                if (!get_suffixed_duration(optarg, &ms))
                {
                    std::cerr << "Invalid --idle-wait value: " << optarg << std::endl;
                    help = true;
                    error = true;
                }
                idle_wait = ms;
            }
            break;

        case 'B':
            // This option is for testing the merge sort and is not visible in --help
            if (!get_suffixed_size(optarg, &chunk_size))
            {
                std::cerr << "Invalid --chunk-size value: " << optarg << std::endl;
                help = true;
                error = true;
            }
            break;


        case 'f':
            query_filter = QueryFilter::NONE;
            if (optarg == "write-only"s)
            {
                query_filter = QueryFilter::WRITE_ONLY;
            }
            else if (optarg == "read-only"s)
            {
                query_filter = QueryFilter::READ_ONLY;
            }
            else if (optarg != "none"s)
            {
                std::cerr << "Invalid --query_filter value: " << optarg << std::endl;
                help = true;
                error = true;
            }
            break;

        case 'V':
            display_version_info_and_exit();
            break;

        default:
            help = true;
            error = true;
            break;
        }
    }

    if (optind >= argc)
    {
        if (!help)
        {
            std::cerr << "error: input FILE missing" << std::endl;
            help = true;
            error = true;
        }
    }
    else
    {
        if (argc - optind > 1)
        {
            command = argv[optind++];

            auto it = std::find_if(s_commands.begin(), s_commands.end(), [&](const auto& kv){
                return kv.first == command;
            });

            if (it == s_commands.end())
            {
                std::cerr << "error: Unknown command " << command << std::endl;
                help = true;
                error = true;
            }
        }

        file_name = argv[optind++];
        if (file_name[0] != '/')
        {
            file_name = capture_dir + '/' + file_name;
        }

        if (!fs::exists(file_name))
        {
            std::cerr << "File " << file_name << " does not exist" << std::endl;
            help = true;
            error = true;
        }

        if (output_file.empty())
        {
            // The RepStorage will rename it with the appropriate file extension.
            output_file = file_name;
        }

        while (optind < argc)
        {
            extra_args.push_back(argv[optind++]);
        }
    }

    if (!help)
    {
        if (command == cmd::SHOW)
        {
            mxb_assert(!extra_args.empty());
        }
        else if (!extra_args.empty())
        {
            std::cerr << "error: Too many arguments" << std::endl;
            help = true;
            error = true;
        }
    }

    if (help)
    {
        show_help();
        exit(error ? EXIT_FAILURE : EXIT_SUCCESS);
    }
}

std::unique_ptr<RepStorage> RepConfig::build_rep_storage() const
{
    fs::path path = output_file;
    bool file_exists = false;

    if (csv == RepConfig::CsvType::NONE)
    {
        path.replace_extension("rx");
    }

    try
    {
        file_exists = fs::exists(path) && fs::file_size(path) > 0;
    }
    catch (const std::exception& e)
    {
        // If the output path exists but is not a real file (e.g. /dev/null or a FIFO) the call to
        // fs::file_size will throw an exception.
    }

    if (file_exists)
    {
        MXB_THROW(WcarError, "The replay file already exists, will not overwrite replay: " << path);
    }

    if (csv == RepConfig::CsvType::MINIMAL)
    {
        return std::make_unique<RepCsvStorage>(path);
    }
    else if (csv == RepConfig::CsvType::FULL)
    {
        CapBoostStorage boost(file_name, ReadWrite::READ_ONLY);
        return std::make_unique<RepCsvStorage>(path, boost.canonicals());
    }
    else
    {
        mxb_assert(csv == RepConfig::CsvType::NONE);
        return std::make_unique<RepBoostStorage>(path, RepBoostStorage::WRITE_ONLY);
    }
}
