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
using Command = std::pair<std::string, std::string>;

std::vector<Command> s_commands{
    {cmd::REPLAY, "Replay the capture."},
    {cmd::TRANSFORM, "Only transform the capture to make it ready for replay."},
    {cmd::CONVERT, "Converts the input file (either .cx or .rx) to a replay file (.rx or .csv)."},
    {cmd::LIST_QUERIES, "List the canonical forms of the captured SQL as CSV."},
    {cmd::DUMP_DATA, "Dump capture data as SQL."},
};
}

const struct option long_opts[] =
{
    {"help",          no_argument,       0, 'h'},
    {"user",          required_argument, 0, 'u'},
    {"password",      required_argument, 0, 'p'},
    {"host",          required_argument, 0, 'H'},
    {"mode",          required_argument, 0, 'm'},
    {"csv",           optional_argument, 0, 'c'},
    {"output",        required_argument, 0, 'o'},
    {"verbose",       no_argument,       0, 'v'},
    {"no-row-counts", no_argument,       0, 'R'},
    {0,               0,                 0, 0  }
};

// This is not seprately checked, keep in sync with long_opts.
const char* short_opts = "hvRu:p:H:c::o:";

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
    if (long_opts[idx].val == 0)
    {
        msg << "\nBUG: invalid option '" << char(optval) << "' in  help function\n";
    }
    else
    {
        msg << "\n-" << char(long_opts[idx].val) << " --"
            << std::setw(indent + 1) << std::left << long_opts[idx].name
            << std::boolalpha << help;
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
    std::cout << "Usage: player [OPTION]... [COMMAND] FILE"
              << OPT('h', "this help text (with current option values)")
              << OPT('c', "Save replay as CSV (options: none, minimal, full)")
              << OPT('o', "Output file (" + output_file + ")")
              << OPT('u', user)
              << OPT('p', password)
              << OPT('H', host)
              << OPT('v', verbosity)
              << OPT('R', row_counts)
              << "\n\nInput file: " << file_name << "\n"
              << "\n"
              << "Commands:\n"
              << list_commands()
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
            if (!optarg || optarg == "minimal"s)
            {
                csv = CsvType::MINIMAL;
            }
            else if (optarg == "full"s)
            {
                csv = CsvType::FULL;
            }
            else
            {
                std::cerr << "Invalid --csv value: " << optarg << std::endl;
                help = true;
                error = true;
            }
            break;

        case 'v':
            verbosity++;
            break;

        case 'o':
            output_file = optarg;
            break;

        case 'R':
            row_counts = false;
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
    else if (argc - optind > 2)
    {
        if (!help)
        {
            std::cerr << "error: Too many arguments" << std::endl;
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

        file_name = argv[optind];
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
    path.replace_extension(csv != RepConfig::CsvType::NONE ? "csv" : "rx");

    if (fs::exists(path))
    {
        MXB_THROW(WcarError, "The replay file already exists, will not overwrite replay: " << path);
    }

    if (csv != RepConfig::CsvType::NONE)
    {
        CapBoostStorage boost(file_name, ReadWrite::READ_ONLY);
        return std::make_unique<RepCsvStorage>(path, boost.canonicals(), csv);
    }
    else
    {
        return std::make_unique<RepBoostStorage>(path, RepBoostStorage::WRITE_ONLY);
    }
}
