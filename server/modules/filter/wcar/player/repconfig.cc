/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "repconfig.hh"
#include "reptransform.hh"
#include <getopt.h>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <map>

namespace
{

const char* MODE_HELP = "Mode of operation: replay (default), transform";

std::map<std::string_view, RepConfig::Mode> s_mode_values{
    {"replay", RepConfig::Mode::REPLAY},
    {"transform", RepConfig::Mode::TRANSFORM},
};

RepConfig::Mode mode_from_string(std::string_view str)
{
    auto it = s_mode_values.find(str);
    return it != s_mode_values.end() ? it->second : RepConfig::Mode::UNKNOWN;
}

std::string mode_to_string(RepConfig::Mode mode)
{
    auto it = std::find_if(s_mode_values.begin(), s_mode_values.end(), [&](const auto& kv){
        return kv.second == mode;
    });
    return std::string(it != s_mode_values.end() ? it->first : "unknown");
}
}

const struct option long_opts[] =
{
    {"help",     no_argument,       0, 'h'},
    {"user",     required_argument, 0, 'u'},
    {"password", required_argument, 0, 'p'},
    {"host",     required_argument, 0, 'H'},
    {"mode",     required_argument, 0, 'm'},
    {0,          0,                 0, 0  }
};

// This is not seprately checked, keep in sync with long_opts.
const char* short_opts = "hu:p:H:m:";

// Creates a stream output overload for M, which is an ostream&
// manipulator usually a lambda returning std::ostream&. Participates
// in overloading only if m(os) is not an error (SFINAE).
template<class M>
auto operator<<(std::ostream& os, const M& m) -> decltype(m(os))
{
    return m(os);
}

constexpr int INDENT = 12;

// Output manipulator for help text.
template<typename H>
auto OPT(int optval, H help)
{
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
            << std::setw(INDENT) << std::left << long_opts[idx].name
            << std::boolalpha << help;
    }

    return [msg = msg.str()](std::ostream& os) -> std::ostream& {
        return os << msg.c_str();
    };
}

void RepConfig::show_help()
{
    std::cout << "Usage: player [OPTION]... FILE"
              << OPT('h', "this help text (with current option values)")
              << OPT('u', user)
              << OPT('p', password)
              << OPT('H', host)
              << OPT('m', MODE_HELP)
              << "\nInput file: " << file_name
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

        case 'm':
            if ((mode = mode_from_string(optarg)) == Mode::UNKNOWN)
            {
                std::cerr << "Invalid mode value: " << optarg << std::endl;
                help = true;
                error = true;
            }
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
        file_name = argv[optind];
        if (file_name[0] != '/')
        {
            file_name = capture_dir + '/' + file_name;
        }
    }

    if (help)
    {
        show_help();
        exit(error ? EXIT_FAILURE : EXIT_SUCCESS);
    }
}
