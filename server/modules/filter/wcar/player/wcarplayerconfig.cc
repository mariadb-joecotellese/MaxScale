/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarplayerconfig.hh"
#include "../wcarsqlitestorage.hh"
#include "../wcarbooststorage.hh"
#include <getopt.h>
#include <iostream>
#include <iomanip>


const struct option long_opts[] =
{
    {"help",     no_argument,       0, 'h'},
    {"user",     required_argument, 0, 'u'},
    {"password", required_argument, 0, 'p'},
    {"host",     required_argument, 0, 'H'},
    {0,          0,                 0, 0  }
};

// This is not seprately checked, keep in sync with long_opts.
const char* short_opts = "hu:p:H:";

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

void PlayerConfig::show_help()
{
    std::cout << "Usage: player [OPTION]... FILE"
              << OPT('h', "this help text (with current option values)")
              << OPT('u', user)
              << OPT('p', password)
              << OPT('H', host)
              << "\nInput file: " << file_base_name
              << std::endl;
}

PlayerConfig::PlayerConfig(int argc, char** argv)
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
        file_base_name = argv[optind];
    }

    if (help)
    {
        show_help();
        exit(error ? EXIT_FAILURE : EXIT_SUCCESS);
    }

    pConn = mysql_init(nullptr);
    if (pConn == nullptr)
    {
        std::cerr << "Could not initialize connector-c " << mysql_error(pConn) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (mysql_real_connect(pConn, host.address().c_str(), user.c_str(),
                           password.c_str(), "", host.port(), nullptr, 0) == nullptr)
    {
        std::cerr << "Could not connect to " << host.address()
                  << ':' << std::to_string(host.port())
                  << " Error: " << mysql_error(pConn) << '\n';
        exit(EXIT_FAILURE);
    }
}

std::unique_ptr<Storage> PlayerConfig::create_read_storage(const std::filesystem::path& path) const
{
    auto ext = path.extension();
    std::cout << "path = " << path << ' ' << ext << std::endl;
    if (ext == ".sqlite")
    {
        return std::make_unique<SqliteStorage>(path, Access::READ_ONLY);
    }
    else if (ext == ".cx" || ext == ".ex")
    {
        return std::make_unique<BoostStorage>(path, ReadWrite::READ_ONLY);
    }

    abort();
}
