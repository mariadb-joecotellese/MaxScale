/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "../capdefs.hh"
#include "repplayer.hh"
#include "repconverter.hh"
#include "repshow.hh"
#include <iostream>
#include <maxbase/maxbase.hh>
#include <maxbase/stacktrace.hh>
#include "../capbooststorage.hh"
#include "repcsvstorage.hh"

void signal_set(int sig, void (* handler)(int))
{
    struct sigaction sigact = {};
    sigact.sa_handler = handler;

    do
    {
        errno = 0;
        sigaction(sig, &sigact, NULL);
    }
    while (errno == EINTR);
}

static bool s_dumped = false;

void write_line(const char* line)
{
    s_dumped = true;
    std::cerr << line << std::endl;
}

static void sigfatal_handler(int i)
{
    if (mxb::have_gdb())
    {
        mxb::dump_gdb_stacktrace(write_line);
    }

    if (!s_dumped)
    {
        mxb::dump_stacktrace(write_line);
    }

    if (!s_dumped)
    {
        mxb::emergency_stacktrace();
    }

    signal_set(i, SIG_DFL);
    raise(i);
}

void set_signal_handlers()
{
    signal_set(SIGSEGV, sigfatal_handler);
    signal_set(SIGABRT, sigfatal_handler);
    signal_set(SIGFPE, sigfatal_handler);
    signal_set(SIGILL, sigfatal_handler);
#ifdef SIGBUS
    signal_set(SIGBUS, sigfatal_handler);
#endif
}

int main(int argc, char** argv)
try
{
    set_signal_handlers();
    mxb::MaxBase mxb(MXB_LOG_TARGET_STDOUT);

    RepConfig config(argc, argv);
    mxb_log_set_syslog_enabled(false);

    if (config.verbosity > 0)
    {
        mxb_log_set_priority_enabled(LOG_INFO, true);
    }

    if (config.verbosity > 1)
    {
        mxb_log_set_priority_enabled(LOG_DEBUG, true);
    }

    if (config.command == cmd::REPLAY)
    {
        RepPlayer player(&config);
        player.replay();
    }
    else if (config.command == cmd::SUMMARY)
    {
        RepTransform transform(&config, RepTransform::TRANSFORM);
    }
    else if (config.command == cmd::CONVERT)
    {
        RepConverter converter(config);
    }
    else if (config.command == cmd::CANONICALS)
    {
        auto canonicals = CapBoostStorage(config.file_name, ReadWrite::READ_ONLY).canonicals();

        if (config.output_file == config.file_name)
        {
            RepCsvStorage::dump_canonicals(canonicals, std::cout);
        }
        else
        {
            std::ofstream output(config.output_file);
            RepCsvStorage::dump_canonicals(canonicals, output);
        }
    }
    else if (config.command == cmd::DUMP_DATA)
    {
        CapBoostStorage storage(config.file_name, ReadWrite::READ_ONLY);

        if (config.output_file == config.file_name)
        {
            storage.events_to_sql(std::cout);
        }
        else
        {
            std::ofstream out(config.output_file);
            storage.events_to_sql(out);
        }
    }
    else if (config.command == cmd::SHOW)
    {
        RepShow(config).show(std::cout);
    }
    else
    {
        // The code should never end up here: RepConfig::RepConfig exits on invalid options and commands
        mxb_assert(!true);
    }

    return EXIT_SUCCESS;
}
catch (std::exception& ex)
{
    std::cerr << "Error: " << ex.what() << std::endl;
    return EXIT_FAILURE;
}
