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
#include "../capbooststorage.hh"
#include "repcsvstorage.hh"

int main(int argc, char** argv)
try
{
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
