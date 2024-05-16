/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "../capbooststorage.hh"
#include <maxbase/log.hh>
#include <maxbase/temp_file.hh>
#include <iostream>
#include <filesystem>

const std::string test_data = "hello world";
std::string filename;

bool write_data()
{
    QueryEvent ev;
    ev.session_id = 1;
    ev.flags = 0;
    ev.start_time = std::chrono::system_clock::now();
    ev.end_time = ev.start_time + 1s;
    ev.sCanonical = std::make_shared<std::string>("USE test");
    ev.event_id = 1;
    ev.sTrx = std::make_unique<Trx>(1, Gtid {0, 1, 2});

    bool ok = true;
    CapBoostStorage storage(filename, ReadWrite::WRITE_ONLY);
    storage.add_query_event(std::move(ev));

    int pos = storage.tell();

    if (pos <= 0)
    {
        std::cout << "Unexpected write position: " << pos << std::endl;
        ok = false;
    }

    return ok;
}

bool read_data()
{
    bool ok = true;
    CapBoostStorage storage(filename, ReadWrite::READ_ONLY);

    for (auto&& ev : storage)
    {
        std::cout << ev << std::endl;
    }

    int pos = storage.tell();

    if (pos <= 0)
    {
        std::cout << "Unexpected write position: " << pos << std::endl;
        ok = false;
    }

    return ok;
}

bool write_corrupted_data()
{
    QueryEvent ev;
    ev.session_id = 0;
    ev.flags = 0;
    ev.start_time = wall_time::EPOCH;
    ev.end_time = wall_time::EPOCH;
    ev.can_id = 0xdeadbeefdeadbeef;
    ev.event_id = 0;

    BoostOFile bof(MAKE_STR(filename << ".ex"));
    CapBoostStorage::save_query_event(bof, ev);

    bool ok = true;
    int pos = bof.tell();

    if (pos <= 0)
    {
        std::cout << "Unexpected write position: " << pos << std::endl;
        ok = false;
    }

    return ok;
}

bool read_corrupted_data()
{
    bool ok = true;

    try
    {
        CapBoostStorage storage(filename, ReadWrite::READ_ONLY);

        for (auto&& ev : storage)
        {
            std::cout << ev << std::endl;
        }

        std::cout << "Corrupted event was not detected" << std::endl;
        ok = false;
    }
    catch (const std::exception& ex)
    {
        std::cout << "Missing canonical was detected: " << ex.what() << std::endl;
    }

    return ok;
}

bool open_truncated_file()
{
    truncate((filename + ".ex").c_str(), 3);
    return read_corrupted_data();
}

int main(int argc, char** argv)
{
    mxb::TempDirectory tmp;
    filename = tmp.dir() + "/storage";

    int rc = EXIT_SUCCESS;
    mxb::Log logger(MXB_LOG_TARGET_STDOUT);

    if (!write_data() || !read_data() || !write_corrupted_data() || !read_corrupted_data()
        || !open_truncated_file())
    {
        rc = EXIT_FAILURE;
    }

    return rc;
}
