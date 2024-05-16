/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "../repbooststorage.hh"
#include "../repcsvstorage.hh"
#include <maxbase/log.hh>
#include <maxbase/temp_file.hh>
#include <iostream>

#define EXPECT(a) if (!(a)) {throw std::runtime_error(#a);}

RepEvent make_repevent()
{
    RepEvent ev;

    ev.event_id = 1;
    ev.start_time = wall_time::EPOCH;
    ev.end_time = wall_time::EPOCH + 1s;
    ev.can_id = 4;
    ev.num_rows = 5;
    ev.rows_read = 6;

    return ev;
}

void compare_events(const RepEvent& lhs, const RepEvent& rhs)
{
    EXPECT(lhs.event_id == rhs.event_id);
    EXPECT(lhs.start_time == rhs.start_time);
    EXPECT(lhs.end_time == rhs.end_time);
    EXPECT(lhs.can_id == rhs.can_id);
    EXPECT(lhs.num_rows == rhs.num_rows);
    EXPECT(lhs.rows_read == rhs.rows_read);
}

void test_boost_storage()
{
    mxb::TempDirectory tmp;
    std::string filename = tmp.dir() + "/repstorage.rx";
    auto ev = make_repevent();
    auto storage = std::make_unique<RepBoostStorage>(filename, RepBoostStorage::WRITE_ONLY);
    storage->add_rep_event(std::move(ev));

    storage = std::make_unique<RepBoostStorage>(filename, RepBoostStorage::READ_ONLY);
    auto end = storage->end();
    auto it = storage->begin();
    EXPECT(it != end);
    compare_events(ev, *it);
}

void test_csv_storage()
{
    mxb::TempDirectory tmp;
    std::string filename = tmp.dir() + "/repstorage.csv";
    auto ev = make_repevent();
    auto storage = std::make_unique<RepCsvStorage>(filename);
    storage->add_rep_event(std::move(ev));

    storage = std::make_unique<RepCsvStorage>(filename);
    // This should work even if RepCsvStorage does not implement the read iterators
    auto end = storage->end();

    try
    {
        auto it = storage->begin();
        EXPECT(std::string("this should") == "never be reached");
    }
    catch (const std::exception& e)
    {
        EXPECT(std::string(e.what()) == "Not implemented");
    }

    std::string data = mxb::strtok(MAKE_STR(std::ifstream(filename).rdbuf()), "\n")[1];
    std::cout << "Stored data: " << data << std::endl;
    EXPECT(data == "1,4,1.000000,0.000000,5,6,0");
}

int main(int argc, char** argv)
{
    mxb::Log logger(MXB_LOG_TARGET_STDOUT);

    test_boost_storage();
    test_csv_storage();

    return 0;
}
