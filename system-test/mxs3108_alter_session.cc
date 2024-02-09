/*
 * Copyright (c) 2022 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * MXS-3108: Session alteration
 */

#include <maxtest/testconnections.hh>

#include <atomic>
#include <thread>
#include <vector>

void query_thread(TestConnections& test, Connection& conn, std::atomic<bool>& keep_going)
{
    while (keep_going && test.ok())
    {
        auto val = conn.field("SELECT 1");
        test.expect(!val.empty(), "SELECT returned an empty value. Error: %s", conn.error());
    }
}

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);

    auto conn = test.maxscale->rwsplit();
    test.expect(conn.connect(), "Connection failed: %s", conn.error());

    std::string cmd_no_filters = "alter session-filters " + std::to_string(conn.thread_id());
    std::string cmd_one_filter = cmd_no_filters + " Regex";
    std::string cmd_two_filters = cmd_no_filters + " Regex Regex";

    test.tprintf("Simple modification of filters");

    test.expect(conn.field("SELECT 1") == "1", "Filter should not be applied. Error: %s", conn.error());
    test.check_maxctrl(cmd_one_filter);
    test.expect(conn.field("SELECT 1") == "2", "Filter should be applied. Error: %s", conn.error());
    test.check_maxctrl(cmd_no_filters);
    test.expect(conn.field("SELECT 1") == "1", "Filter should not be applied. Error: %s", conn.error());

    test.expect(conn.send_query("SELECT SLEEP(3)"), "Failed to send query: %s", conn.error());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    test.check_maxctrl(cmd_one_filter);
    test.expect(conn.read_query_result(), "Failed to read query result: %s", conn.error());
    test.expect(conn.field("SELECT 1") == "2", "Filter should be applied");

    test.tprintf("Modification of filters under load");

    std::atomic<bool> keep_going {true};
    std::thread thr(query_thread, std::ref(test), std::ref(conn), std::ref(keep_going));

    for (int i = 0; i < 5; i++)
    {
        test.check_maxctrl(cmd_one_filter);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        test.check_maxctrl(cmd_two_filters);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        test.check_maxctrl(cmd_no_filters);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    keep_going = false;
    thr.join();

    test.tprintf("Session log configuration");

    test.check_maxctrl("alter session " + std::to_string(conn.thread_id()) + " log_info true");
    test.expect(conn.query("SELECT 123"), "Query failed: %s", conn.error());
    test.log_includes("info   :.*SELECT 123");

    test.check_maxctrl("alter session " + std::to_string(conn.thread_id()) + " log_info false");
    test.expect(conn.query("SELECT 456"), "Query failed: %s", conn.error());
    test.log_excludes("info   :.*SELECT 456");

    return test.global_result;
}
