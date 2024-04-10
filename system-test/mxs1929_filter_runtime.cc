/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * MXS-1929: Runtime filter creation
 */

#include <maxtest/testconnections.hh>

#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <functional>

using namespace std;

void create_all(TestConnections& test)
{
    auto& repl = *test.repl;
    test.check_maxctrl("create server server1 " + string(repl.ip(0)) + " "
                       + to_string(repl.port(0)));
    test.check_maxctrl("create server server2 " + string(repl.ip(1)) + " "
                       + to_string(repl.port(1)));
    test.check_maxctrl("create server server3 " + string(repl.ip(2)) + " "
                       + to_string(repl.port(2)));
    test.check_maxctrl(
        "create service svc1 readwritesplit user=skysql password=skysql --servers server1 server2 server3");
    test.check_maxctrl("create listener svc1 listener1 4006");
    test.check_maxctrl(
        "create monitor mon1 mariadbmon user=skysql password=skysql --servers server1 server2 server3");
}

void destroy_all(TestConnections& test)
{
    test.check_maxctrl("unlink monitor mon1 server1 server2 server3");
    test.check_maxctrl("unlink service svc1 server1 server2 server3");
    test.check_maxctrl("destroy listener svc1 listener1");
    test.check_maxctrl("destroy service svc1");
    test.check_maxctrl("destroy monitor mon1");
    test.check_maxctrl("destroy server server1");
    test.check_maxctrl("destroy server server2");
    test.check_maxctrl("destroy server server3");
}

void basic(TestConnections& test)
{
    test.check_maxctrl("create filter test1 regexfilter \"match=SELECT 1\" \"replace=SELECT 2\"");
    test.check_maxctrl("alter service-filters svc1 test1");

    Connection c = test.maxscale->rwsplit();
    c.connect();
    test.expect(c.check("SELECT 1", "2"), "The regex filter did not replace the query");


    auto res = test.maxctrl("destroy filter test1");
    test.expect(res.rc != 0, "Destruction should fail when filter is in use");

    test.check_maxctrl("alter service-filters svc1");
    test.check_maxctrl("destroy filter test1");

    test.expect(c.check("SELECT 1", "2"), "The filter should not yet be destroyed");

    c.disconnect();
    c.connect();

    test.expect(c.check("SELECT 1", "1"), "The filter should be destroyed");
}

void visibility(TestConnections& test)
{
    auto in_list_filters = [&](std::string value) {
            auto res = test.maxctrl("list filters --tsv");
            return res.output.find(value) != string::npos;
        };

    test.check_maxctrl("create filter test1 hintfilter");
    test.expect(in_list_filters("test1"), "The filter should be visible after creation");

    test.check_maxctrl("destroy filter test1");
    test.expect(!in_list_filters("test1"), "The filter should not be visible after destruction");

    test.check_maxctrl("create filter test1 hintfilter");
    test.expect(in_list_filters("test1"), "The filter should again be visible after recreation");
    test.expect(!in_list_filters("svc1"), "Filter should not be in use");

    test.check_maxctrl("alter service-filters svc1 test1");
    test.expect(in_list_filters("svc1"), "Service should use the filter");

    test.check_maxctrl("alter service-filters svc1");
    test.expect(!in_list_filters("svc1"), "Service should not use the filter");

    test.check_maxctrl("destroy filter test1");
    test.expect(!in_list_filters("test1"), "The filter should not be visible after destruction");
}

void do_load_test(TestConnections& test,
                  std::function<void ()> tester,
                  std::function<void(std::atomic<bool>&)> worker)
{
    std::vector<std::thread> threads;
    std::atomic<bool> running {true};
    using std::chrono::milliseconds;

    for (int i = 0; i < 10; i++)
    {
        threads.emplace_back(worker, std::ref(running));
    }

    for (int i = 0; i < 10; i++)
    {
        tester();
    }

    running = false;

    for (auto& a : threads)
    {
        a.join();
    }
}

void load(TestConnections& test)
{
    auto tester = [&]() {
            test.check_maxctrl("create filter test1 regexfilter \"match=SELECT 1\" \"replace=SELECT 2\"");
            test.check_maxctrl("alter service-filters svc1 test1");
            test.check_maxctrl("alter service-filters svc1");
            test.check_maxctrl("destroy filter test1");
        };

    auto worker = [&](std::atomic<bool>& running) {
            while (running && test.global_result == 0)
            {
                Connection c = test.maxscale->rwsplit();
                c.connect();

                while (running && test.global_result == 0)
                {
                    test.expect(c.query("select 1"), "Query should succeed: %s", c.error());
                }
            }
        };

    do_load_test(test, tester, worker);
}

void filter_swap(TestConnections& test)
{
    test.check_maxctrl("create filter test1 regexfilter \"match=SELECT 1\" \"replace=SELECT 2\"");
    test.check_maxctrl("create filter test2 regexfilter \"match=SELECT 1\" \"replace=SELECT 3\"");

    auto tester = [&]() {
            test.check_maxctrl("alter service-filters svc1 test1");
            test.check_maxctrl("alter service-filters svc1 test2");
        };

    auto worker = [&](std::atomic<bool>& running) {
            while (running && test.global_result == 0)
            {
                Connection c = test.maxscale->rwsplit();
                c.connect();

                while (running && test.global_result == 0)
                {
                    test.expect(c.check("select 1", "1"), "Query should not return 1 as a result");
                }
            }
        };

    do_load_test(test, tester, worker);

    test.check_maxctrl("alter service-filters svc1");
    test.check_maxctrl("destroy filter test1");
    test.check_maxctrl("destroy filter test2");
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    test.tprintf("Creating servers, monitors and services");
    test.reset_timeout();
    create_all(test);

    test.tprintf("Basic test");
    test.reset_timeout();
    basic(test);

    test.tprintf("Visibility test");
    test.reset_timeout();
    visibility(test);

    test.tprintf("Load test");
    test.reset_timeout();
    load(test);

    test.tprintf("Filter swap test");
    test.reset_timeout();
    filter_swap(test);

    test.tprintf("Destroying servers, monitors and services");
    test.reset_timeout();
    destroy_all(test);

    return test.global_result;
}
