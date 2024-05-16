/*
 * Copyright (c) 2024 MariaDB plc
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

#include "diff.hh"
#include "../enterprise_test.hh"

using namespace std;

namespace
{

std::string username;
std::string password;

/**
 * Check that data written to MaxScale ends up in both servers.
 */
void test_duplication(TestConnections& test,
                      const std::string& main_server_ip,
                      const std::string& other_server_ip)
{
    auto& mxt = *test.maxscale;

    Connection maxscale(mxt.ip4(), 4006, username, password);
    test.expect(maxscale.connect(), "Could not connect to MaxScale.");

    maxscale.query("DROP TABLE IF EXISTS test.Diff");
    maxscale.query("CREATE TABLE test.Diff (f int)");
    maxscale.query("INSERT INTO test.Diff VALUES (1)");
    maxscale.query("INSERT INTO test.Diff VALUES (2)");

    auto maxscale_result = maxscale.rows("SELECT * FROM test.Diff");

    mxt::ReplicationCluster* pCluster = test.repl;

    Connection main_server(main_server_ip, 3306, username, password);
    test.expect(main_server.connect(), "Could not connect to %s.", main_server_ip.c_str());

    auto main_server_result = main_server.rows("SELECT * FROM test.Diff");

    test.expect(maxscale_result == main_server_result,
                "Results from MaxScale and from main server are not identical.");

    Connection other_server(other_server_ip, 3306, username, password);
    test.expect(other_server.connect(), "Could not connect to %s.", other_server_ip.c_str());

    auto other_server_result = other_server.rows("SELECT * FROM test.Diff");

    test.expect(maxscale_result == other_server_result,
                "Results from MaxScale and from other server are not identical.");
}

void test_duplication(TestConnections& test,
                      const std::string& main_server, int main_server_idx,
                      const std::string& other_server, int other_server_idx)
{
    cout << "Testing duplication of data." << endl;

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", main_server, other_server);
    mxb::Json status = diff.start();

    diff.wait_for_state(status, "comparing", 2);

    mxt::ReplicationCluster* pCluster = test.repl;
    test_duplication(test, pCluster->ip(main_server_idx), pCluster->ip(other_server_idx));

    diff.stop();
    diff.destroy();
}

/**
 * Simplest possible case, no concurrent activity.
 */
void test_easy_setup(TestConnections& test, const std::string& main_server, const std::string& other_server)
{
    cout << "Easy case, no concurrent activity." << endl;

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", main_server, other_server);
    diff.status();
    diff.start();
    diff.status();
    diff.summary();
    diff.stop();
    diff.destroy();
}

void n_fast_selects(TestConnections* pTest, int n)
{
    auto& mxt = *pTest->maxscale;

    Connection c(mxt.ip4(), 4006, username, password);

    if (c.connect())
    {
        for (int i = 0; i < n; ++i)
        {
            c.query("SELECT 1");
        }
    }
    else
    {
        pTest->expect(false, "Could not connect to MaxScale.");
    }
}

void busy_fast_selects(TestConnections* pTest, std::atomic<bool>* pStop)
{
    auto& mxt = *pTest->maxscale;

    Connection c(mxt.ip4(), 4006, username, password);

    if (c.connect())
    {
        auto& stop = *pStop;

        while (!stop)
        {
            c.query("SELECT 1");
        }
    }
    else
    {
        pTest->expect(false, "Could not connect to MaxScale.");
    }
}

void busy_slow_selects(TestConnections* pTest, std::atomic<bool>* pStop)
{
    auto& mxt = *pTest->maxscale;

    Connection c(mxt.ip4(), 4006, username, password);

    if (c.connect())
    {
        auto& stop = *pStop;

        while (!stop)
        {
            c.query("BEGIN");
            c.query("SELECT SLEEP(5)");
            c.query("COMMIT");
        }
    }
    else
    {
        pTest->expect(false, "Could not connect to MaxScale.");
    }
}

/**
 * Hard case, concurrent activity ongoing.
 */
void test_hard_setup(TestConnections& test, const std::string& main_server, const std::string& other_server)
{
    cout << "Hard case, concurrent activity ongoing." << endl;

    // Setup

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", main_server, other_server);

    std::atomic<bool> stop { false };
    vector<std::thread> clients;

    size_t i = 0;
    for (; i < 5; ++i)
    {
        clients.emplace_back(busy_fast_selects, &diff.test(), &stop);
    }

    for (; i < 10; ++i)
    {
        clients.emplace_back(busy_slow_selects, &diff.test(), &stop);
    }

    sleep(1);

    mxb::Json json;

    json = diff.start();

    diff.wait_for_state(json, "comparing");

    // Tear down

    json = diff.stop();

    sleep(1);

    stop = true;

    diff.wait_for_state(json, "created");

    for (i = 0; i < clients.size(); ++i)
    {
        clients[i].join();
    }

    diff.destroy();
}

/**
 * Hard case, abort setup.
 */
void test_abort_setup(TestConnections& test, const std::string& main_server, const std::string& other_server)
{
    cout << "Hard case, setup is aborted." << endl;

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", main_server, other_server);

    std::atomic<bool> stop { false };
    vector<std::thread> clients;

    size_t i = 0;
    for (; i < 5; ++i)
    {
        clients.emplace_back(busy_slow_selects, &diff.test(), &stop);
    }

    mxb::Json json;

    json = diff.start();

    // The slow selects take 5 seconds. Thus, waiting for the "comparing" state for
    // 2 seconds, should fail.
    bool started = diff.wait_for_state(json, "comparing", 2);

    test.expect(!started, "Diff should not have started.");

    diff.stop();

    json = diff.status();

    auto meta = json.get_object("meta");
    auto state = meta.get_string("state");

    test.expect(state == "created", "Should have been back at 'created' state.");

    diff.destroy();

    stop = true;

    for (i = 0; i < clients.size(); ++i)
    {
        clients[i].join();
    }
}

/**
 * Ensure that EXPLAINs are made by turning on 'explain_always'.
 */
void test_with_explain(TestConnections& test, const std::string& main_server, const std::string& other_server)
{
    cout << "Testing with EXPLAINs." << endl;

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", main_server, other_server);

    int samples = 100;
    diff.set_samples(samples); // Less samples so that we dont have to wait for so long.
    diff.set_explain_always(true); // Always EXPLAIN
    diff.set_explain_period(std::chrono::milliseconds { 2000 }); // Short period, to trigger activities.

    mxb::Json json = diff.start();

    bool started = diff.wait_for_state(json, "comparing", 2);

    n_fast_selects(&test, samples + 1);

    std::atomic<bool> stop { false };
    vector<std::thread> clients;

    size_t i = 0;
    for (; i < 5; ++i)
    {
        clients.emplace_back(busy_fast_selects, &diff.test(), &stop);
    }

    sleep(5);

    stop = true;

    for (i = 0; i < clients.size(); ++i)
    {
        clients[i].join();
    }

    diff.stop();
    diff.destroy();
}

void test_with_other_being_a_slave(TestConnections& test)
{
    cout << "Testing with main being master, and other being a slave of master.\n"
         << "------------------------------------------------------------------" << endl;

    test_easy_setup(test, "server1", "server2");
    test_hard_setup(test, "server1", "server2");
    test_abort_setup(test, "server1", "server2");
    test_duplication(test, "server1", 0, "server2", 1);
    test_with_explain(test, "server1", "server2");
}

void test_with_main_and_other_being_peers(TestConnections& test)
{
    cout << "Testing with main and other being slaves of common master.\n"
         << "----------------------------------------------------------" << endl;

    MaxRest maxrest(&test);

    maxrest.fail_on_error(false);

    try
    {
        // This should fail, because after the previous tests, "server2" is not
        // replicating from "server1", and hence its (MaxScale) state will not
        // be 'slave'.
        Diff::create(maxrest, "DiffMyService", "MyService", "server2", "server3");

        test.expect(false, "Creation of DiffService should have failed.");
    }
    catch (const std::exception& x)
    {
        cout << "Creation failed, as expected." << endl;
    }

    maxrest.fail_on_error(true);

    auto c = test.repl->get_connection(1); // I.e. "server2"

    test.expect(c.connect(), "Could not connect to 'server2'");
    test.expect(c.query("START SLAVE"), "Could not start replication.");

    test.maxscale->wait_for_monitor(2);

    test_easy_setup(test, "server2", "server3");
    test_hard_setup(test, "server2", "server3");
    test_abort_setup(test, "server2", "server3");
    test_with_explain(test, "server2", "server3");
}

void test_error_handling(TestConnections& test)
{
    cout << "Testing error handling.\n"
         << "-----------------------" << endl;

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", "server1", "server2");

    mxb::Json status = diff.start();

    diff.wait_for_state(status, "comparing", 2);

    auto& maxscale = *test.maxscale;

    Connection c(maxscale.ip4(), 4006, username, password);

    test.expect(c.connect(), "Could not connect to MaxScale.");

    test.expect(c.query("SELECT 1"), "Query failed.");

    cout << "Stopping other server2, everything shold continue." << endl;
    test.expect(test.repl->stop_node(1) == 0, "Could not stop MariaDB on server2.");

    // This should continue to work.
    test.expect(c.query("SELECT 1"), "Query failed.");

    diff.stop();
    diff.destroy();
}

void test_main(TestConnections& test)
{
    username = test.repl->user_name();
    password = test.repl->password();

    test_with_other_being_a_slave(test);
    test_with_main_and_other_being_peers(test);
    test_error_handling(test);
}

}

ENTERPRISE_TEST_MAIN(test_main)
