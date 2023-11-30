/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * Test runtime modification of router options
 */

#include <maxtest/testconnections.hh>
#include <vector>
#include <iostream>
#include <functional>

#define TEST(a) {#a, a}

void alter_readwritesplit(TestConnections& test)
{
    test.maxscale->wait_for_monitor();

    // Open a connection before and after setting master_failure_mode to fail_on_write
    Connection first = test.maxscale->rwsplit();
    Connection second = test.maxscale->rwsplit();
    Connection third = test.maxscale->rwsplit();
    test.maxscale->wait_for_monitor();

    first.connect();
    test.check_maxctrl("alter service RW-Split-Router master_failure_mode fail_on_write");
    second.connect();

    // Check that writes work for both connections
    test.expect(first.query("SELECT @@last_insert_id"),
                "Write to first connection should work: %s",
                first.error());
    test.expect(second.query("SELECT @@last_insert_id"),
                "Write to second connection should work: %s",
                second.error());

    // Block the master
    test.repl->block_node(0);
    test.maxscale->wait_for_monitor();

    // Check that reads work for the newer connection and fail for the older one
    test.expect(!first.query("SELECT 1"),
                "Read to first connection should fail.");
    test.expect(second.query("SELECT 1"),
                "Read to second connection should work: %s",
                second.error());

    // Unblock the master, restart Maxscale and check that changes are persisted
    test.repl->unblock_node(0);
    test.maxscale->wait_for_monitor();
    test.maxscale->restart();

    third.connect();
    test.expect(third.query("SELECT @@last_insert_id"),
                "Write to third connection should work: %s",
                third.error());

    test.repl->block_node(0);
    test.maxscale->wait_for_monitor();

    test.expect(third.query("SELECT 1"),
                "Read to third connection should work: %s",
                third.error());

    test.repl->unblock_node(0);
    test.maxscale->wait_for_monitor();
}

void alter_readconnroute(TestConnections& test)
{
    test.repl->connect();
    std::string master_id = test.repl->get_server_id_str(0);
    test.repl->disconnect();

    Connection conn = test.maxscale->readconn_master();

    for (int i = 0; i < 5; i++)
    {
        conn.connect();
        auto id = conn.field("SELECT @@server_id");
        test.expect(!id.empty(), "Expected a response: %s", conn.error());
        conn.disconnect();
        test.expect(id == master_id,
                    "First connection should use master: %s != %s",
                    id.c_str(),
                    master_id.c_str());
    }

    test.check_maxctrl("alter service Read-Connection-Router-Master router_options slave");

    for (int i = 0; i < 5; i++)
    {
        conn.connect();
        auto id = conn.field("SELECT @@server_id");
        test.expect(!id.empty(), "Expected a response: %s", conn.error());
        conn.disconnect();
        test.expect(id != master_id,
                    "Second connection should not use master: %s == %s",
                    id.c_str(),
                    master_id.c_str());
    }
}

void alter_schemarouter(TestConnections& test)
{
    Connection conn = test.maxscale->readconn_slave();
    conn.connect();
    test.expect(!conn.query("SELECT 1"), "Query before reconfiguration should fail");
    conn.disconnect();

    test.check_maxctrl("alter service SchemaRouter ignore_tables_regex \".*\"");

    conn.connect();
    test.expect(conn.query("SELECT 1"), "Query after reconfiguration should work: %s", conn.error());
    conn.disconnect();
}

void alter_unsupported(TestConnections& test)
{
    int rc = test.maxscale->ssh_node_f(true, "maxctrl alter service RW-Split-Router unknown parameter");
    test.expect(rc != 0, "Unknown router parameter should be detected");
    rc = test.maxscale->ssh_node_f(true, "maxctrl alter service RW-Split-Router filters Regex");
    test.expect(rc != 0, "Unsupported router parameter should be detected");
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    std::vector<std::pair<const char*, std::function<void(TestConnections&)>>> tests =
    {
        TEST(alter_readwritesplit),
        TEST(alter_readconnroute),
        TEST(alter_schemarouter),
        TEST(alter_unsupported)
    };

    for (auto& a : tests)
    {
        std::cout << a.first << std::endl;
        a.second(test);
    }

    return test.global_result;
}
