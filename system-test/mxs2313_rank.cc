/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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
 * MXS-2313: `rank` functional tests
 * https://jira.mariadb.org/browse/MXS-2313
 */

#include <maxtest/testconnections.hh>
#include <iostream>

std::function<void(int)> block_wait;
std::function<void(int)> unblock_wait;

void test_rwsplit(TestConnections& test, std::vector<std::string> ids)
{
    std::cout << "Servers in two groups with different ranks" << std::endl;

    test.check_maxctrl("alter server server1 rank=primary");
    test.check_maxctrl("alter server server2 rank=primary");
    test.check_maxctrl("alter server server3 rank=secondary");
    test.check_maxctrl("alter server server4 rank=secondary");

    Connection c = test.maxscale->rwsplit();

    auto is_primary = [&]() {
            auto id = c.field("SELECT @@server_id");
            test.expect(id == ids[0] || id == ids[1], "Primary servers should reply");
        };
    auto is_secondary = [&]() {
            auto id = c.field("SELECT @@server_id");
            test.expect(id == ids[2] || id == ids[3], "Secondary servers should reply");
        };

    c.connect();
    is_primary();

    block_wait(0);
    is_primary();

    block_wait(1);
    is_secondary();

    block_wait(2);
    is_secondary();

    block_wait(3);
    test.expect(!c.query("SELECT @@server_id"), "Query should fail");

    unblock_wait(3);
    c.disconnect();
    c.connect();
    is_secondary();

    unblock_wait(2);
    is_secondary();

    unblock_wait(1);
    is_secondary();

    unblock_wait(0);
    is_secondary();

    test.expect(c.query("SELECT @@server_id, @@last_insert_id"), "Query should work");
    is_primary();

    std::cout << "Grouping servers into a three-node cluster with one low-ranking server" << std::endl;

    test.check_maxctrl("alter server server1 rank=primary");
    test.check_maxctrl("alter server server2 rank=primary");
    test.check_maxctrl("alter server server3 rank=primary");
    test.check_maxctrl("alter server server4 rank=secondary");

    c.disconnect();
    c.connect();

    block_wait(0);
    auto id = c.field("SELECT @@server_id");
    test.expect(!id.empty() && id != ids[3], "Third slave should not reply");

    block_wait(1);
    id = c.field("SELECT @@server_id");
    test.expect(!id.empty() && id != ids[3], "Third slave should not reply");

    block_wait(2);
    test.expect(c.field("SELECT @@server_id") == ids[3], "Third slave should reply");

    for (int i = 0; i < 3; i++)
    {
        unblock_wait(i);
        test.expect(c.field("SELECT @@server_id") == ids[3], "Third slave should reply");
    }

    block_wait(3);
    id = c.field("SELECT @@server_id");
    test.expect(!id.empty() && id != ids[3], "Third slave should not reply");
    unblock_wait(3);
}

void test_readconnroute(TestConnections& test, std::vector<std::string> ids)
{
    std::cout << "Readconnroute with descending server rank" << std::endl;

    test.check_maxctrl("alter server server1 rank=primary");
    test.check_maxctrl("alter server server2 rank=primary");
    test.check_maxctrl("alter server server3 rank=secondary");
    test.check_maxctrl("alter server server4 rank=secondary");

    auto do_test = [&](int node) {
            Connection c = test.maxscale->readconn_master();
            c.connect();
            test.expect(c.field("SELECT @@server_id") == ids[node], "server%d should reply", node + 1);
        };

    do_test(0);
    block_wait(0);
    do_test(1);
    block_wait(1);
    do_test(2);
    block_wait(2);
    do_test(3);
    unblock_wait(2);
    do_test(2);
    unblock_wait(1);
    do_test(1);
    unblock_wait(0);
    do_test(0);

    std::cout << "MXS-4132: Rank of the first server is ignored with router_options=master" << std::endl;

    test.check_maxctrl("alter service Read-Connection-Router router_options=master");
    test.check_maxctrl("stop monitor MySQL-Monitor");
    test.check_maxctrl("set server server2 master");
    test.check_maxctrl("set server server3 master");
    test.check_maxctrl("set server server4 master");
    test.check_maxctrl("alter server server2 rank=secondary");

    do_test(0);
    test.check_maxctrl("clear server server1 master");
    do_test(1);
    test.check_maxctrl("clear server server2 master");
    do_test(2);
    test.check_maxctrl("clear server server3 master");
    do_test(3);

    test.check_maxctrl("alter service Read-Connection-Router router_options=running");
    test.check_maxctrl("start monitor MySQL-Monitor");
}

void test_hints(TestConnections& test, std::vector<std::string> ids)
{
    std::cout << "Test that routing hints override server rank" << std::endl;

    test.check_maxctrl("alter server server1 rank=primary");
    test.check_maxctrl("alter server server2 rank=primary");
    test.check_maxctrl("alter server server3 rank=primary");
    test.check_maxctrl("alter server server4 rank=secondary");

    Connection c = test.maxscale->rwsplit();
    c.connect();

    auto id = c.field("SELECT @@server_id -- maxscale route to server server4");
    test.expect(!id.empty() && id == ids[3], "Third slave should reply");

    id = c.field("SELECT @@server_id -- maxscale route to slave");
    test.expect(!id.empty() && (id == ids[1] || id == ids[2]), "Primary slave should reply");

    id = c.field("SELECT @@server_id -- maxscale route to master");
    test.expect(!id.empty() && id == ids[0], "Master should reply");
}

void test_services(TestConnections& test, std::vector<std::string> ids)
{
    test.log_printf("Test that rank works with services");

    test.check_maxctrl("alter server server1 rank=primary");
    test.check_maxctrl("alter server server2 rank=primary");
    test.check_maxctrl("alter server server3 rank=primary");
    test.check_maxctrl("alter server server4 rank=primary");

    Connection c = test.maxscale->get_connection(4009);

    test.check_maxctrl("alter service service1 rank=primary");
    test.check_maxctrl("alter service service2 rank=secondary");
    test.check_maxctrl("alter service service3 rank=secondary");

    // service1 uses server1 and server2
    c.connect();
    test.expect(c.field("SELECT @@server_id") == ids[1], "Second slave should reply");

    test.check_maxctrl("alter service service1 rank=secondary");
    test.check_maxctrl("alter service service2 rank=primary");
    test.check_maxctrl("alter service service3 rank=secondary");

    // service2 uses server1 and server3
    c.connect();
    test.expect(c.field("SELECT @@server_id") == ids[2], "Third slave should reply");

    test.check_maxctrl("alter service service1 rank=secondary");
    test.check_maxctrl("alter service service2 rank=secondary");
    test.check_maxctrl("alter service service3 rank=primary");

    // service3 uses server1 and server4
    c.connect();
    test.expect(c.field("SELECT @@server_id") == ids[3], "Fourth slave should reply");

    // Set all serviecs to the same rank
    test.check_maxctrl("alter service service1 rank=secondary");
    test.check_maxctrl("alter service service2 rank=secondary");
    test.check_maxctrl("alter service service3 rank=secondary");

    c.connect();
    std::set<std::string> id_set(ids.begin() + 1, ids.end());
    test.expect(id_set.count(c.field("SELECT @@server_id")), "Any slave should reply");

    test.check_maxctrl("alter service service1 rank=primary");
    test.check_maxctrl("alter service service2 rank=primary");
    test.check_maxctrl("alter service service3 rank=primary");

    c.connect();
    test.expect(id_set.count(c.field("SELECT @@server_id")), "Any slave should reply");
}

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);

    block_wait = [&](int node) {
            std::cout << "Block server" << (node + 1) << std::endl;
            test.repl->block_node(node);
            test.maxscale->wait_for_monitor(2);
        };
    unblock_wait = [&](int node) {
            std::cout << "Unblock server" << (node + 1) << std::endl;
            test.repl->unblock_node(node);
            test.maxscale->wait_for_monitor(2);
        };

    test.repl->connect();
    auto ids = test.repl->get_all_server_ids_str();
    test.repl->disconnect();

    test_rwsplit(test, ids);
    test_readconnroute(test, ids);
    test_hints(test, ids);
    test_services(test, ids);

    return test.global_result;
}
