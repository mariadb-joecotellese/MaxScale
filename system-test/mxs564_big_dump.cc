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
 * @file mxs564_big_dump.cpp MXS-564 regression case ("Loading database dump through readwritesplit fails")
 * - configure Maxscale to use Galera cluster
 * - start several threads which are executing session command and then sending INSERT queries agaist RWSplit
 * router
 * - after a while block first slave
 * - after a while block second slave
 * - check that all INSERTs are ok
 * - repeat with both RWSplit and ReadConn master maxscales->routers[0]
 * - check Maxscale is alive
 */

#include <maxtest/testconnections.hh>
#include <maxtest/galera_cluster.hh>
#include <maxtest/sql_t1.hh>
#include <atomic>
#include <thread>
#include <vector>

static std::atomic<bool> running {true};

void query_thread(TestConnections* t)
{
    TestConnections& test = *t;     // For some reason CentOS 7 doesn't like passing references to std::thread
    std::string sql(1000000, '\0');
    create_insert_string(&sql[0], 1000, 2);

    MYSQL* conn1 = test.maxscale->open_rwsplit_connection();
    MYSQL* conn2 = test.maxscale->open_readconn_master_connection();

    test.add_result(mysql_errno(conn1), "Error connecting to readwritesplit: %s", mysql_error(conn1));
    test.add_result(mysql_errno(conn2), "Error connecting to readconnroute: %s", mysql_error(conn2));

    test.try_query(conn1, "SET SESSION SQL_LOG_BIN=0");
    test.try_query(conn2, "SET SESSION SQL_LOG_BIN=0");

    while (running)
    {
        test.try_query(conn1, "%s", sql.c_str());
        test.try_query(conn2, "%s", sql.c_str());
    }

    mysql_close(conn1);
    mysql_close(conn2);
}

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    std::set<int> slaves;

    for (int i = 0; i < 4; i++)
    {
        if (test.get_server_status(("server" + std::to_string(i + 1)).c_str()).count("Slave"))
        {
            slaves.insert(i);
        }
    }

    test.maxscale->connect();
    test.try_query(test.maxscale->conn_rwsplit, "DROP TABLE IF EXISTS t1");
    test.try_query(test.maxscale->conn_rwsplit, "CREATE TABLE t1 (x1 int, fl int)");
    test.maxscale->disconnect();

    std::vector<std::thread> threads;

    for (int i = 0; i < 4; i++)
    {
        threads.emplace_back(query_thread, &test);
    }

    for (auto&& i : slaves)
    {
        test.tprintf("Blocking node %d", i);
        test.galera->block_node(i);
        test.maxscale->wait_for_monitor();
    }

    test.tprintf("Unblocking nodes\n");

    for (auto&& i : slaves)
    {
        test.galera->unblock_node(i);
    }

    test.maxscale->wait_for_monitor();

    running = false;
    test.reset_timeout();
    test.tprintf("Waiting for all threads to exit");

    for (auto&& a : threads)
    {
        a.join();
    }

    test.maxscale->connect();
    execute_query(test.maxscale->conn_rwsplit, "DROP TABLE t1");
    test.maxscale->disconnect();

    return test.global_result;
}
