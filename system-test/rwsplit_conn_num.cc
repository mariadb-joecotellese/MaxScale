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
 * @file rwsplit_conn_num.cpp Checks connections are distributed equaly among backends
 * - create 100 connections to RWSplit
 * - check all slaves have equal number of connections
 * - check sum of number of connections to all slaves is equal to 100
 */


#include <iostream>
#include <maxtest/testconnections.hh>

using namespace std;

int main(int argc, char* argv[])
{
    TestConnections* Test = new TestConnections(argc, argv);
    Test->reset_timeout();

    Test->repl->connect();

    const int TestConnNum = 100;
    MYSQL* conn[TestConnNum];
    int conn_num;

    MYSQL* backend_conn;
    for (int i = 0; i < Test->repl->N; i++)
    {
        backend_conn = open_conn(Test->repl->port[i],
                                 Test->repl->ip4(i),
                                 Test->repl->user_name(),
                                 Test->repl->password(),
                                 Test->repl->ssl());
        execute_query(backend_conn, "SET GLOBAL max_connections = 200;");
        mysql_close(backend_conn);
    }

    Test->tprintf("Creating %d connections to RWSplit router\n", TestConnNum);
    for (int i = 0; i < TestConnNum; i++)
    {
        conn[i] = Test->maxscale->open_rwsplit_connection();
    }
    Test->tprintf("Waiting %d seconds\n", 2 * Test->repl->N);
    sleep(2 * Test->repl->N);
    Test->reset_timeout();

    int ConnFloor = floor((float)TestConnNum / (Test->repl->N - 1));
    int ConnCell = ceil((float)TestConnNum / (Test->repl->N - 1));
    int TotalConn = 0;

    Test->tprintf("Checking connections to Master: should be %d\n", TestConnNum);
    conn_num = get_conn_num(Test->repl->nodes[0],
                            Test->maxscale->ip(),
                            Test->maxscale->hostname(),
                            (char*) "test");
    if (conn_num != TestConnNum)
    {
        Test->add_result(1, "number of connections to Master is %d\n", conn_num);
    }

    Test->tprintf("Number of connections to each slave should be between %d and %d\n", ConnFloor, ConnCell);
    Test->tprintf("Checking connections to each node\n");
    for (int i = 1; i < Test->repl->N; i++)
    {
        Test->reset_timeout();
        conn_num =
            get_conn_num(Test->repl->nodes[i],
                         Test->maxscale->ip(),
                         Test->maxscale->hostname(),
                         (char*) "test");
        TotalConn += conn_num;
        Test->tprintf("Connections to node %d (%s):\t%d\n", i, Test->repl->ip4(i), conn_num);
        if ((conn_num > ConnCell) || (conn_num < ConnFloor))
        {
            Test->add_result(1, "wrong number of connections to node %d\n", i);
        }
    }
    Test->tprintf("Total number of connections %d\n", TotalConn);
    if (TotalConn != TestConnNum)
    {
        Test->add_result(1, "total number of connections is wrong\n");
    }
    for (int i = 0; i < TestConnNum; i++)
    {
        mysql_close(conn[i]);
    }

    int rval = Test->global_result;
    delete Test;
    return rval;
}
