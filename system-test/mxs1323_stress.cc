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
 * Test for MXS-1323.
 * - Check that retried reads work with persistent connections
 */

#include <maxtest/testconnections.hh>
#include <sstream>

static bool running = true;

void* async_query(void* data)
{
    TestConnections* test = (TestConnections*)data;

    while (running && test->global_result == 0)
    {
        MYSQL* conn = test->maxscale->open_rwsplit_connection();

        for (int i = 0; i < 50 && running && test->global_result == 0; i++)
        {
            test->try_query(conn, "SET @a = (SELECT SLEEP(0.1))");
            test->try_query(conn, "SELECT SLEEP(1)");
        }

        mysql_close(conn);
    }

    return NULL;
}

#define NUM_THR 5

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    pthread_t query_thr[NUM_THR];
    std::stringstream ss;

    ss << "CREATE OR REPLACE TABLE test.t1 (id INT)";
    test.maxscale->connect_maxscale();
    test.try_query(test.maxscale->conn_rwsplit, "%s", ss.str().c_str());

    ss.str("");
    ss << "INSERT INTO test.t1 VALUES (0)";
    for (int i = 1; i <= 10000; i++)
    {
        ss << ",(" << i << ")";
    }
    test.try_query(test.maxscale->conn_rwsplit, "%s", ss.str().c_str());

    test.maxscale->close_maxscale_connections();

    if (test.global_result)
    {
        return test.global_result;
    }

    for (int i = 0; i < NUM_THR; i++)
    {
        pthread_create(&query_thr[i], NULL, async_query, &test);
    }

    for (int i = 0; i < 3 && test.global_result == 0; i++)
    {
        test.tprintf("Round %d", i + 1);
        test.repl->block_node(1);
        sleep(5);
        test.repl->unblock_node(1);
        sleep(5);
    }

    running = false;

    for (int i = 0; i < NUM_THR; i++)
    {
        test.reset_timeout();
        pthread_join(query_thr[i], NULL);
    }

    return test.global_result;
}
