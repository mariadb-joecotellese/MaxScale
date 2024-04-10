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
 * MXS-1585: https://jira.mariadb.org/browse/MXS-1585
 *
 * Check that MaxScale doesn't crash when the master is set into maintenance
 * mode when master_failure_mode is fail_on_write.
 */

#include <maxtest/testconnections.hh>
#include <vector>
#include <atomic>

static std::atomic<bool> running {true};

void* query_thr(void* data)
{
    TestConnections* test = (TestConnections*)data;

    while (running)
    {
        MYSQL* mysql = test->maxscale->open_rwsplit_connection();

        while (running)
        {
            if (mysql_query(mysql, "INSERT INTO test.mxs1585 VALUES (1)")
                || mysql_query(mysql, "DELETE FROM test.mxs1585 LIMIT 100"))
            {
                break;
            }
        }

        mysql_close(mysql);
    }

    return NULL;
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    test.log_printf("Creating tables");
    test.maxscale->connect_maxscale();
    test.try_query(test.maxscale->conn_rwsplit, "DROP TABLE IF EXISTS test.mxs1585");
    test.try_query(test.maxscale->conn_rwsplit, "CREATE TABLE test.mxs1585(id INT) ENGINE=MEMORY");
    test.maxscale->close_maxscale_connections();

    std::vector<pthread_t> threads;
    threads.resize(100);

    for (auto& a : threads)
    {
        pthread_create(&a, NULL, query_thr, &test);
    }

    for (int i = 0; i < 2; i++)
    {
        for (int x = 1; x <= 2; x++)
        {
            test.log_printf("Set maintenance on server%d", x);
            test.maxscale->ssh_node_f(true, "maxctrl set server server%d maintenance", x);
            sleep(1);
            test.log_printf("Clear maintenance on server%d", x);
            test.maxscale->ssh_node_f(true, "maxctrl clear server server%d maintenance", x);
            sleep(2);
        }
    }

    running = false;
    test.reset_timeout();

    test.log_printf("Waiting for threads to exit");

    for (auto& a : threads)
    {
        test.reset_timeout();
        pthread_join(a, NULL);
    }

    test.log_printf("Cleanup");
    test.maxscale->connect_maxscale();
    test.try_query(test.maxscale->conn_rwsplit, "DROP TABLE test.mxs1585");
    test.check_maxscale_alive();

    return test.global_result;
}
