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
 * @file mxs812_2.cpp - Execute binary protocol prepared statements while master is blocked, checks "Current
 * no. of conns" after the test
 * - start threads which prepares and executes simple statement in the loop
 * - every 5 seconds block and after another 5 seconds unblock Master
 * - checks "Current no. of conns" after the test, expect 0
 */

#include <maxtest/testconnections.hh>

int test_ps(TestConnections* Test, MYSQL_STMT* stmt)
{
    const char select_stmt[] = "SELECT ?, ?, ?, ?";

    mysql_stmt_prepare(stmt, select_stmt, sizeof(select_stmt) - 1);

    int value = 1;
    MYSQL_BIND param[4];

    param[0].buffer_type = MYSQL_TYPE_LONG;
    param[0].is_null = 0;
    param[0].buffer = &value;
    param[1].buffer_type = MYSQL_TYPE_LONG;
    param[1].is_null = 0;
    param[1].buffer = &value;
    param[2].buffer_type = MYSQL_TYPE_LONG;
    param[2].is_null = 0;
    param[2].buffer = &value;
    param[3].buffer_type = MYSQL_TYPE_LONG;
    param[3].is_null = 0;
    param[3].buffer = &value;

    mysql_stmt_bind_param(stmt, param);
    mysql_stmt_execute(stmt);
    mysql_stmt_close(stmt);

    return 0;
}

static bool running = true;

void* test_thr(void* data)
{
    TestConnections* Test = (TestConnections*)data;

    while (running)
    {
        auto rws = Test->maxscale->rwsplit();

        if (rws.connect())
        {
            for (int i = 0; i < 3; i++)
            {
                test_ps(Test, rws.stmt());
            }
        }
        else
        {
            sleep(1);
        }
    }

    return NULL;
}

#define THREADS 5

int main(int argc, char* argv[])
{
    TestConnections* Test = new TestConnections(argc, argv);
    pthread_t thr[THREADS];
    int iter = 5;

    Test->tprintf("Starting %d query threads", THREADS);

    for (int i = 0; i < THREADS; i++)
    {
        pthread_create(&thr[i], NULL, test_thr, Test);
    }

    for (int i = 0; i < iter; i++)
    {
        Test->tprintf("Blocking master");
        Test->repl->block_node(0);
        Test->maxscale->wait_for_monitor();
        Test->tprintf("Unblocking master");
        Test->repl->unblock_node(0);
        Test->maxscale->wait_for_monitor();
    }

    running = false;

    Test->tprintf("Joining threads");
    for (int i = 0; i < THREADS; i++)
    {
        pthread_join(thr[i], NULL);
    }

    Test->check_maxscale_alive();
    Test->check_current_operations(0);

    int rval = Test->global_result;
    delete Test;
    return rval;
}
