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
 * @file mxs280_select_outfile.cpp bug mxs280 regression case ("SELECT INTO OUTFILE query succeeds even if
 * backed fails")
 *
 * - Create /tmp/t1.csv on all backends
 * - creat t1 table, put some data into it
 * - try SELECT * INTO OUTFILE '/tmp/t1.csv' FROM t1 and expect failure
 */


#include <iostream>
#include <unistd.h>
#include <maxtest/testconnections.hh>
#include <maxtest/sql_t1.hh>

using namespace std;

int main(int argc, char* argv[])
{
    int i;
    TestConnections* Test = new TestConnections(argc, argv);
    Test->reset_timeout();
    Test->maxscale->connect_maxscale();

    Test->tprintf("Create /tmp/t1.csv on all backend nodes\n");
    for (i = 0; i < Test->repl->N; i++)
    {
        Test->reset_timeout();
        Test->repl->ssh_node(i, (char*) "touch /tmp/t1.csv", true);
    }

    Test->add_result(create_t1(Test->maxscale->conn_rwsplit), "Error creating t1\n");
    Test->try_query(Test->maxscale->conn_rwsplit,
                    (char*) "INSERT INTO t1 (x1, fl) VALUES (0, 0), (1, 0)");

    if ((execute_query(Test->maxscale->conn_rwsplit,
                       (char*) "SELECT * INTO OUTFILE '/tmp/t1.csv' FROM t1;")) == 0)
    {
        Test->add_result(1, "SELECT INTO OUTFILE epected to fail, but it is OK\n");
    }



    Test->tprintf("Remove /tmp/t1.csv from all backend nodes\n");
    for (i = 0; i < Test->repl->N; i++)
    {
        Test->reset_timeout();
        Test->repl->ssh_node(i, (char*) "rm -rf /tmp/t1.csv", true);
    }

    Test->reset_timeout();
    sleep(5);

    int rval = Test->global_result;
    delete Test;
    return rval;
}
