/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * MXS-359: Switch master mid-session
 *
 * https://jira.mariadb.org/browse/MXS-359
 */
#include <maxtest/testconnections.hh>

TestConnections* global_test;

void change_master(int next, int current)
{
    TestConnections& test = *global_test;
    test.maxctrl("stop monitor MySQL-Monitor");
    test.repl->connect();
    test.repl->change_master(current, next);
    test.repl->close_connections();
    test.maxctrl("start monitor MySQL-Monitor");
}

struct Test
{
    const char* query;
    bool        should_work;

    Test(const char* q = NULL, bool works = true)
        : query(q)
        , should_work(works)
    {
    }
};

void do_test(Test pre, Test post)
{
    TestConnections& test = *global_test;
    int rc;
    test.maxscale->connect_rwsplit();

    if (pre.query)
    {
        rc = execute_query_silent(test.maxscale->conn_rwsplit, pre.query);
        test.expect((rc == 0) == pre.should_work,
                    "Expected query '%s' to %s: %s",
                    pre.query,
                    pre.should_work ? "succeed" : "fail",
                    mysql_error(test.maxscale->conn_rwsplit));
    }

    change_master(1, 0);
    sleep(5);

    rc = execute_query_silent(test.maxscale->conn_rwsplit, post.query);
    test.expect((rc == 0) == post.should_work,
                "Expected query '%s' to %s: %s",
                post.query,
                post.should_work ? "succeed" : "fail",
                mysql_error(test.maxscale->conn_rwsplit));

    change_master(0, 1);
    test.maxscale->disconnect();

    sleep(5);
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    global_test = &test;

    // Prepare a table for testing
    test.maxscale->connect_rwsplit();
    test.try_query(test.maxscale->conn_rwsplit, "CREATE OR REPLACE TABLE test.t1(id INT)");
    test.repl->sync_slaves();
    test.maxscale->disconnect();

    test.tprintf("Check that write after change works");
    do_test({}, {"INSERT INTO test.t1 VALUES (1)"});

    test.tprintf("Check that write with open transaction fails");
    do_test({"START TRANSACTION"}, {"INSERT INTO test.t1 VALUES (1)", false});

    test.tprintf("Check that read with open read-only transaction works");
    do_test({"START TRANSACTION READ ONLY"}, {"SELECT 1"});

    test.tprintf("Check that write right after autocommit=0 works");
    do_test({"SET autocommit=0"}, {"INSERT INTO test.t1 VALUES (1)"});

    test.maxscale->connect_rwsplit();
    test.try_query(test.maxscale->conn_rwsplit, "DROP TABLE test.t1");
    test.maxscale->disconnect();

    return test.global_result;
}
