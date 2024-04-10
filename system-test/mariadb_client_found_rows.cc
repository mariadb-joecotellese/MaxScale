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
 * @file bug565.cpp  regression case for bug 565 ( "Clients CLIENT_FOUND_ROWS setting is ignored by maxscale"
 *) MAX-311
 *
 * - open connection with CLIENT_FOUND_ROWS flag
 * - CREATE TABLE t1(id INT PRIMARY KEY, val INT, msg VARCHAR(100))
 * - INSERT INTO t1 VALUES (1, 1, 'foo'), (2, 1, 'bar'), (3, 2, 'baz'), (4, 2, 'abc')"
 * - check 'affected_rows' for folloing UPDATES:
 *   + UPDATE t1 SET msg='xyz' WHERE val=2" (expect 2)
 *   + UPDATE t1 SET msg='xyz' WHERE val=2 (expect 0)
 *   + UPDATE t1 SET msg='xyz' WHERE val=2 (expect 2)
 */

/*
 *  Hartmut Holzgraefe 2014-10-02 14:27:18 UTC
 *  Created attachment 155 [details]
 *  test for mysql_affected_rows() with/without CLIENT_FOUND_ROWS connection flag
 *
 *  Even worse: connections via maxscale always behave as if CLIENT_FOUND_ROWS is set even though the default
 * is NOT having it set.
 *
 *  When doing the same update two times in a row without CLIENT_FOUND_ROWS
 *  mysql_affected_rows() should return the number of rows actually changed
 *  by the last query, while with CLIENT_FOUND_ROWS connection flag set the
 *  number of matching rows is returned, even if the UPDATE didn't change
 *  any column values.
 *
 *  With a direct connection to mysqld this works as expected,
 *  through readconnroute(master) I'm always getting the number of matching
 *  rows (as if CLIENT_FOUND_ROWS was set), and not the number of actually
 *  changed rows when CLIENT_FOUND_ROWS is not set (which is the default
 *  behaviour when not setting connection options)
 *
 *  Attaching PHP mysqli test script, result with direct mysqld connection is
 *
 *  update #1: 2
 *  update #2: 0
 *  update #3: 2
 *
 *  while through maxscale it is
 *
 *  update #1: 2
 *  update #2: 2
 *  update #3: 2
 *
 *  I also verified this using the C API directly to rule out that this is
 *  a PHP specific problem
 *  Comment 1 Vilho Raatikka 2014-10-08 14:11:38 UTC
 *  Client flags are not passed to backend server properly.
 *  Comment 2 Vilho Raatikka 2014-10-08 19:35:58 UTC
 *  Pushed initial fix to MAX-311. Waiting for validation for the fix.
 */


#include <iostream>
#include <maxtest/testconnections.hh>

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    test.reset_timeout();
    MYSQL* conn_found_rows;
    my_ulonglong rows;


    test.repl->connect();
    test.maxscale->connect_maxscale();

    conn_found_rows = open_conn_db_flags(test.maxscale->rwsplit_port,
                                         test.maxscale->ip4(),
                                         (char*) "test",
                                         test.maxscale->user_name(),
                                         test.maxscale->password(),
                                         CLIENT_FOUND_ROWS,
                                         test.maxscale_ssl);

    test.reset_timeout();
    execute_query(test.maxscale->conn_rwsplit, "DROP TABLE IF EXISTS t1");
    execute_query(test.maxscale->conn_rwsplit,
                  "CREATE TABLE t1(id INT PRIMARY KEY, val INT, msg VARCHAR(100))");
    execute_query(test.maxscale->conn_rwsplit,
                  "INSERT INTO t1 VALUES (1, 1, 'foo'), (2, 1, 'bar'), (3, 2, 'baz'), (4, 2, 'abc')");

    test.reset_timeout();
    execute_query_affected_rows(test.maxscale->conn_rwsplit,
                                "UPDATE t1 SET msg='xyz' WHERE val=2",
                                &rows);
    test.tprintf("update #1: %ld (expeced value is 2)\n", (long) rows);
    if (rows != 2)
    {
        test.add_result(1, "Affected rows is not 2\n");
    }

    test.reset_timeout();
    execute_query_affected_rows(test.maxscale->conn_rwsplit,
                                "UPDATE t1 SET msg='xyz' WHERE val=2",
                                &rows);
    test.tprintf("update #2: %ld  (expeced value is 0)\n", (long) rows);
    if (rows != 0)
    {
        test.add_result(1, "Affected rows is not 0\n");
    }

    test.reset_timeout();
    execute_query_affected_rows(conn_found_rows, "UPDATE t1 SET msg='xyz' WHERE val=2", &rows);
    test.tprintf("update #3: %ld  (expeced value is 2)\n", (long) rows);
    if (rows != 2)
    {
        test.add_result(1, "Affected rows is not 2\n");
    }

    test.maxscale->close_maxscale_connections();

    mysql_close(conn_found_rows);

    return test.global_result;
}
