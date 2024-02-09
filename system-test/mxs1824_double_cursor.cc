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
 * MXS-1824: Debug expection with two open cursors
 *
 * https://jira.mariadb.org/browse/MXS-1824
 */

#include <maxtest/testconnections.hh>

void double_cursor(TestConnections& test, MYSQL* conn)
{
    test.try_query(conn, "CREATE OR REPLACE TABLE test.t1(id int)");
    test.try_query(conn, "INSERT INTO test.t1 VALUES (1), (2), (3)");

    test.repl->connect();
    test.repl->sync_slaves();
    test.repl->disconnect();

    MYSQL_STMT* stmt1 = mysql_stmt_init(conn);
    const char* query = "SELECT id FROM test.t1";
    int rc = mysql_stmt_prepare(stmt1, query, strlen(query));
    test.expect(rc == 0, "First prepare should work: %s %s", mysql_stmt_error(stmt1), mysql_error(conn));
    unsigned long type = CURSOR_TYPE_READ_ONLY;
    test.expect(mysql_stmt_attr_set(stmt1, STMT_ATTR_CURSOR_TYPE, &type) == 0,
                "Set of first attribute should work: %s %s",
                mysql_stmt_error(stmt1),
                mysql_error(conn));

    MYSQL_BIND bind[1] {};
    uint32_t id;
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &id;
    mysql_stmt_bind_result(stmt1, bind);

    test.expect(mysql_stmt_execute(stmt1) == 0,
                "Execute of first statement should work: %s %s",
                mysql_stmt_error(stmt1),
                mysql_error(conn));
    test.expect(mysql_stmt_fetch(stmt1) == 0,
                "First fetch should work: %s %s",
                mysql_stmt_error(stmt1),
                mysql_error(conn));

    MYSQL_STMT* stmt2 = mysql_stmt_init(conn);
    rc = mysql_stmt_prepare(stmt2, query, strlen(query));
    test.expect(rc == 0, "Second prepare should work: %s %s", mysql_stmt_error(stmt2), mysql_error(conn));
    test.expect(mysql_stmt_attr_set(stmt2, STMT_ATTR_CURSOR_TYPE, &type) == 0,
                "Set of second attribute should work: %s %s",
                mysql_stmt_error(stmt2),
                mysql_error(conn));
    mysql_stmt_bind_result(stmt2, bind);

    test.expect(mysql_stmt_execute(stmt2) == 0,
                "Execute of second statement should work: %s %s",
                mysql_stmt_error(stmt2),
                mysql_error(conn));
    test.expect(mysql_stmt_fetch(stmt2) == 0,
                "Second fetch should work: %s %s",
                mysql_stmt_error(stmt2),
                mysql_error(conn));
    mysql_stmt_reset(stmt2);

    test.expect(mysql_stmt_fetch(stmt1) == 0,
                "Third fetch should work: %s %s",
                mysql_stmt_error(stmt1),
                mysql_error(conn));

    mysql_stmt_close(stmt1);
    mysql_stmt_close(stmt2);

    test.try_query(conn, "DROP TABLE test.t1");
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    test.maxctrl("enable log-priority info");
    test.maxscale->connect();
    double_cursor(test, test.maxscale->conn_rwsplit);
    test.maxscale->disconnect();

    return test.global_result;
}
