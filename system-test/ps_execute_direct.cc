/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * MXS-2490: Unknown prepared statement handler (0) given to mysqld_stmt_execute
 * MXS-3392: Connection reset fails after execute_direct for an unknown table
 *
 * See:
 *
 * https://mariadb.com/kb/en/library/mariadb_stmt_execute_direct/
 * https://mariadb.com/kb/en/library/com_stmt_execute/#statement-id
 */

#include <maxtest/testconnections.hh>

void mxs2490(TestConnections& test, MYSQL* conn)
{
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    std::string query = "SELECT user FROM mysql.user";

    for (int i = 0; i < 10 && test.ok(); i++)
    {
        test.expect(mariadb_stmt_execute_direct(stmt, query.c_str(), query.length()) == 0,
                    "execute_direct should work: %s", mysql_stmt_error(stmt));
    }

    mysql_stmt_close(stmt);
}

void mxs3392(TestConnections& test, MYSQL* conn)
{
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    test.expect(mariadb_stmt_execute_direct(stmt, "SELECT 1 FROM test.nonexisting_table", -1),
                "Direct execution should fail");
    test.expect(mysql_stmt_close(stmt) == 0, "Closing the statement should work: %s", mysql_error(conn));
    test.expect(mysql_reset_connection(conn) == 0, "Connection reset should work: %s", mysql_error(conn));
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    test.reset_timeout();
    test.maxscale->connect();

    test.tprintf("MXS-2490: PS direct execution");
    test.tprintf("Testing readwritesplit");
    mxs2490(test, test.maxscale->conn_rwsplit);
    test.tprintf("Testing readconnroute");
    mxs2490(test, test.maxscale->conn_master);

    test.tprintf("MXS-3392: mariadb_stmt_execute_direct sends send an extra error");
    mxs3392(test, test.maxscale->conn_rwsplit);

    return test.global_result;
}
