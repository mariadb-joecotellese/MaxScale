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
 * MXS-3339: Hang when COM_STMT_CLOSE is stored in the session command history
 */

#include <maxtest/testconnections.hh>

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    test.tprintf("Create a table");

    auto conn = test.maxscale->rwsplit();
    conn.set_timeout(15);
    test.expect(conn.connect(), "Connection should work: %s", conn.error());
    test.expect(conn.query("CREATE TABLE test.t1(id INT)"), "Query failed: %s", conn.error());
    test.expect(conn.query("INSERT INTO test.t1 VALUES (1), (2), (3)"), "Query failed: %s", conn.error());

    test.tprintf("Prepare a statement and close it immediately");

    MYSQL_STMT* stmt = conn.stmt();
    std::string query = "SELECT id FROM test.t1";

    test.expect(mysql_stmt_prepare(stmt, query.c_str(), query.length()) == 0,
                "Prepare failed: %s", mysql_stmt_error(stmt));
    mysql_stmt_close(stmt);

    test.tprintf("Block and unblock the slave to force a reconnection");

    test.repl->block_node(1);
    test.maxscale->wait_for_monitor(2);
    test.repl->unblock_node(1);

    test.tprintf("Execute a query on the master to force the next query to "
                 "the slave in case both classify as equally good");

    test.expect(conn.query("SELECT LAST_INSERT_ID()"), "Query should work: %s", conn.error());
    sleep(1);

    test.tprintf("Execute a query that is routed to a slave with a session command history");

    test.reset_timeout();
    test.expect(conn.query("SELECT 1"), "Query should work: %s", conn.error());

    test.tprintf("Cleanup");

    conn.disconnect();
    conn.connect();
    conn.query("DROP TABLE test.t1");

    return test.global_result;
}
