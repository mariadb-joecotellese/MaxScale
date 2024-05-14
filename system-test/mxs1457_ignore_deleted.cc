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
 * MXS-1457: Deleted servers are not ignored when users are loaded
 *
 * Check that a corrupt and deleted server is not used to load users
 */

#include <maxtest/testconnections.hh>

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);

    test.reset_timeout();
    test.repl->connect();
    execute_query(test.repl->nodes[0], "CREATE USER 'auth_test'@'%%' IDENTIFIED BY 'test'");
    execute_query(test.repl->nodes[0], "GRANT ALL ON *.* to 'auth_test'@'%%'");
    test.repl->sync_slaves();
    test.repl->close_connections();

    /**
     * The monitor needs to be stopped before the slaves are stopped to prevent
     * it from detecting the broken replication.
     */
    test.maxctrl("stop monitor MySQL-Monitor");
    // Stop slaves and drop the user on the master
    test.repl->stop_slaves();
    test.repl->connect();
    execute_query(test.repl->nodes[0], "DROP USER 'auth_test'@'%%'");
    test.repl->close_connections();

    test.reset_timeout();
    test.check_maxctrl("reload service RW-Split-Router");
    MYSQL* conn = open_conn_db(test.maxscale->rwsplit_port,
                               test.maxscale->ip(),
                               "test",
                               "auth_test",
                               "test",
                               false);
    test.add_result(mysql_errno(conn) == 0, "Connection with users from master should fail");
    mysql_close(conn);

    test.maxctrl("unlink service RW-Split-Router server1");
    conn = open_conn_db(test.maxscale->rwsplit_port,
                        test.maxscale->ip(),
                        "test",
                        "auth_test",
                        "test",
                        false);
    test.add_result(mysql_errno(conn), "Connection should be OK: %s", mysql_error(conn));
    test.try_query(conn, "SELECT 1");
    mysql_close(conn);

    test.reset_timeout();
    test.repl->connect();
    execute_query(test.repl->nodes[1], "START SLAVE");
    execute_query(test.repl->nodes[2], "START SLAVE");
    execute_query(test.repl->nodes[3], "START SLAVE");
    test.repl->sync_slaves();
    test.repl->close_connections();

    return test.global_result;
}
