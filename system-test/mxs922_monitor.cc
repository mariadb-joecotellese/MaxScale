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
 * @file mxs922_monitor.cpp MXS-922: Monitor creation test
 *
 */

#include <maxtest/config_operations.hh>

int main(int argc, char* argv[])
{
    TestConnections* test = new TestConnections(argc, argv);
    Config config(test);

    test->tprintf("Creating monitor");

    config.create_all_listeners();
    config.create_monitor("mysql-monitor", "mysqlmon", 500);
    config.reset();

    test->maxscale->wait_for_monitor();

    test->check_maxscale_alive();

    test->maxscale->ssh_node("maxctrl unlink monitor mysql-monitor server{0,1,2,3}", true);
    config.destroy_monitor("mysql-monitor");

    test->check_maxscale_alive();

    test->maxscale->ssh_node("for i in 0 1 2 3; do maxctrl clear server server$i running; done", true);

    test->add_result(test->maxscale->connect_maxscale() == 0, "Should not be able to connect");

    config.create_monitor("mysql-monitor2", "mysqlmon", 500);
    config.add_created_servers("mysql-monitor2");

    test->maxscale->wait_for_monitor();
    test->check_maxscale_alive();

    /** Try to alter the monitor user */
    test->maxscale->connect_maxscale();
    execute_query(test->maxscale->conn_rwsplit, "DROP USER 'test'@'%%'");
    execute_query(test->maxscale->conn_rwsplit, "CREATE USER 'test'@'%%' IDENTIFIED BY 'test'");
    execute_query(test->maxscale->conn_rwsplit, "GRANT ALL ON *.* TO 'test'@'%%'");
    test->maxscale->close_maxscale_connections();

    config.alter_monitor("mysql-monitor2", "user", "test");
    config.alter_monitor("mysql-monitor2", "password", "test");

    test->maxscale->wait_for_monitor();
    test->check_maxscale_alive();

    /** Remove the user */
    test->maxscale->connect_maxscale();
    execute_query(test->maxscale->conn_rwsplit, "DROP USER 'test'@'%%'");

    config.restart_monitors();

    /**
     * Make sure the server are in a bad state. This way we'll know that the
     * monitor is running if the states have changed and the query is
     * successful.
     */
    test->maxscale->ssh_node("for i in 0 1 2 3; do maxctrl clear server server$i running; done", true);

    test->maxscale->wait_for_monitor();
    test->add_result(execute_query_silent(test->maxscale->conn_rwsplit, "SELECT 1") == 0,
                     "Query should fail when monitor has wrong credentials");
    test->maxscale->close_maxscale_connections();

    for (int i = 0; i < test->repl->N; i++)
    {
        config.alter_server(i, "monitoruser", "skysql", "monitorpw", "skysql");
    }

    config.restart_monitors();
    test->maxscale->wait_for_monitor();
    test->check_maxscale_alive();

    int rval = test->global_result;
    delete test;
    return rval;
}
