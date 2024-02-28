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
 * Readwritesplit multi-statment test
 *
 * - Configure strict multi-statement mode
 * - Execute multi-statment query
 * - All queries should go to the master
 * - Configure for relaxed multi-statement mode
 * - Execute multi-statment query
 * - Only the multi-statement query should go to the master
 */

#include <maxtest/testconnections.hh>

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    char master_id[200];
    char slave_id[200];

    // Get the server IDs of the master and the slave
    test.repl->connect();
    sprintf(master_id, "%d", test.repl->get_server_id(0));
    sprintf(slave_id, "%d", test.repl->get_server_id(1));

    test.maxscale->connect_rwsplit();
    test.tprintf("Configuration: strict_multi_stmt=true");

    test.add_result(execute_query_check_one(test.maxscale->conn_rwsplit,
                                            "SELECT @@server_id",
                                            slave_id),
                    "Query should be routed to slave");

    test.add_result(execute_query_check_one(test.maxscale->conn_rwsplit,
                                            "USE test; SELECT @@server_id",
                                            master_id),
                    "Query should be routed to master");

    test.add_result(execute_query_check_one(test.maxscale->conn_rwsplit,
                                            "SELECT @@server_id",
                                            master_id),
                    "All queries should be routed to master");

    test.maxscale->disconnect();

    // Reconfigure MaxScale
    test.maxscale->ssh_node(
        "sed -i 's/strict_multi_stmt=true/strict_multi_stmt=false/' /etc/maxscale.cnf",
        true);
    test.maxscale->restart_maxscale();

    test.maxscale->connect_rwsplit();
    test.tprintf("Configuration: strict_multi_stmt=false");

    test.add_result(execute_query_check_one(test.maxscale->conn_rwsplit,
                                            "SELECT @@server_id",
                                            slave_id),
                    "Query should be routed to slave");

    test.add_result(execute_query_check_one(test.maxscale->conn_rwsplit,
                                            "USE test; SELECT @@server_id",
                                            master_id),
                    "Query should be routed to master");

    test.add_result(execute_query_check_one(test.maxscale->conn_rwsplit,
                                            "SELECT @@server_id",
                                            slave_id),
                    "Query should be routed to slave");

    test.maxscale->disconnect();

    return test.global_result;
}
