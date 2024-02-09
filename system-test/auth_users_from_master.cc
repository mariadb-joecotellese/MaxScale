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
 * @file bug592.cpp  regression case for bug 592 ( "slave in "Running" state breaks authorization" ) MXS-326
 *
 * - stop all slaves: "stop slave;" directly to every node (now they are in "Running" state, not in "Russning,
 * Slave")
 * - via RWSplit "CREATE USER 'test_user'@'%' IDENTIFIED BY 'pass'"
 * - try to connect using 'test_user' (expecting success)
 * - start all slaves: "start slave;" directly to every node
 * - via RWSplit: "DROP USER 'test_user'@'%'"
 */

/*
 *  Timofey Turenko 2014-10-24 09:35:35 UTC
 *  1. setup: Master/Slave replication
 *  2. reboot slaves
 *  3. create user usinf connection to RWSplit
 *  4. try to use this user to connect to Maxscale
 *
 *  expected result:
 *  Authentication is ok
 *
 *  actual result:
 *  Access denied for user 'user'@'192.168.122.1' (using password: YES)
 *
 *  Th issue was discovered with following setup state:
 *
 *  MaxScale> show servers
 *  Server 0x3428260 (server1)
 *   Server:             192.168.122.106
 *   Status:                     Master, Running
 *   Protocol:           MySQLBackend
 *   Port:               3306
 *   Server Version:         5.5.40-MariaDB-log
 *   Node Id:            106
 *   Master Id:          -1
 *   Slave Ids:          107, 108 , 109
 *   Repl Depth:         0
 *   Number of connections:      0
 *   Current no. of conns:       0
 *   Current no. of operations:  0
 *  Server 0x3428160 (server2)
 *   Server:             192.168.122.107
 *   Status:                     Slave, Running
 *   Protocol:           MySQLBackend
 *   Port:               3306
 *   Server Version:         5.5.40-MariaDB-log
 *   Node Id:            107
 *   Master Id:          106
 *   Slave Ids:
 *   Repl Depth:         1
 *   Number of connections:      0
 *   Current no. of conns:       0
 *   Current no. of operations:  0
 *  Server 0x3428060 (server3)
 *   Server:             192.168.122.108
 *   Status:                     Running
 *   Protocol:           MySQLBackend
 *   Port:               3306
 *   Server Version:         5.5.40-MariaDB-log
 *   Node Id:            108
 *   Master Id:          106
 *   Slave Ids:
 *   Repl Depth:         1
 *   Number of connections:      0
 *   Current no. of conns:       0
 *   Current no. of operations:  0
 *  Server 0x338c3f0 (server4)
 *   Server:             192.168.122.109
 *   Status:                     Running
 *   Protocol:           MySQLBackend
 *   Port:               3306
 *   Server Version:         5.5.40-MariaDB-log
 *   Node Id:            109
 *   Master Id:          106
 *   Slave Ids:
 *   Repl Depth:         1
 *   Number of connections:      0
 *   Current no. of conns:       0
 *   Current no. of operations:  0
 *
 *
 *  Maxscale read mysql.user table from server4 which was not properly replicated
 *  Comment 1 Mark Riddoch 2014-11-05 09:55:07 UTC
 *  In the reload users routine, if there is a master available then use that rather than the first.
 */



#include <iostream>
#include <maxtest/testconnections.hh>

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    int i;

    test.repl->connect();
    test.maxscale->connect_maxscale();

    for (i = 1; i < test.repl->N; i++)
    {
        execute_query(test.repl->nodes[i], (char*) "stop slave;");
    }

    execute_query(test.maxscale->conn_rwsplit,
                  (char*) "CREATE USER 'test_user'@'%%' IDENTIFIED BY 'pass'");

    MYSQL* conn = open_conn_no_db(test.maxscale->rwsplit_port,
                                  test.maxscale->ip4(),
                                  (char*) "test_user",
                                  (char*) "pass",
                                  test.maxscale_ssl);

    if (conn == NULL)
    {
        test.add_result(1, "Connections error\n");
    }

    for (i = 1; i < test.repl->N; i++)
    {
        execute_query(test.repl->nodes[i], (char*) "start slave;");
    }

    execute_query(test.maxscale->conn_rwsplit, (char*) "DROP USER 'test_user'@'%%'");

    test.repl->close_connections();
    test.maxscale->close_maxscale_connections();

    mysql_close(conn);

    return test.global_result;
}
