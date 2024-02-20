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

#include <iostream>
#include <unistd.h>
#include <maxtest/testconnections.hh>
#include <maxtest/replication_cluster.hh>

using namespace std;

char* create_event_size(unsigned long size)
{
    char* prefix = (char*) "insert into test.large_event values (1, '";
    unsigned long prefix_size = strlen(prefix);
    char* postfix = (char*) "');";
    char* event = (char*)malloc(size + 1);
    strcpy(event, prefix);

    unsigned long max = size - 55 - 45;


    // printf("BLOB data size %lu\n", max);

    for (unsigned long i = 0; i < max; i++)
    {
        event[i + prefix_size] = 'a';
    }

    strcpy((char*) event + max + prefix_size, postfix);
    return event;
}

MYSQL* connect_to_serv(TestConnections* Test, bool binlog)
{
    MYSQL* conn;
    if (binlog)
    {
        conn = open_conn(Test->repl->port(0), Test->repl->ip4(0),
                         Test->repl->user_name(), Test->repl->password(),
                         Test->maxscale_ssl);
    }
    else
    {
        conn = Test->maxscale->open_rwsplit_connection();
    }
    return conn;
}

void set_max_packet(TestConnections* Test, bool binlog, char* cmd)
{
    Test->tprintf("Setting maximum packet size ...");
    if (binlog)
    {
        Test->repl->connect();
        Test->try_query(Test->repl->nodes[0], "%s", cmd);
        Test->repl->close_connections();
    }
    else
    {
        Test->maxscale->connect_maxscale();
        Test->try_query(Test->maxscale->conn_rwsplit, "%s", cmd);
        Test->maxscale->close_maxscale_connections();
    }
    Test->tprintf(".. done\n");
}

void different_packet_size(TestConnections* Test, bool binlog)
{
    Test->tprintf("Set big max_allowed_packet\n");
    set_max_packet(Test, binlog, (char*) "set global max_allowed_packet = 200000000;");

    Test->tprintf("Create table\n");
    MYSQL* conn = connect_to_serv(Test, binlog);
    Test->try_query(conn,
                    "DROP TABLE IF EXISTS test.large_event;"
                    "CREATE TABLE test.large_event(id INT, data LONGBLOB);");
    mysql_close(conn);

    const int loops = 3;
    const int range = 2;

    for (int i = 1; i <= loops; i++)
    {
        for (int j = -range; j <= range; j++)
        {
            size_t size = 0x0ffffff * i + j;
            Test->tprintf("Trying event app. %lu bytes", size);

            char* event = create_event_size(size);
            conn = connect_to_serv(Test, binlog);
            Test->expect(execute_query_silent(conn, event) == 0, "Query should succeed");
            free(event);
            execute_query_silent(conn, (char*) "DELETE FROM test.large_event");
            mysql_close(conn);
        }
    }

    Test->tprintf("Restoring max_allowed_packet");
    set_max_packet(Test, binlog, (char*) "set global max_allowed_packet = 16777216;");

    conn = connect_to_serv(Test, binlog);
    Test->try_query(conn, "DROP TABLE test.large_event");
    mysql_close(conn);
}
