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
 * MXS-872: MaxScale doesn't understand roles
 *
 * https://jira.mariadb.org/browse/MXS-872
 */

#include <maxtest/testconnections.hh>
#include <vector>

using namespace std;

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    test.repl->connect();
    for (auto a : vector<string>({"DROP DATABASE IF EXISTS my_db",
                                  "CREATE DATABASE my_db",
                                  "DROP ROLE IF EXISTS dba",
                                  "CREATE ROLE dba",
                                  "GRANT SELECT ON my_db.* TO dba",
                                  "DROP USER IF EXISTS 'test'@'%'",
                                  "DROP USER IF EXISTS 'test2'@'%'",
                                  "CREATE USER 'test'@'%' IDENTIFIED BY 'test'",
                                  "CREATE USER 'test2'@'%' IDENTIFIED BY 'test2'",
                                  "GRANT dba TO 'test'@'%'",
                                  "GRANT dba TO 'test2'@'%'",
                                  "SET DEFAULT ROLE dba FOR 'test'@'%'"}))
    {
        test.try_query(test.repl->nodes[0], "%s", a.c_str());
    }

    // Wait for the users to replicate
    test.repl->sync_slaves();

    test.tprintf("Connect with a user that has a default role");
    MYSQL* conn =
        open_conn_db(test.maxscale->rwsplit_port, test.maxscale->ip4(), "my_db", "test", "test");
    test.expect(mysql_errno(conn) == 0, "Connection failed: %s", mysql_error(conn));
    char value[100] {};
    find_field(conn, "SELECT CURRENT_ROLE() AS role", "role", value);
    test.expect(strcmp(value, "dba") == 0, "Current role should be 'dba' but is: %s", value);
    mysql_close(conn);

    test.tprintf("Connect with a user that doesn't have a default role, expect failure");
    conn = open_conn_db(test.maxscale->rwsplit_port, test.maxscale->ip4(), "my_db", "test2", "test2");
    test.expect(mysql_errno(conn) != 0, "Connection should fail");
    mysql_close(conn);

    // Cleanup
    for (auto a : vector<string>({"DROP DATABASE IF EXISTS my_db",
                                  "DROP ROLE IF EXISTS dba",
                                  "DROP USER 'test'@'%'",
                                  "DROP USER 'test2'@'%'"}))
    {
        execute_query_silent(test.repl->nodes[0], "%s", a.c_str());
    }

    test.repl->disconnect();
    return test.global_result;
}
