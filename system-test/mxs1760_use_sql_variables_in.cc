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
 * MXS-1760: Adding use_sql_variables_in=master resulted in error "Client requests unknown
 * prepared statement ID '0' that does not map to an internal ID"
 *
 * https://jira.mariadb.org/browse/MXS-1760
 */

#include <maxtest/testconnections.hh>
#include <vector>
#include <iostream>

using namespace std;

const int NUM_STMT = 2000;

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    std::vector<MYSQL_STMT*> stmts;

    test.maxscale->connect();

    cout << "Setting variable @a to 123" << endl;
    mysql_query(test.maxscale->conn_rwsplit, "SET @a = 123");
    int rc = execute_query_check_one(test.maxscale->conn_rwsplit, "SELECT @a", "123");
    test.expect(rc == 0, "Text protocol should return 123 as the value of @a");

    cout << "Preparing and executing " << NUM_STMT << " prepared statements" << endl;
    for (int i = 0; i < NUM_STMT && test.global_result == 0; i++)
    {
        stmts.push_back(mysql_stmt_init(test.maxscale->conn_rwsplit));
        MYSQL_STMT* stmt = stmts.back();
        const char* query = "SELECT @a";
        test.add_result(mysql_stmt_prepare(stmt, query, strlen(query)),
                        "Failed to prepare: %s",
                        mysql_stmt_error(stmt));
    }

    for (auto stmt : stmts)
    {
        char buffer[100] = "";
        my_bool err = false;
        my_bool isnull = false;
        MYSQL_BIND bind[1] = {};

        bind[0].buffer_length = sizeof(buffer);
        bind[0].buffer = buffer;
        bind[0].error = &err;
        bind[0].is_null = &isnull;

        // Execute a write, should return the master's server ID

        test.add_result(mysql_stmt_execute(stmt), "Failed to execute: %s", mysql_stmt_error(stmt));
        test.add_result(mysql_stmt_bind_result(stmt, bind),
                        "Failed to bind result: %s",
                        mysql_stmt_error(stmt));

        while (mysql_stmt_fetch(stmt) == 0)
        {
        }

        test.add_result(strcmp(buffer, "123"), "Value is '%s', not '123'", buffer);
        mysql_stmt_close(stmt);
    }

    test.maxscale->disconnect();
    test.log_excludes("unknown prepared statement");

    return test.global_result;
}
