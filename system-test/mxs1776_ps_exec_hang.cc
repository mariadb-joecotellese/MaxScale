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

#include <maxtest/testconnections.hh>
#include <iostream>
#include <functional>
#include <vector>

using namespace std;

struct Bind
{
    Bind()
    {
        bind.buffer = buffer;
        bind.buffer_type = MYSQL_TYPE_LONG;
        bind.error = &err;
        bind.is_null = &is_null;
        bind.length = &length;
    }

    MYSQL_BIND    bind;
    char          err = 0;
    char          is_null = 0;
    char          is_unsigned = 0;
    uint8_t       buffer[1024];
    unsigned long length = 0;
};

struct TestCase
{
    std::string                                     name;
    std::function<bool(MYSQL*, MYSQL_STMT*, Bind&)> func;
};

void run_test(TestConnections& test, TestCase test_case)
{
    test.maxscale->connect();

    MYSQL_STMT* stmt = mysql_stmt_init(test.maxscale->conn_rwsplit);
    std::string query = "SELECT * FROM test.t1";
    unsigned long cursor_type = CURSOR_TYPE_READ_ONLY;
    mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor_type);

    Bind bind;

    test.reset_timeout();

    if (mysql_stmt_prepare(stmt, query.c_str(), query.size()))
    {
        test.expect(false, "Prepared statement failure: %s", mysql_stmt_error(stmt));
    }

    cout << test_case.name << endl;
    test.expect(test_case.func(test.maxscale->conn_rwsplit, stmt, bind),
                "Test '%s' failed: %s %s", test_case.name.c_str(),
                mysql_error(test.maxscale->conn_rwsplit),
                mysql_stmt_error(stmt));

    mysql_stmt_close(stmt);

    test.expect(mysql_query(test.maxscale->conn_rwsplit, "SELECT 1") == 0,
                "Normal queries should work: %s", mysql_error(test.maxscale->conn_rwsplit));

    test.maxscale->disconnect();
}


int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);

    test.maxctrl("enable log-priority info");
    test.maxscale->connect();

    test.try_query(test.maxscale->conn_rwsplit, "CREATE OR REPLACE TABLE test.t1(id INT)");
    test.try_query(test.maxscale->conn_rwsplit, "BEGIN");

    for (int i = 0; i < 100; i++)
    {
        execute_query(test.maxscale->conn_rwsplit, "INSERT INTO test.t1 VALUES (%d)", i);
    }

    test.try_query(test.maxscale->conn_rwsplit, "COMMIT");
    test.maxscale->disconnect();
    test.repl->sync_slaves();

    vector<TestCase> tests =
    {
        {
            "Simple execute and fetch",
            [](MYSQL* conn, MYSQL_STMT* stmt, Bind& bind){
                bool rval = true;

                if (mysql_stmt_execute(stmt)
                    || mysql_stmt_bind_result(stmt, &bind.bind))
                {
                    rval = false;
                }

                while (mysql_stmt_fetch(stmt) == 0)
                {
                }

                return rval;
            }
        },
        {
            "Multiple overlapping executions without fetch",
            [](MYSQL* conn, MYSQL_STMT* stmt, Bind& bind){
                bool rval = true;

                if (mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt))
                {
                    rval = false;
                }

                return rval;
            }
        },
        {
            "Multiple overlapping executions with fetch",
            [](MYSQL* conn, MYSQL_STMT* stmt, Bind& bind){
                bool rval = true;

                if (mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_bind_result(stmt, &bind.bind))
                {
                    rval = false;
                }

                while (mysql_stmt_fetch(stmt) == 0)
                {
                }

                return rval;
            }
        },
        {
            "Execution of queries while fetching",
            [](MYSQL* conn, MYSQL_STMT* stmt, Bind& bind){
                bool rval = true;

                if (mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_bind_result(stmt, &bind.bind))
                {
                    rval = false;
                }

                while (mysql_stmt_fetch(stmt) == 0)
                {
                    mysql_query(conn, "SELECT 1");
                    mysql_free_result(mysql_store_result(conn));
                }

                return rval;
            }
        },
        {
            "Multiple overlapping executions and a query",
            [](MYSQL* conn, MYSQL_STMT* stmt, Bind& bind){
                bool rval = true;

                if (mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_stmt_execute(stmt)
                    || mysql_query(conn, "SET @a = 1"))
                {
                    rval = false;
                }

                return rval;
            }
        }
    };

    for (auto a : tests)
    {
        run_test(test, a);
    }

    return test.global_result;
}
