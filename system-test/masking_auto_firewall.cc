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

#include <iostream>
#include <maxtest/testconnections.hh>

using namespace std;

namespace
{

void init(TestConnections& test)
{
    MYSQL* pMysql = test.maxscale->conn_rwsplit;

    test.try_query(pMysql, "DROP TABLE IF EXISTS masking_auto_firewall");
    test.try_query(pMysql, "CREATE TABLE masking_auto_firewall (a TEXT, b TEXT)");
    test.try_query(pMysql, "INSERT INTO masking_auto_firewall VALUES ('hello', 'world')");
}

enum class Expect
{
    FAILURE,
    SUCCESS
};

void test_one(TestConnections& test, const char* zQuery, Expect expect)
{
    MYSQL* pMysql = test.maxscale->conn_rwsplit;

    const char* zExpect = (expect == Expect::SUCCESS ? "SHOULD" : "should NOT");

    test.tprintf("Executing '%s', %s succeed.", zQuery, zExpect);
    int rv = execute_query_silent(pMysql, zQuery);

    if (expect == Expect::SUCCESS)
    {
        test.add_result(rv, "Could NOT execute query '%s'.", zQuery);
    }
    else
    {
        test.add_result(rv == 0, "COULD execute query '%s'.", zQuery);
    }
}

void test_one_ps(TestConnections& test, const char* zQuery, Expect expect)
{
    MYSQL* pMysql = test.maxscale->conn_rwsplit;

    MYSQL_STMT* pPs = mysql_stmt_init(pMysql);
    int rv = mysql_stmt_prepare(pPs, zQuery, strlen(zQuery));

    if (expect == Expect::SUCCESS)
    {
        test.add_result(rv, "Could NOT prepare statement.");
    }
    else
    {
        test.add_result(rv == 0, "COULD prepare statement.");
    }

    mysql_stmt_close(pPs);
}

void run(TestConnections& test)
{
    MYSQL* pMysql = test.maxscale->conn_rwsplit;

    int rv;

    // This SHOULD go through, a is simply masked.
    test_one(test, "SELECT a, b FROM masking_auto_firewall", Expect::SUCCESS);

    // This should NOT go through as a function is used with a masked column.
    test_one(test, "SELECT LENGTH(a), b FROM masking_auto_firewall", Expect::FAILURE);

    // This should NOT go through as a function is used with a masked column (that happens to be uppercase).
    test_one(test, "SELECT LENGTH(A), b FROM masking_auto_firewall", Expect::FAILURE);

    // This should NOT go through as a function is used with a masked column.
    test_one(test, "SELECT CAST(A as CHAR), b FROM masking_auto_firewall", Expect::FAILURE);

    // This SHOULD go through as a function is NOT used with a masked column
    // in a prepared statement.
    test_one(test, "PREPARE ps1 FROM 'SELECT a, LENGTH(b) FROM masking_auto_firewall'", Expect::SUCCESS);

    // This should NOT go through as a function is used with a masked column
    // in a prepared statement.
    test_one(test, "PREPARE ps2 FROM 'SELECT LENGTH(a), b FROM masking_auto_firewall'", Expect::FAILURE);

    rv = execute_query_silent(pMysql, "set @a = 'SELECT LENGTH(a), b FROM masking_auto_firewall'");
    test.add_result(rv, "Could NOT set variable.");
    // This should NOT go through as a prepared statement is prepared from a variable.
    test_one(test, "PREPARE ps3 FROM @a", Expect::FAILURE);

    // This SHOULD succeed as a function is NOT used with a masked column
    // in a binary prepared statement.
    test_one_ps(test, "SELECT a, LENGTH(b) FROM masking_auto_firewall", Expect::SUCCESS);

    // This should NOT succeed as a function is used with a masked column
    // in a binary prepared statement.
    test_one_ps(test, "SELECT LENGTH(a), b FROM masking_auto_firewall", Expect::FAILURE);

    // A failed preparation of a binary prepared statement seems to leave some
    // garbage that causes the returned results of subsequent statements to be
    // out of sync. Instead of figuring out the actual cause, we'll just close
    // and reopen the connection.
    test.add_result(test.maxscale->disconnect(), "Could NOT close RWS connection.");
    test.add_result(test.maxscale->connect_rwsplit(), "Could NOT open the RWS connection.");

    // This should NOT succeed as a masked column is used in a statement
    // defining a variable.
    test_one(test, "set @a = (SELECT a, b FROM masking_auto_firewall)", Expect::FAILURE);

    // This SHOULD succeed as a masked column is not used in the statment.
    test_one(test, "select 1 UNION select b FROM masking_auto_firewall", Expect::SUCCESS);

    // This should NOT succeed as a masked column is used in the statment.
    test_one(test, "select 1 UNION select a FROM masking_auto_firewall", Expect::FAILURE);

    // This should NOT succeed as a masked column is used in the statment.
    test_one(test, "select 1 UNION ALL select a FROM masking_auto_firewall", Expect::FAILURE);

    // This should NOT succeed as '*' is used in the statment.
    test_one(test, "select 1 UNION select * FROM masking_auto_firewall", Expect::FAILURE);

    // This SHOULD succeed as a masked column is not used in the statment.
    test_one(test, "select * FROM (select b from masking_auto_firewall) tbl", Expect::SUCCESS);

    // This SHOULD succeed as a masked column is not used in the statment.
    test_one(test, "select * FROM (select a as b from masking_auto_firewall) tbl", Expect::FAILURE);

    // This SHOULD succeed as '*' is used in the statment.
    test_one(test, "select * FROM (select * from masking_auto_firewall) tbl", Expect::FAILURE);

    // These SHOULD succeed as they do not access actual data, but wont unless
    // the parser has been extended to parse these statements or the masking filter
    // handles EXPLAIN|DESCRIBE|ANALYZE explicitly.
    test_one(test, "EXPLAIN select a from masking_auto_firewall", Expect::SUCCESS);
    test_one(test, "DESCRIBE select a from masking_auto_firewall", Expect::SUCCESS);
    test_one(test, "ANALYZE select a from masking_auto_firewall", Expect::SUCCESS);
}

void run_ansi_quotes(TestConnections& test)
{
    // This SHOULD go through as we have 'treat_string_arg_as_field=false"
    test_one(test, "select concat(\"a\") from masking_auto_firewall", Expect::SUCCESS);

    Connection c = test.maxscale->rwsplit();
    c.connect();

    test.expect(c.query("SET @@SQL_MODE = CONCAT(@@SQL_MODE, ',ANSI_QUOTES')"),
                "Could not turn on 'ANSI_QUOTES'");

    // This SHOULD still go through as we still have 'treat_string_arg_as_field=false"
    test_one(test, "select concat(\"a\") from masking_auto_firewall", Expect::SUCCESS);

    // Let's turn on 'treat_string_arg_as_field=true'
    test.maxscale->ssh_node(
        "sed -i -e "
        "'s/treat_string_arg_as_field=false/treat_string_arg_as_field=true/' "
        "/etc/maxscale.cnf",
        true);
    // and restart MaxScale
    test.maxscale->restart();

    // This should NOT go through as we have 'treat_string_arg_as_field=true" and ANSI_QUOTES.
    test_one(test, "select concat(\"a\") from masking_auto_firewall", Expect::FAILURE);

    // Have to reconnect as we restarted MaxScale.
    c.connect();
    test.expect(c.query("SET @@SQL_MODE = REPLACE(@@SQL_MODE, 'ANSI_QUOTES', '')"),
                "Could not turn off 'ANSI_QUOTES'");
}
}

int main(int argc, char* argv[])
{
    TestConnections::skip_maxscale_start(true);

    TestConnections test(argc, argv);

    std::string json_file("/masking_auto_firewall.json");
    std::string from = mxt::SOURCE_DIR + json_file;
    std::string to = test.maxscale->access_homedir() + json_file;

    if (test.maxscale->copy_to_node(from.c_str(), to.c_str()))
    {
        test.maxscale->ssh_node((std::string("chmod a+r ") + to).c_str(), true);
        test.maxscale->start();
        if (test.ok())
        {
            sleep(2);
            test.maxscale->wait_for_monitor();

            if (test.maxscale->connect_rwsplit() == 0)
            {
                init(test);
                run(test);
                run_ansi_quotes(test);
            }
            else
            {
                test.expect(false, "Could not connect to RWS.");
            }
        }
    }
    else
    {
        test.expect(false, "Could not copy masking file to MaxScale node.");
    }

    return test.global_result;
}
