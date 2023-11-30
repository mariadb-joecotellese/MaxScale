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
 * @file prepared_statement.cpp Checks if prepared statement works via Maxscale
 *
 * - Create table t1 and fill it ith some data
 * - via RWSplit:
 *   + PREPARE stmt FROM 'SELECT * FROM t1 WHERE fl=@x;';
 *   + SET @x = 3;")
 *   + EXECUTE stmt")
 *   + SET @x = 4;")
 *   + EXECUTE stmt")
 * - check if Maxscale is alive
 */

#include <maxtest/testconnections.hh>
#include <maxtest/sql_t1.hh>

using namespace std;

void test_basic(TestConnections& test)
{
    test.reset_timeout();
    int N = 4;

    test.repl->connect();
    test.maxscale->connect_maxscale();

    create_t1(test.maxscale->conn_rwsplit);
    insert_into_t1(test.maxscale->conn_rwsplit, N);

    test.reset_timeout();
    test.try_query(test.maxscale->conn_rwsplit, "PREPARE stmt FROM 'SELECT * FROM t1 WHERE fl=@x;';");
    test.try_query(test.maxscale->conn_rwsplit, "SET @x = 3;");
    test.try_query(test.maxscale->conn_rwsplit, "EXECUTE stmt");
    test.try_query(test.maxscale->conn_rwsplit, "SET @x = 4;");
    test.try_query(test.maxscale->conn_rwsplit, "EXECUTE stmt");

    test.check_maxscale_alive();
}

void test_routing(TestConnections& test)
{
    test.reset_timeout();
    test.repl->connect();
    int server_id = test.repl->get_server_id(0);
    test.maxscale->connect_maxscale();

    // Test that reads are routed to slaves
    char buf[1024] = "-1";
    test.try_query(test.maxscale->conn_rwsplit, "PREPARE ps1 FROM 'SELECT @@server_id'");

    // Sleep so that the slave has time to execute the prepare
    sleep(3);

    test.add_result(find_field(test.maxscale->conn_rwsplit, "EXECUTE ps1", "@@server_id", buf),
                    "Execute should succeed");
    int res = atoi(buf);
    test.add_result(res == server_id,
                    "Query should be routed to a slave (got %d, master is %d)",
                    res,
                    server_id);


    // Test reads inside transactions are routed to master
    strcpy(buf, "-1");
    test.try_query(test.maxscale->conn_rwsplit, "BEGIN");
    test.add_result(find_field(test.maxscale->conn_rwsplit, "EXECUTE ps1", "@@server_id", buf),
                    "Execute should succeed");
    res = atoi(buf);
    test.add_result(res != server_id,
                    "Query should be routed to master inside a transaction (got %d, master is %d)",
                    res,
                    server_id);
    test.try_query(test.maxscale->conn_rwsplit, "COMMIT");

    // Test reads inside read-only transactions are routed slaves
    strcpy(buf, "-1");
    test.try_query(test.maxscale->conn_rwsplit, "START TRANSACTION READ ONLY");
    test.add_result(find_field(test.maxscale->conn_rwsplit, "EXECUTE ps1", "@@server_id", buf),
                    "Execute should succeed");
    res = atoi(buf);
    test.add_result(res == server_id,
                    "Query should be routed to a slave inside a read-only transaction (got %d, master is %d)",
                    res,
                    server_id);
    test.try_query(test.maxscale->conn_rwsplit, "COMMIT");

    // Test prepared statements that modify data
    strcpy(buf, "-1");
    test.try_query(test.maxscale->conn_rwsplit, "CREATE OR REPLACE TABLE test.t1 (id INT)");
    test.try_query(test.maxscale->conn_rwsplit, "PREPARE ps2 FROM 'INSERT INTO test.t1 VALUES (?)'");
    test.try_query(test.maxscale->conn_rwsplit, "SET @a = @@server_id");
    test.try_query(test.maxscale->conn_rwsplit, "EXECUTE ps2 USING @a");
    test.reset_timeout();
    test.repl->sync_slaves();
    test.add_result(find_field(test.maxscale->conn_rwsplit, "SELECT id FROM test.t1", "id", buf),
                    "Read should succeed");
    res = atoi(buf);
    test.add_result(res != server_id,
                    "Writes should be routed to the master (got %d, master is %d)",
                    res,
                    server_id);

    // Cleanup
    test.check_maxscale_alive();
}

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);

    test.tprintf("Running basic test");
    test_basic(test);

    test.tprintf("Running text PS routing test");
    test_routing(test);

    return test.global_result;
}
