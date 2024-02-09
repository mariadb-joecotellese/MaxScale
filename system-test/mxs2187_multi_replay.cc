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
 * MXS-1507: Test inconsistent result detection
 *
 * https://jira.mariadb.org/browse/MXS-1507
 */
#include <maxtest/testconnections.hh>
#include <functional>
#include <iostream>
#include <vector>

using namespace std;

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    auto query = [&](string q) {
            return execute_query_silent(test.maxscale->conn_rwsplit, q.c_str()) == 0;
        };

    auto ok = [&](string q) {
            test.expect(query(q),
                        "Query '%s' should work: %s",
                        q.c_str(),
                        mysql_error(test.maxscale->conn_rwsplit));
        };

    auto kill_master = [&]() {
            test.repl->connect();
            test.maxscale->wait_for_monitor(1);
            auto master = test.get_repl_master();
            if (master)
            {
                test.repl->disconnect();
                test.repl->block_node(master->ind());
                test.maxscale->wait_for_monitor(3);
                test.repl->unblock_node(master->ind());
                test.maxscale->wait_for_monitor(3);
            }
            else
            {
                test.add_failure("No master to kill.");
            }
        };

    // Create a table
    test.maxscale->connect_rwsplit();
    ok("CREATE OR REPLACE TABLE test.t1 (id INT)");
    test.maxscale->disconnect();

    // Make sure it's replicated to all slaves before starting the transaction
    test.repl->connect();
    test.repl->sync_slaves();
    test.repl->disconnect();

    // Try to do a transaction across multiple master failures
    test.maxscale->connect_rwsplit();

    cout << "Start transaction, insert a value and read it" << endl;
    ok("START TRANSACTION");
    ok("INSERT INTO test.t1 VALUES (1)");
    ok("SELECT * FROM test.t1 WHERE id = 1");

    cout << "Killing master" << endl;
    kill_master();

    cout << "Insert value and read it" << endl;
    ok("INSERT INTO test.t1 VALUES (2)");
    ok("SELECT * FROM test.t1 WHERE id = 2");

    cout << "Killing second master" << endl;
    kill_master();

    cout << "Inserting value 3" << endl;
    ok("INSERT INTO test.t1 VALUES (3)");
    ok("SELECT * FROM test.t1 WHERE id = 3");

    cout << "Killing third master" << endl;
    kill_master();

    cout << "Selecting final result" << endl;
    ok("SELECT SUM(id) FROM test.t1");

    cout << "Killing fourth master" << endl;
    kill_master();

    cout << "Committing transaction" << endl;
    ok("COMMIT");
    test.maxscale->disconnect();

    test.maxscale->connect_rwsplit();
    cout << "Checking results" << endl;
    Row r = get_row(test.maxscale->conn_rwsplit, "SELECT SUM(id), @@last_insert_id FROM t1");
    test.expect(!r.empty() && r[0] == "6", "All rows were not inserted: %s",
                r.empty() ? "No rows" : r[0].c_str());
    test.maxscale->disconnect();

    test.maxscale->connect_rwsplit();
    ok("DROP TABLE test.t1");
    test.maxscale->disconnect();

    return test.global_result;
}
