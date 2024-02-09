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
 * MXS-1506: Delayed query retry without master
 *
 * https://jira.mariadb.org/browse/MXS-1506
 */
#include <maxtest/testconnections.hh>
#include <string>
#include <functional>
#include <thread>
#include <iostream>
#include <vector>

using namespace std;

bool query(TestConnections& test)
{
    test.maxscale->connect_rwsplit();
    execute_query_silent(test.maxscale->conn_rwsplit, "SET @a = 1");
    sleep(5);
    Row row = get_row(test.maxscale->conn_rwsplit, "SELECT @a");
    test.maxscale->disconnect();
    return !row.empty() && row[0] == "1";
}

void block(TestConnections& test, std::vector<int> nodes)
{
    for (auto a : nodes)
    {
        test.repl->block_node(a);
    }

    sleep(10);

    for (auto a : nodes)
    {
        test.repl->unblock_node(a);
    }
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    thread thr;

    cout << "Blocking the master and executing a SELECT" << endl;
    thr = thread(block, std::ref(test), vector<int>({0}));
    test.expect(query(test), "Select without master should work");
    thr.join();

    cout << "Blocking the slave and executing a SELECT" << endl;
    thr = thread(block, std::ref(test), vector<int>({1}));
    test.expect(query(test), "Select without slave should work");
    thr.join();

    cout << "Blocking both servers and executing a SELECT" << endl;
    thr = thread(block, std::ref(test), vector<int>({0, 1}));
    test.expect(query(test), "Select with no servers should work");
    thr.join();

    return test.global_result;
}
