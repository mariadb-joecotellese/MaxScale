/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * @file session_limits.cpp - test for 'max_sescmd_history' and 'connection_timeout' parameters
 * - add follwoling to router configuration
 * @verbatim
 *  connection_timeout=30
 *  router_options=max_sescmd_history=10
 *  @endverbatim
 * - open session
 * - wait 20 seconds, check if session is alive, expect ok
 * - wait 20 seconds more, check if session is alive, expect failure
 * - open new session
 * - execute 10 session commands
 * - check if session is alive, expect ok
 * - execute one more session commad, excpect failure
 */

#include <maxtest/testconnections.hh>

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    int first_sleep = 5;
    int second_sleep = 12;

    test.reset_timeout();

    test.tprintf("Open session, wait %d seconds and execute a query", first_sleep);
    test.maxscale->connect_maxscale();
    sleep(first_sleep);
    test.try_query(test.maxscale->conn_rwsplit, "SELECT 1");

    test.tprintf("Wait %d seconds and execute query, expecting failure", second_sleep);
    sleep(second_sleep);
    test.add_result(execute_query(test.maxscale->conn_rwsplit, "SELECT 1") == 0,
                    "Session was not closed after %d seconds",
                    second_sleep);
    test.maxscale->close_maxscale_connections();

    test.tprintf("Open session and execute 10 session commands");
    test.maxscale->connect_maxscale();
    for (int i = 0; i < 10; i++)
    {
        test.try_query(test.maxscale->conn_rwsplit,
                       "%s",
                       std::string("set @test=" + std::to_string(i)).c_str());
    }

    test.tprintf("Execute one more session command");
    execute_query(test.maxscale->conn_rwsplit, "set @test=11");
    test.maxscale->close_maxscale_connections();

    return test.global_result;
}
