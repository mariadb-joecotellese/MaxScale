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
 * @file check_backend.cpp simply checks if backend is alive
 */


#include <iostream>
#include <maxtest/testconnections.hh>

int main(int argc, char *argv[])
{

    TestConnections * Test = new TestConnections(argc, argv);

    // Reset server settings by replacing the config files
    Test->repl->reset_all_servers_settings();

    Test->tprintf("Connecting to Maxscale maxscales->routers[0] with Master/Slave backend\n");
    Test->maxscale->connect_maxscale();
    Test->tprintf("Testing connections\n");

    Test->add_result(Test->test_maxscale_connections(true, true, true), "Can't connect to backend\n");

    Test->tprintf("Connecting to Maxscale router with Galera backend\n");
    MYSQL * g_conn = open_conn(4016, Test->maxscale->ip4(), Test->maxscale->user_name(),
                               Test->maxscale->password(), Test->maxscale_ssl);
    if (g_conn != NULL )
    {
        Test->tprintf("Testing connection\n");
        Test->add_result(Test->try_query(g_conn, (char *) "SELECT 1"),
                         (char *) "Error executing query against RWSplit Galera\n");
    }

    Test->tprintf("Closing connections\n");
    Test->maxscale->close_maxscale_connections();
    Test->check_maxscale_alive();

    auto ver = Test->maxscale->ssh_output("maxscale --version-full", false);
    Test->tprintf("Maxscale_full_version_start:\n%s\nMaxscale_full_version_end\n", ver.output.c_str());

    int rval = Test->global_result;
    delete Test;
    return rval;
}
