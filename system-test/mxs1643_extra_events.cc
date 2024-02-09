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
 * MXS-1643: Too many monitor events are triggered
 *
 * https://jira.mariadb.org/browse/MXS-1643
 */
#include <maxtest/testconnections.hh>

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    // Check that master gets the slave status when set into read-only mode
    test.tprintf("Set master into read-only mode");
    test.repl->connect();
    execute_query(test.repl->nodes[0], "SET GLOBAL read_only=ON");
    test.maxscale->wait_for_monitor();
    test.tprintf("Check that the current master now has the slave label");
    test.log_excludes("server1.*\\[Master, Running\\] -> \\[Running\\]");
    test.log_includes("server1.*\\[Master, Running\\] -> \\[Slave, Running\\]");
    test.maxscale->ssh_node_f(true, "truncate -s 0 /var/log/maxscale/maxscale.log");

    // Check that the Master and Slave status aren't both set
    execute_query(test.repl->nodes[0], "SET GLOBAL read_only=OFF");
    test.maxscale->wait_for_monitor();
    test.tprintf("Check that the new master doesn't have both slave and master labels");
    test.log_excludes("server1.*\\[Slave, Running\\] -> \\[Master, Slave, Running\\]");
    test.log_excludes("server1.*\\[Running\\] -> \\[Master, Running\\]");
    test.log_includes("server1.*\\[Slave, Running\\] -> \\[Master, Running\\]");

    return test.global_result;
}
