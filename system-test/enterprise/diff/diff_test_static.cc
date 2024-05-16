/*
 * Copyright (c) 2024 MariaDB plc
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "diff.hh"
#include "../enterprise_test.hh"

namespace
{

void test_main(TestConnections& test)
{
    // The system test framework will setup 'server3' to replicate from 'server1'.
    // That replication must now be stopped, as otherwise Diff refuses to start.

    auto c = test.repl->get_connection(2); // I.e. "server3"

    test.expect(c.connect(), "Could not connect to 'server3'");
    test.expect(c.query("STOP SLAVE"), "Could not stop replication.");

    // Now MaxScale can be started.
    test.maxscale->start();

    sleep(2);

    Diff diff("DiffMyService", &test);

    diff.status();
}

}

int main(int argc, char** argv)
{
    TestConnections::skip_maxscale_start(true);

    return TestConnections().run_test(argc, argv, test_main);
}
