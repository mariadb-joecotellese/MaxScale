/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
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

/**
 * MXS-2414: Block host after repeated authentication failures
 * https://jira.mariadb.org/browse/MXS-2414
 */

#include <maxtest/testconnections.hh>

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    bool found = false;

    for (int i = 0; i < 1000; i++)
    {
        test.reset_timeout();
        auto c = test.maxscale->rwsplit();
        c.set_credentials("wrong-user", "wrong-pw");
        test.expect(!c.connect(), "Connection should fail");

        if (strstr(c.error(), "temporarily blocked due to too many authentication failures"))
        {
            test.tprintf("Got correct error: %s", c.error());
            found = true;

            // Make sure some valid logins are blocked.  Note that this part is not fully deterministic which
            // means we cannot interpret a lack of authentication failures as a sign of a problem. The only
            // thing we can check is that, in case an authentication failure occurs, the correct error is
            // returned.
            for (int j = 0; j < 100; j++)
            {
                auto c2 = test.maxscale->rwsplit();

                if (!c2.connect())
                {
                    test.expect(strstr(c2.error(), "temporarily blocked due to too many authentication failures"),
                                "The same error should be returned: %s", c2.error());
                    break;
                }
            }
            break;
        }
    }

    test.expect(found, "Host should be blocked");

    return test.global_result;
}
