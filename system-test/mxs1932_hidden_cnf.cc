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
 * MXS-1932: Hidden files are not ignored
 *
 * https://jira.mariadb.org/browse/MXS-1932
 */

#include <maxtest/testconnections.hh>

#include <fstream>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
    TestConnections::skip_maxscale_start(true);
    TestConnections test(argc, argv);

    // Create a file with a guaranteed bad configuration (turbochargers are not yet supported)
    ofstream cnf("hidden.cnf");
    cnf << "[something]" << endl;
    cnf << "type=turbocharger" << endl;
    cnf << "target=maxscale" << endl;
    cnf << "speed=maximum" << endl;
    cnf.close();

    // Copy the configuration to MaxScale
    test.maxscale->copy_to_node("hidden.cnf", test.maxscale->access_homedir());

    // Move it into the maxscale.cnf.d directory and make it a hidden file
    test.maxscale->ssh_node_f(true,
                              "mkdir -p /etc/maxscale.cnf.d/;"
                              "mv %s/hidden.cnf /etc/maxscale.cnf.d/.hidden.cnf;"
                              "chown -R maxscale:maxscale /etc/maxscale.cnf.d/",
                              test.maxscale->access_homedir());

    // Make sure the hidden configuration is not read and that MaxScale starts up
    test.expect(test.maxscale->restart_maxscale() == 0, "Starting MaxScale should succeed");

    test.maxscale->ssh_node_f(true, "rm -r /etc/maxscale.cnf.d/");
    remove("hidden.cnf");

    return test.global_result;
}
