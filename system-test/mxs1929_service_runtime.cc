/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * MXS-1929: Runtime service creation
 */
#include <maxtest/testconnections.hh>

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace std;

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    auto& repl = *test.repl;

    auto maxctrl = [&](string cmd, bool print = true) {
            test.reset_timeout();
            auto rv = test.maxscale->ssh_output("maxctrl " + cmd);

            if (rv.rc != 0 && print)
            {
                cout << "MaxCtrl: " << rv.output << endl;
            }

            return rv.rc == 0;
        };

    Connection c1 = test.maxscale->rwsplit();
    string host1 = repl.ip4(0);
    string port1 = to_string(repl.port(0));
    string host2 = repl.ip4(1);
    string port2 = to_string(repl.port(1));
    string host3 = repl.ip4(2);
    string port3 = to_string(repl.port(2));

    cout << "Create a service and check that it works" << endl;

    maxctrl("create service svc1 readwritesplit user=skysql password=skysql");

    maxctrl("create listener svc1 listener1 4006");
    maxctrl("create monitor mon1 mariadbmon user=skysql password=skysql");
    maxctrl("create server server1 " + host1 + " " + port1 + " --services svc1 --monitors mon1");
    maxctrl("create server server2 " + host2 + " " + port2 + " --services svc1 --monitors mon1");
    maxctrl("create server server3 " + host3 + " " + port3 + " --services svc1 --monitors mon1");

    c1.connect();
    test.expect(c1.query("SELECT 1"), "Query to simple service should work: %s", c1.error());
    c1.disconnect();

    cout << "Destroy the service and check that it is removed" << endl;

    test.expect(!maxctrl("destroy service svc1", false), "Destroying linked service should fail");
    maxctrl("unlink service svc1 server1 server2 server3");
    test.expect(!maxctrl("destroy service svc1", false),
                "Destroying service with active listeners should fail");
    maxctrl("destroy listener svc1 listener1");
    test.expect(maxctrl("destroy service svc1"), "Destroying valid service should work");

    test.reset_timeout();
    test.expect(!c1.connect(), "Connection should be rejected");

    cout << "Create the same service again and check that it still works" << endl;

    maxctrl("create service svc1 readwritesplit user=skysql password=skysql");
    maxctrl("create listener svc1 listener1 4006");
    maxctrl("link service svc1 server1 server2 server3");

    c1.connect();
    test.expect(c1.query("SELECT 1"), "Query to recreated service should work: %s", c1.error());
    c1.disconnect();

    cout << "Check that active connections aren't closed when service is destroyed" << endl;

    c1.connect();
    maxctrl("unlink service svc1 server1 server2 server3");
    maxctrl("destroy listener svc1 listener1");
    maxctrl("destroy service svc1");

    test.expect(c1.query("SELECT 1"), "Query to destroyed service should still work");

    // Start a thread to attempt a connection before the last connection
    // is closed. The connection attempt should be rejected when the
    // listener is freed.
    mutex m;
    condition_variable cv;
    thread t([&]() {
                 cv.notify_one();
                 test.expect(!test.maxscale->rwsplit().connect(),
                             "New connections to created service "
                             "should fail with a timeout while the original connection is open");
             });

    // Wait until the thread starts
    unique_lock<mutex> ul(m);
    cv.wait(ul);
    ul.unlock();

    // This is unreliable but it's adequate for testing to ensure a connection
    // is opened before the old one is closed
    sleep(1);

    test.reset_timeout();

    // Disconnect the original connection and try to reconnect
    c1.disconnect();
    test.expect(!c1.connect(), "New connections should be rejected after original connection is closed");

    // The connection should be rejected once the last connection is closed. If
    // it doesn't, we hit the test timeout before the connection timeout.
    t.join();

    return test.global_result;
}
