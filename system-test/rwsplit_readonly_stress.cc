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
 * @file rwsplit_readonly.cpp Test of the read-only mode for readwritesplit when master fails with load
 * - start query threads which does SELECTs in the loop
 * - every 10 seconds block Master and then after another 10 seconds unblock master
 */

#include <atomic>
#include <iostream>
#include <unistd.h>
#include <cstdlib>
#include <string>
#include <thread>
#include <maxtest/testconnections.hh>

#define THREADS 16
using Clock = std::chrono::steady_clock;

static std::atomic<int> running {0};

int64_t diff_to_ms(Clock::time_point t)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t).count();
}

void query_thread(TestConnections& test)
{
    int counter = 0;

    while (running.load() == 0)
    {
        sleep(1);
    }

    while (running.load() == 1 && test.ok())
    {
        auto conn = counter % 2 == 0 ?
            test.maxscale->readconn_slave() : test.maxscale->readconn_master();
        const char* type = counter % 2 == 0 ?
            "master_failure_mode=error_on_write" : "master_failure_mode=fail_on_write";

        conn.set_timeout(30);
        test.expect(conn.connect(), "Failed to connect to MaxScale: %s", conn.error());

        int i = 0;
        auto loop_start = Clock::now();

        while (Clock::now() - loop_start < std::chrono::seconds(5) && test.ok())
        {
            auto start = Clock::now();

            if (!conn.query("select repeat('a', 1000)"))
            {
                test.add_failure(
                    "Query failed (iteration %d, query %d) for %s, waited for %lums, thread ID %u: %s",
                    i, counter, type, diff_to_ms(start), conn.thread_id(), conn.error());
            }

            ++i;
        }

        ++counter;
    }
}

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    std::vector<std::thread> threads;

    for (int i = 0; i < THREADS; i++)
    {
        threads.emplace_back(query_thread, std::ref(test));
    }

    running = 1;

    for (int i = 0; i < 5 && test.ok(); i++)
    {
        test.tprintf("Blocking master");
        test.repl->block_node(0);
        test.maxscale->wait_for_monitor();

        test.tprintf("Unblocking master");
        test.repl->unblock_node(0);
        test.maxscale->wait_for_monitor();
    }

    test.tprintf("Waiting for all threads to finish\n");
    test.reset_timeout();
    running = 2;

    for (auto& t : threads)
    {
        t.join();
    }

    return test.global_result;
}
