/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcar_common.hh"

std::atomic<int> deadlocks{0};

void update_values(TestConnections& test, int val)
{
    auto c = test.maxscale->rwsplit();

    if (test.expect(c.connect(), "Failed to connect: %s", c.error()))
    {
        while (test.ok() && deadlocks.load() < 200)
        {
            bool ok = true;
            c.query("BEGIN");

            for (int j = 0; j < 6 && ok; j++)
            {
                int id = ((j + val) % 6) + 1;
                ok = c.query("UPDATE test.t1 SET val = val + 1 WHERE id = " + std::to_string(id));
            }

            if (ok)
            {
                c.query("COMMIT");
            }
            else
            {
                test.expect(c.errnum() == 1213, "Expected deadlock error but got: %d, %s",
                            c.errnum(), c.error());
                ++deadlocks;
            }
        }
    }
}

void test_main(TestConnections& test)
{
    Cleanup cleanup(test);

    test.repl->connect();
    test.repl->execute_query_all_nodes("SET GLOBAL innodb_lock_wait_timeout=10");
    test.repl->disconnect();

    std::vector<std::thread> threads;
    auto c = test.maxscale->rwsplit();
    MXT_EXPECT(c.connect());

    c.query("CREATE TABLE test.t1(id INT PRIMARY KEY, val INT DEFAULT 0)");
    c.query("INSERT INTO test.t1(id) VALUES (0), (1), (2), (3), (4), (5)");
    cleanup.add_table("test.t1");

    test.tprintf("Waiting for 200 deadlocks to occur during the capture.");

    for (int i = 0; i < 100; i++)
    {
        threads.emplace_back(update_values, std::ref(test), i % 6);
    }

    for (auto& t : threads)
    {
        t.join();
    }

    test.maxscale->stop();

    test.set_verbose(true);
    int rc = test.maxscale->ssh_node_f(true, ASAN_OPTS "maxplayer summary /var/lib/maxscale/wcar/WCAR/*.cx");
    test.set_verbose(false);
    MXT_EXPECT_F(rc == 0, "'maxplayer summary' failed.");

    cleanup.add_files("/tmp/replay-WCAR.csv");
    test.tprintf("Replaying capture");
    do_replay(test, "WCAR");

    test.tprintf("Replaying capture with --speed=0");
    do_replay(test, "WCAR", "--speed=0");

    test.tprintf("Replaying capture with --speed=0 --commit-order=none");
    do_replay(test, "WCAR", "--speed=0 --commit-order=none");

    test.tprintf("Replaying capture with --speed=0 --commit-order=serialized");
    do_replay(test, "WCAR", "--speed=0 --commit-order=serialized");

    test.repl->connect();
    test.repl->execute_query_all_nodes("SET GLOBAL innodb_lock_wait_timeout=DEFAULT");
    test.repl->disconnect();
}

ENTERPRISE_TEST_MAIN(test_main)
