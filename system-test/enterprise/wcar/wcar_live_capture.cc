/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcar_common.hh"

std::atomic<bool> running {true};
std::atomic<uint64_t> transactions {0};
std::atomic<uint64_t> connections {0};

std::thread make_long_connection(TestConnections& test)
{
    return std::thread([&](){
        auto c = test.maxscale->rwsplit();

        if (test.expect(c.connect(), "Failed to connect: %s", c.error()))
        {
            c.query("INSERT INTO test.t1 VALUES (1, 0)");

            while (running)
            {
                c.query("BEGIN");
                c.query("SELECT val FROM test.t1");
                c.query("UPDATE test.t1 SET val = val + 1 WHERE id = 1");
                c.query("COMMIT");
                transactions++;
            }
        }
    });
}

std::thread make_short_connection(TestConnections& test)
{
    return std::thread([&](){
        while (running)
        {
            auto c = test.maxscale->rwsplit();

            if (test.expect(c.connect(), "Failed to connect: %s", c.error()))
            {
                c.query("INSERT INTO test.t1 VALUES (2, 0) ON DUPLICATE KEY UPDATE val = 0");

                for (int i = 0; i < 10; i++)
                {
                    c.query("BEGIN");
                    c.query("SELECT val FROM test.t1");
                    c.query("UPDATE test.t1 SET val = val + 1 WHERE id = 2");
                    c.query("COMMIT");
                }

                connections++;
            }
        }
    });
}

void live_capture(TestConnections& test)
{
    Cleanup cleanup(test);
    cleanup.add_table("test.t1");
    auto report = [&](){
        test.tprintf("Transactions: %lu Connections: %lu\n%s", transactions.load(), connections.load(),
                     get_capture_status(test).to_string().c_str());
    };

    auto trx_open_always = test.maxscale->rwsplit();
    auto trx_open_on_start = test.maxscale->rwsplit();
    auto trx_open_on_end = test.maxscale->rwsplit();
    MXT_EXPECT(trx_open_always.connect() && trx_open_on_start.connect() && trx_open_on_end.connect());

    MXT_EXPECT(trx_open_always.query("CREATE TABLE test.t1 (id INT PRIMARY KEY, val INT)"));
    MXT_EXPECT(trx_open_always.query("INSERT INTO test.t1 VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)"));
    MXT_EXPECT(trx_open_always.query("BEGIN"));
    MXT_EXPECT(trx_open_always.query("UPDATE test.t1 SET val = val + 1 WHERE id = 3"));
    MXT_EXPECT(trx_open_on_start.query("BEGIN"));
    MXT_EXPECT(trx_open_on_start.query("UPDATE test.t1 SET val = val + 1 WHERE id = 4"));

    std::vector<std::thread> threads;
    for (int i = 0; i < 150; i++)
    {
        if (i % 2 == 0)
        {
            threads.push_back(make_long_connection(test));
        }
        else
        {
            threads.push_back(make_short_connection(test));
        }
    }

    test.tprintf("Waiting for sessions to start");

    while ((transactions < 10 || connections < 5) && test.ok())
    {
        report();
        std::this_thread::sleep_for(2s);
    }

    report();

    test.tprintf("Starting capture");
    auto trx_start = transactions.load();
    auto conn_start = connections.load();
    test.maxscale->maxctrl("call command wcar start WCAR");
    test.maxscale->maxctrl("call command wcar start WCAR-Time-Limit");
    test.maxscale->maxctrl("call command wcar start WCAR-Size-Limit");

    mxb::Json status;
    bool first_loop = true;

    do
    {
        status = get_capture_status(test);
        report();
        std::this_thread::sleep_for(2s);

        if (first_loop)
        {
            MXT_EXPECT(trx_open_on_start.query("COMMIT"));

            MXT_EXPECT(trx_open_on_end.query("BEGIN"));
            MXT_EXPECT(trx_open_on_end.query("UPDATE test.t1 SET val = val + 1 WHERE id = 5"));

            first_loop = false;
        }
    }
    while (test.ok()
           && (status.get_real("duration") < 10.0 || status.get_int("size") < 1024 * 1024));

    test.tprintf("Stopping capture");
    test.maxscale->maxctrl("call command wcar stop WCAR");
    test.maxscale->maxctrl("call command wcar stop WCAR-Time-Limit");
    test.maxscale->maxctrl("call command wcar stop WCAR-Size-Limit");
    auto trx_end = transactions.load();
    auto conn_end = connections.load();

    first_loop = true;

    do
    {
        report();
        std::this_thread::sleep_for(2s);

        if (first_loop)
        {
            MXT_EXPECT(trx_open_on_end.query("COMMIT"));
            first_loop = false;
        }
    }
    while (test.ok()
           && (transactions.load() - trx_end < 100 || connections.load() - conn_end < 10));

    report();
    running = false;

    for (auto& t : threads)
    {
        t.join();
    }

    threads.clear();
    report();

    test.maxscale->stop();

    // Create a copy of the normal capture for processing it with --chunk-size=1Ki.
    copy_capture(test, "WCAR", "WCAR-Chunked");

    cleanup.add_files("/tmp/replay-WCAR.csv",
                      "/tmp/replay-WCAR-Chunked.csv",
                      "/tmp/replay-WCAR-Size-Limit.csv",
                      "/tmp/replay-WCAR-Time-Limit.csv");

    threads.emplace_back([&](){
        test.tprintf("Replaying the unlimited capture");
        do_replay(test, "WCAR");
    });
    threads.emplace_back([&](){
        test.tprintf("Replaying the size limited capture");
        do_replay(test, "WCAR-Size-Limit");
    });
    threads.emplace_back([&](){
        test.tprintf("Replaying the time limited capture");
        do_replay(test, "WCAR-Time-Limit");
    });
    threads.emplace_back([&](){
        test.tprintf("Replaying the unlimited capture with --chunk-size=1Ki");
        do_replay(test, "WCAR-Chunked", "--chunk-size=1Ki");
    });

    for (auto& thr : threads)
    {
        thr.join();
    }
    threads.clear();
}

void test_main(TestConnections& test)
{
    live_capture(test);
}

ENTERPRISE_TEST_MAIN(test_main)
