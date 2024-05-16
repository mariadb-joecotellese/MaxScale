/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcar_common.hh"

void test_main(TestConnections& test)
{
    Cleanup cleanup(test);
    std::vector<std::thread> threads;
    std::atomic<uint64_t> connections{0};
    std::atomic<bool> running {true};

    for (int i = 0; i < 100; i++)
    {
        threads.emplace_back([&](){
            while (running)
            {
                auto c = test.maxscale->rwsplit("");
                c.connect();
                c.change_db("test");
                c.query("SET NAMES latin1");
                c.query("SELECT 1");
                ++connections;
            }
        });
    }

    for (int i = 0; i < 10; i++)
    {
        auto before = connections.load();
        test.tprintf("Round %d: %lu connections", i, before);
        test.maxscale->maxctrl("call command wcar start WCAR");

        while (connections.load() <= before + 2)
        {
            std::this_thread::sleep_for(1ms);
        }

        before = connections.load();
        test.maxscale->maxctrl("call command wcar stop WCAR");

        while (connections.load() <= before + 2)
        {
            std::this_thread::sleep_for(1ms);
        }
    }

    running = false;

    for (auto& t : threads)
    {
        t.join();
    }

    test.maxscale->stop();

    test.tprintf("Replaying all captures");
    do_replay(test, "WCAR");
}

ENTERPRISE_TEST_MAIN(test_main)
