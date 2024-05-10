/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "../enterprise_test.hh"
#include <maxbase/string.hh>
#include <filesystem>
#include <fstream>
#include <iostream>

#define ASAN_OPTS "ASAN_OPTIONS=abort_on_error=1 UBSAN_OPTIONS=abort_on_error=1 "

namespace fs = std::filesystem;

class Cleanup
{
public:
    Cleanup(TestConnections& test)
        : m_test(test)
    {
        cleanup();
    }

    ~Cleanup()
    {
        if (m_test.verbose())
        {
            m_test.tprintf("Verbose test, skipping cleanup.");
        }
        else
        {
            cleanup();
        }
    }

    void add_table(const std::string& table)
    {
        m_created_tables.insert(table);
    }

private:
    void cleanup()
    {
        m_test.maxscale->stop();
        m_test.maxscale->ssh_node("rm -f /var/lib/maxscale/wcar/WCAR/* /tmp/replay.csv", true);

        if (!m_created_tables.empty())
        {
            if (auto c = m_test.repl->get_connection(0); c.connect())
            {
                for (auto tbl : m_created_tables)
                {
                    c.query("DROP TABLE " + tbl);
                }
            }
        }

        m_test.maxscale->start();
    }

    TestConnections&      m_test;
    std::set<std::string> m_created_tables;
};

void sanity_check(TestConnections& test)
{
    Cleanup cleanup(test);
    cleanup.add_table("test.wcar_basic");

    auto c = test.maxscale->rwsplit();
    MXT_EXPECT(c.connect());

    // 1. Do a capture
    // 2. Replay the capture
    // 3. Check that the contents of the database are identical

    for (const char* query : {
        "CREATE TABLE test.wcar_basic(id INT PRIMARY KEY, data INT)",

        "START TRANSACTION",
        "INSERT INTO wcar_basic VALUES (1, 1)",
        "COMMIT",

        "START TRANSACTION",
        "INSERT INTO wcar_basic VALUES (2, 2)",
        "ROLLBACK",

        "START TRANSACTION",
        "UPDATE wcar_basic SET data = 2 WHERE id = 1",
        "COMMIT",

        "SELECT 'hello world'",
    })
    {
        MXT_EXPECT_F(c.query(query), "Query %s failed: %s", query, c.error());
    }

    test.maxscale->stop();

    //
    // Check that the expected files exist and that the contents of them are what we expect them to be.
    //
    auto res = test.maxscale->ssh_output("find /var/lib/maxscale/wcar/WCAR/ -type f");
    std::set<std::string> files;
    std::set<std::string> expected{".cx", ".ex", ".gx"};
    std::string replay_file;
    std::string event_file;

    for (fs::path file : mxb::strtok(res.output, "\n"))
    {
        if (file.extension() == ".cx")
        {
            replay_file = file;
        }
        else if (file.extension() == ".ex")
        {
            event_file = file;
        }

        files.insert(file.extension());
    }

    MXT_EXPECT_F(files == expected, "Expected files with extensions %s but got %s",
                 mxb::join(files).c_str(), mxb::join(expected).c_str());

    res = test.maxscale->ssh_output("strings " + event_file);
    test.expect(res.output.find("hello world") != std::string::npos,
                "File does not contain 'hello world'");

    //
    // Replay the capture and verify that the database is in the same state after it.
    //
    auto m = test.repl->get_connection(0);
    MXT_EXPECT(m.connect());

    auto before = m.field("CHECKSUM TABLE test.wcar_basic EXTENDED", 1);
    MXT_EXPECT(m.query("DROP TABLE test.wcar_basic"));

    test.tprintf("Attempting replay without the correct file permissions");
    int rc = test.maxscale->ssh_node_f(false, ASAN_OPTS
                                       "maxplayer replay -u %s -p %s -H %s:%d --csv -o /tmp/replay.csv %s",
                                       test.repl->user_name().c_str(),
                                       test.repl->password().c_str(),
                                       test.repl->ip(0),
                                       test.repl->port(0),
                                       replay_file.c_str());

    MXT_EXPECT_F(rc != 0, "'maxplayer replay' should fail if there's no write access to the file");

    test.tprintf("Attempting a summary without the correct file permissions");
    res = test.maxscale->ssh_output("maxplayer summary " + replay_file + " 2>&1", false);
    MXT_EXPECT_F(res.rc != 0, "'maxplayer summary' should fail if there's no write access to the file");
    MXT_EXPECT_F(res.output.find("Could not open file") != std::string::npos,
                 "The failure to open should be reported");
    std::cout << res.output << std::endl;

    test.tprintf("Attempting a summary with the correct file permissions");
    res = test.maxscale->ssh_output("maxplayer summary " + replay_file + " 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer summary' should work as root");
    std::cout << res.output << std::endl;


    test.tprintf("Attempting a replay with the correct file permissions");
    rc = test.maxscale->ssh_node_f(true, ASAN_OPTS
                                   "maxplayer replay -u %s -p %s -H %s:%d --csv -o /tmp/replay.csv %s",
                                   test.repl->user_name().c_str(),
                                   test.repl->password().c_str(),
                                   test.repl->ip(0),
                                   test.repl->port(0),
                                   replay_file.c_str());

    MXT_EXPECT_F(rc == 0, "'maxplayer replay' should work after running 'maxplayer summary' as root");

    test.maxscale->copy_from_node("/tmp/replay.csv", "./replay.csv");

    for (auto line : mxb::strtok(MAKE_STR(std::ifstream("./replay.csv").rdbuf()), "\n"))
    {
        test.tprintf("%s", line.c_str());
    }

    auto after = m.field("CHECKSUM TABLE test.wcar_basic EXTENDED", 1);
    MXT_EXPECT_F(before == after, "CHECKSUM TABLE mismatch: %s != %s", before.c_str(), after.c_str());
}

void test_main(TestConnections& test)
{
    sanity_check(test);
}

ENTERPRISE_TEST_MAIN(test_main)
