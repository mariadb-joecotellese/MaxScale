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

    template<class ... Args>
    void add_files(Args ... args)
    {
        (m_files.push_back(args), ...);
    }

private:
    void cleanup()
    {
        m_test.maxscale->stop();
        m_test.maxscale->ssh_node("rm -f /var/lib/maxscale/wcar/WCAR/* /tmp/replay.csv "
                                  + mxb::join(m_files, " "), true);

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

    TestConnections&         m_test;
    std::set<std::string>    m_created_tables;
    std::vector<std::string> m_files;
};

void sanity_check(TestConnections& test)
{
    test.tprintf("%s", __func__);
    Cleanup cleanup(test);
    cleanup.add_table("test.wcar_basic");
    cleanup.add_files("/tmp/replay.rx", "/tmp/replay.csv", "/tmp/converted.cx",
                      "/tmp/converted.rx", "/tmp/converted.csv", "/tmp/converted2.csv",
                      "/tmp/output-full.csv", "/tmp/output.csv", "/tmp/dump.txt", "/tmp/canonicals.csv");

    auto c = test.maxscale->rwsplit();
    MXT_EXPECT(c.connect());

    // 1. Do a capture
    // 2. Replay the capture
    // 3. Check that the contents of the database are identical

    for (std::string query : {
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

        "SELECT THIS SHOULD BE A SYNTAX ERROR",
        "SELECT 'hello world'",
    })
    {
        MXT_EXPECT_F(c.query(query) || query.find("SYNTAX ERROR") != std::string::npos,
                     "Query %s failed: %s", query.c_str(), c.error());
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
    std::string opt_outfile = "-o /tmp/output.csv --csv";
    std::vector<std::string> options = {
        "-o /tmp/replay.rx",                        // Generates .rx files
        "-o /tmp/output-full.csv --csv=full",       // Full CSV output
        "-o /dev/null --csv",                       // Discards output
        opt_outfile + " --speed 0",                 // Replay as fast as possible
        opt_outfile + " -R",                        // No Rows_read counts
        opt_outfile + " -v",                        // Verbose output
        opt_outfile + " -vv",                       // Very verbose output
        opt_outfile + " --commit-order=none",       // No commit ordering
        opt_outfile + " --commit-order=optimistic", // No optimistic ordering
        opt_outfile + " --commit-order=serialized", // No serialized ordering
    };

    for (auto opt : options)
    {
        test.tprintf("Replay options: %s", opt.c_str());

        test.maxscale->ssh_node("rm -f /tmp/output.csv", true);
        MXT_EXPECT(m.query("DROP TABLE test.wcar_basic"));
        rc = test.maxscale->ssh_node_f(true, ASAN_OPTS
                                       "maxplayer replay -u %s -p %s -H %s:%d %s %s",
                                       test.repl->user_name().c_str(), test.repl->password().c_str(),
                                       test.repl->ip(0), test.repl->port(0),
                                       replay_file.c_str(), opt.c_str());

        MXT_EXPECT_F(rc == 0, "'maxplayer replay' with '%s' failed.", opt.c_str());
    }

    test.tprintf("The 'show' output of event 1 should be 'CREATE TABLE'");
    res = test.maxscale->ssh_output("maxplayer show " + replay_file + " 1 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer show' should work: %s", res.output.c_str());
    MXT_EXPECT_F(res.output.find("CREATE TABLE") != std::string::npos,
                 "Output should contain 'CREATE TABLE': %s", res.output.c_str());

    test.tprintf("The 'dump-data' output should contain 'hello world'");
    res = test.maxscale->ssh_output("maxplayer dump-data " + replay_file + " 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer dump-data' should work: %s", res.output.c_str());
    MXT_EXPECT_F(res.output.find("hello world") != std::string::npos,
                 "Output should contain 'hello world': %s", res.output.c_str());

    test.tprintf("The -o option writes the output of 'dump-data' to a file.");
    res = test.maxscale->ssh_output("maxplayer dump-data " + replay_file + " -o /tmp/dump.txt 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer dump-data' should work: %s", res.output.c_str());
    test.maxscale->copy_from_node("/tmp/dump.txt", "./dump.txt");
    auto dump_contents = MAKE_STR(std::ifstream("./dump.txt").rdbuf());
    MXT_EXPECT(dump_contents.find("hello world") != std::string::npos);

    std::string canonical = "UPDATE wcar_basic SET data = ? WHERE id = ?";
    test.tprintf("The 'canonicals' output should contain '%s'", canonical.c_str());
    res = test.maxscale->ssh_output("maxplayer canonicals " + replay_file + " 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer canonicals' should work: %s", res.output.c_str());
    MXT_EXPECT_F(res.output.find(canonical) != std::string::npos,
                 "Output should contain '%s': %s", canonical.c_str(), res.output.c_str());

    test.tprintf("The -o option writes the output of 'canonicals' to a file.");
    res = test.maxscale->ssh_output("maxplayer canonicals " + replay_file + " -o /tmp/canonicals.csv 2>&1",
                                    true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer canonicals' should work: %s", res.output.c_str());
    test.maxscale->copy_from_node("/tmp/canonicals.csv", "./canonicals.csv");
    auto canonicals_csv_contents = MAKE_STR(std::ifstream("./canonicals.csv").rdbuf());
    MXT_EXPECT(canonicals_csv_contents.find(canonical) != std::string::npos);

    test.tprintf("Convert .cx into .rx");
    res = test.maxscale->ssh_output("maxplayer convert " + replay_file + " -o /tmp/converted.rx 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "Convert from .cx to .rx should work: %s", res.output.c_str());
    rc = test.maxscale->ssh_node("test -f /tmp/converted.rx", true);
    MXT_EXPECT_F(rc == 0, ".rx file doesn't exist");

    test.tprintf("Convert .cx into .csv");
    res = test.maxscale->ssh_output("maxplayer convert " + replay_file + " --csv -o /tmp/converted.csv 2>&1",
                                    true);
    MXT_EXPECT_F(res.rc == 0, "Convert from .cx to .csv should work: %s", res.output.c_str());
    rc = test.maxscale->ssh_node("test -f /tmp/converted.csv", true);
    MXT_EXPECT_F(rc == 0, ".csv file doesn't exist");

    test.tprintf("Convert .rx into .csv");
    res = test.maxscale->ssh_output("maxplayer convert /tmp/replay.rx --csv -o /tmp/converted2.csv 2>&1",
                                    true);
    MXT_EXPECT_F(res.rc == 0, "Convert from .rx to .csv should work: %s", res.output.c_str());
    rc = test.maxscale->ssh_node("test -f /tmp/converted2.csv", true);
    MXT_EXPECT_F(rc == 0, ".csv file doesn't exist");
}

void do_replay_and_checksum(TestConnections& test)
{
    test.maxscale->stop();

    int rc = test.maxscale->ssh_node_f(true, ASAN_OPTS
                                       "maxplayer replay -u %s -p %s -H %s:%d --csv -o /tmp/replay.csv "
                                       "/var/lib/maxscale/wcar/WCAR/*.cx",
                                       test.repl->user_name().c_str(), test.repl->password().c_str(),
                                       test.repl->ip(0), test.repl->port(0));

    MXT_EXPECT_F(rc == 0, "Replay should work.");
    auto res = test.maxscale->ssh_output("wc -l /tmp/replay.csv");
    MXT_EXPECT_F(std::stoi(res.output) > 1,
                 "Replay should generate a CSV file with at least one line: %s", res.output.c_str());
}

void simple_binary_ps(TestConnections& test)
{
    test.tprintf("%s", __func__);
    Cleanup cleanup(test);
    auto c = test.maxscale->rwsplit();
    MXT_EXPECT(c.connect());
    MYSQL_STMT* stmt = c.stmt();

    std::string query = "SELECT ?, ?, ?, ?";

    mysql_stmt_prepare(stmt, query.c_str(), query.size());

    int value = 1;
    MYSQL_BIND param[4];

    param[0].buffer_type = MYSQL_TYPE_LONG;
    param[0].is_null = 0;
    param[0].buffer = &value;
    param[1].buffer_type = MYSQL_TYPE_LONG;
    param[1].is_null = 0;
    param[1].buffer = &value;
    param[2].buffer_type = MYSQL_TYPE_LONG;
    param[2].is_null = 0;
    param[2].buffer = &value;
    param[3].buffer_type = MYSQL_TYPE_LONG;
    param[3].is_null = 0;
    param[3].buffer = &value;

    mysql_stmt_bind_param(stmt, param);
    mysql_stmt_execute(stmt);
    mysql_stmt_close(stmt);

    do_replay_and_checksum(test);
}

void simple_text_ps(TestConnections& test)
{
    test.tprintf("%s", __func__);
    Cleanup cleanup(test);

    auto c = test.maxscale->rwsplit();
    MXT_EXPECT(c.connect());
    c.query("PREPARE stmt FROM 'SELECT ?, ?, ?, ?'");
    c.query("SET @a = 1, @b = 2, @c = 3, @d = 4");
    c.query("EXECUTE STMT USING @a, @b, @c, @d");

    do_replay_and_checksum(test);
}

void test_main(TestConnections& test)
{
    sanity_check(test);
    simple_binary_ps(test);
    simple_text_ps(test);
}

ENTERPRISE_TEST_MAIN(test_main)
