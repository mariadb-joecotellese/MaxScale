/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcar_common.hh"

void sanity_check(TestConnections& test)
{
    test.tprintf("%s", __func__);

    test.repl->connect();
    test.repl->execute_query_all_nodes("SET GLOBAL max_allowed_packet=33554432");
    test.repl->disconnect();

    Cleanup cleanup(test);
    cleanup.add_table("test.wcar_basic");
    cleanup.add_files("/tmp/replay.rx", "/tmp/replay.csv", "/tmp/converted.cx",
                      "/tmp/converted.rx", "/tmp/converted.csv", "/tmp/converted2.csv",
                      "/tmp/output-full.csv", "/tmp/output.csv", "/tmp/dump.txt", "/tmp/canonicals.csv");

    // This'll catch any massive problems in the executable itself
    MXT_EXPECT(test.maxscale->ssh_node("maxplayer --help", false) == 0);

    auto c = test.maxscale->rwsplit();
    MXT_EXPECT(c.connect());

    // 1. Do a capture
    // 2. Replay the capture
    // 3. Check that the contents of the database are identical
    int queries = 0;

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

        "SELECT \n 1, \t 2, \r\n 3",
        "SELECT THIS SHOULD BE A SYNTAX ERROR",
        "SELECT 'hello world'",
    })
    {
        MXT_EXPECT_F(c.query(query) || query.find("SYNTAX ERROR") != std::string::npos,
                     "Query %s failed: %s", query.c_str(), c.error());
        ++queries;
    }

    MXT_EXPECT_F(c.query("SELECT '" + std::string(1024 * 1024 * 20, 'a') + "'"),
                 "Large multi-packet query failed: %s", c.error());
    // TODO: Once MXS-5099 is fixed, uncomment this line.
    // ++queries;

    test.maxscale->stop();

    //
    // Check that the expected files exist and that the contents of them are what we expect them to be.
    //
    auto res = test.maxscale->ssh_output("find /var/lib/maxscale/wcar/WCAR/ -type f");
    std::set<std::string> files;
    std::set<std::string> expected{".cx", ".ex", ".gx"};
    std::string replay_file;
    std::string event_file;
    std::string gtid_file;

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
        else if (file.extension() == ".gx")
        {
            gtid_file = file;
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
                 "The failure to open should be reported: %s", res.output.c_str());

    test.tprintf("Attempting a summary with the correct file permissions");
    res = test.maxscale->ssh_output("maxplayer summary " + replay_file + " 2>&1", true);
    std::string summary = res.output;
    MXT_EXPECT_F(res.rc == 0, "'maxplayer summary' should work as root: %s", summary.c_str());

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
    int lines = 0;

    for (auto line : mxb::strtok(MAKE_STR(std::ifstream("./replay.csv").rdbuf()), "\n"))
    {
        lines++;
    }

    MXT_EXPECT_F(lines == queries + 1, "Expected %d lines but only found %d", queries + 1, lines);

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

    auto replay_cmd = MAKE_STR(ASAN_OPTS
                               << "maxplayer replay"
                               << " -u " << test.repl->user_name()
                               << " -p " << test.repl->password()
                               << " -H " << test.repl->ip(0) << ":" << test.repl->port(0)
                               << " ");

    for (auto opt : options)
    {
        test.tprintf("Replay options: %s", opt.c_str());

        test.maxscale->ssh_node("rm -f /tmp/output.csv", true);
        MXT_EXPECT(m.query("DROP TABLE test.wcar_basic"));
        rc = test.maxscale->ssh_node_f(true, "%s %s %s", replay_cmd.c_str(), replay_file.c_str(),
                                       opt.c_str());
        MXT_EXPECT_F(rc == 0, "'maxplayer replay' with '%s' failed.", opt.c_str());
    }

    test.tprintf("The 'show' output of event 1 should be 'CREATE TABLE'");
    res = test.maxscale->ssh_output("maxplayer show " + replay_file + " 1 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer show' should work: %s", res.output.c_str());
    MXT_EXPECT_F(res.output.find("CREATE TABLE") != std::string::npos,
                 "Output should contain 'CREATE TABLE': %s", res.output.c_str());

    test.tprintf("The 'show' event for the first GTID should be 'CREATE TABLE'");
    std::string needle = "First GTID: \"";
    auto pos = summary.find(needle);
    MXT_EXPECT(pos != std::string::npos);
    pos += needle.size();
    auto gtid = summary.substr(pos, summary.find('"', pos + 1) - pos);
    res = test.maxscale->ssh_output("maxplayer show " + replay_file + " " + gtid + " 2>&1", true);
    MXT_EXPECT_F(res.rc == 0, "'maxplayer show' with GTID should work: %s", res.output.c_str());
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

    test.tprintf("Converting a .csv should result in an error");
    res = test.maxscale->ssh_output("maxplayer convert /tmp/converted2.csv --csv -o /tmp/converted3.csv 2>&1",
                                    true);
    MXT_EXPECT_F(res.rc != 0, "Convert from .csv should fail");

    cleanup.add_files("/tmp/readable.cx", "/tmp/readable.ex", "/tmp/readable.gx",
                      "/tmp/readable.rx", "/tmp/readable.tx");
    test.maxscale->ssh_node_f(true,
                              "cp %s /tmp/readable.cx;cp %s /tmp/readable.ex;cp %s /tmp/readable.gx;"
                              "chmod a+rwx /tmp/readable.*",
                              replay_file.c_str(), event_file.c_str(), gtid_file.c_str());
    std::string good_cmd = replay_cmd + "--csv -o /dev/null /tmp/readable.cx";
    MXT_EXPECT(m.query("DROP TABLE test.wcar_basic"));
    res = test.maxscale->ssh_output(good_cmd, false);
    MXT_EXPECT_F(res.rc == 0, "Replay failed: %s", res.output.c_str());

    for (std::string bad_cmd : std::vector<std::string> {
        "maxplayer",
        "maxplayer foo",
        "maxplayer foo bar",
        "maxplayer show",
        "maxplayer show foo",
        "maxplayer show -o foo",
        "maxplayer show foo bar",
        "maxplayer show 1 " + gtid,
        "maxplayer canonicals file that does not exist",
        "maxplayer replay",
        good_cmd + " --commit-order=anything",
        good_cmd + " --csv=perhaps",
        good_cmd + " --host=/",
        good_cmd + " --foo=bar",
        good_cmd + " --csv -o /tmp/foobar",
    })
    {
        res = test.maxscale->ssh_output(bad_cmd, false);
        MXT_EXPECT_F(res.rc != 0, "Command did not fail: %s", bad_cmd.c_str());
    }

    // Check that the diagnostic output works
    test.maxctrl("show filters");
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

void load_data_local_infile(TestConnections& test)
{
    test.tprintf("%s", __func__);
    Cleanup cleanup(test);

    std::ofstream out("./data.csv");

    for (int i = 0; i < 10; i++)
    {
        out << i << "," << i + 1 << "," << i + 2 << "\n";
    }

    out.close();

    auto c = test.maxscale->rwsplit();
    MXT_EXPECT(c.connect());
    MXT_EXPECT(c.query("CREATE TABLE test.wcar_ldli(a INT, b INT, c INT)"));
    cleanup.add_table("test.wcar_ldli");
    MXT_EXPECT(c.query("LOAD DATA LOCAL INFILE 'data.csv' INTO TABLE test.wcar_ldli "
                       "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"));
    c.disconnect();

    test.maxscale->stop();

    auto m = test.repl->get_connection(0);
    MXT_EXPECT(m.connect());
    auto before = m.field("SELECT COUNT(*) FROM test.wcar_ldli");
    MXT_EXPECT(m.query("DROP TABLE test.wcar_ldli"));

    do_replay_and_checksum(test);

    auto after = m.field("SELECT COUNT(*) FROM test.wcar_ldli");
    // Once MXS-5100 is fixed, change this to after == before
    MXT_EXPECT_F(after == "0", "Expected 0 rows but got %s", after.c_str());

    remove("./data.csv");
}

void test_main(TestConnections& test)
{
    sanity_check(test);
    simple_binary_ps(test);
    simple_text_ps(test);
    load_data_local_infile(test);
}

ENTERPRISE_TEST_MAIN(test_main)
