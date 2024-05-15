/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

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
        std::lock_guard guard(m_table_lock);
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
        m_test.maxscale->ssh_node("rm -rf /var/lib/maxscale/wcar/* /tmp/replay.csv "
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
    std::mutex               m_table_lock;
};

mxb::Json get_capture_status(TestConnections& test, std::string filter = "WCAR")
{
    mxb::Json js;
    js.load_string(test.maxctrl("api get filters/" + filter + " data.attributes.filter_diagnostics").output);
    return js;
}

void copy_capture(TestConnections& test, std::string src, std::string dest)
{
    int rc = test.maxscale->ssh_node("cp -r /var/lib/maxscale/wcar/" + src
                                     + " /var/lib/maxscale/wcar/" + dest, true);
    test.expect(rc == 0, "Failed to copy capture files");
}

void do_replay(TestConnections& test, std::string filter, std::string options = "")
{
    try
    {
        int rc = test.maxscale->ssh_node_f(
            true,
            ASAN_OPTS
            "maxplayer replay -u %s -p %s -H %s:%d --csv -o /tmp/replay-%s.csv "
            "%s /var/lib/maxscale/wcar/%s/*.cx",
            test.repl->user_name().c_str(), test.repl->password().c_str(),
            test.repl->ip(0), test.repl->port(0),
            filter.c_str(), options.c_str(), filter.c_str());

        MXT_EXPECT_F(rc == 0, "Replay should work.");
        auto res = test.maxscale->ssh_output("wc -l /tmp/replay-" + filter + ".csv");
        MXT_EXPECT_F(std::stoi(res.output) > 1,
                     "Replay should generate a CSV file with at least one line: %s", res.output.c_str());
    }
    catch (const std::exception& e)
    {
        test.add_failure("Caught exception: %s", e.what());
    }

    test.maxscale->ssh_output("rm /tmp/replay-" + filter + ".csv");
}
