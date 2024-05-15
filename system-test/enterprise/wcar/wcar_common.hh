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
    std::mutex               m_table_lock;
};

mxb::Json get_capture_status(TestConnections& test, std::string filter = "WCAR")
{
    mxb::Json js;
    js.load_string(test.maxctrl("api get filters/" + filter + " data.attributes.filter_diagnostics").output);
    return js;
}
