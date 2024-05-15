/*
 * Copyright (c) 2024 MariaDB plc
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include <iostream>
#include <maxbase/string.hh>
#include <maxtest/testconnections.hh>
#include <maxtest/maxrest.hh>
#include "../enterprise_test.hh"

using namespace std;

namespace
{

class Diff
{
public:
    Diff(std::string_view name,
         TestConnections* pTest)
        : m_name(name)
        , m_test(*pTest)
        , m_maxrest(pTest)
    {
    }

    const std::string& name() const
    {
        return m_name;
    }

    TestConnections& test() const
    {
        return m_test;
    }

    static Diff create(TestConnections* pTest,
                       const std::string& diff_service,
                       const std::string& service,
                       const std::string& a_server,
                       const std::string& another_server)
    {
        MaxRest maxrest(pTest);

        call_command(maxrest, MaxRest::POST, "create", diff_service, CallRepeatable::NO,
                     { service, a_server, another_server });

        return Diff(diff_service, pTest);
    }

    void set_explain_always(bool explain_always)
    {
        m_maxrest.alter_service(m_name, "explain_always", explain_always);
    }

    void set_explain_period(std::chrono::milliseconds explain_period)
    {
        std::stringstream ss;
        ss << explain_period.count() << "ms";

        m_maxrest.alter_service(m_name, "explain_period", ss.str());
    }

    void set_samples(int64_t samples)
    {
        m_maxrest.alter_service(m_name, "samples", samples);
    }

    mxb::Json start() const
    {
        return call_command(MaxRest::POST, "start", m_name, CallRepeatable::NO);
    }

    mxb::Json status() const
    {
        return call_command(MaxRest::POST, "status", m_name, CallRepeatable::YES);
    }

    mxb::Json stop() const
    {
        return call_command(MaxRest::POST, "stop", m_name, CallRepeatable::NO);
    }

    mxb::Json summary() const
    {
        return call_command(MaxRest::GET, "summary", m_name, CallRepeatable::YES, {"return"});
    }

    mxb::Json destroy() const
    {
        return call_command(MaxRest::POST, "destroy", m_name, CallRepeatable::NO);
    }

private:
    enum class CallRepeatable
    {
        NO,
        YES
    };

    mxb::Json call_command(MaxRest::Verb verb,
                           const std::string& command,
                           const std::string& instance,
                           CallRepeatable call_repeatable,
                           const std::vector<std::string>& params = std::vector<std::string>()) const
    {
        return call_command(m_maxrest, verb, command, instance, call_repeatable, params);
    }

    static mxb::Json call_command(MaxRest& maxrest,
                                  MaxRest::Verb verb,
                                  const std::string& command,
                                  const std::string& instance,
                                  CallRepeatable call_repeatable,
                                  const std::vector<std::string>& params = std::vector<std::string>())
    {
        auto& test = maxrest.test();

        std::stringstream ss;
        ss << "diff " << command << " " << instance;
        auto args = mxb::join(params, " ");
        if (!args.empty())
        {
            ss << " " << args;
        }

        auto rv = maxrest.call_command(verb, "diff", command, instance, params);

        if (call_repeatable == CallRepeatable::NO)
        {
            auto fail_on_error = maxrest.fail_on_error();
            maxrest.fail_on_error(false);
            bool failed = false;

            try
            {
                // Since the call succeeded, the state has changed and it should not
                // be possible to call again using the same arguments.
                maxrest.call_command(verb, "diff", command, instance, params);
            }
            catch (const std::exception& x)
            {
                failed = true;
            }

            maxrest.fail_on_error(fail_on_error);

            test.expect(failed, "Command succeeded although it should not have: %s", ss.str().c_str());
        }

        return rv;
    }

    std::string      m_name;
    TestConnections& m_test;
    mutable MaxRest  m_maxrest;
};

/**
 * Simplest possible case, no concurrent activity.
 */
void test_easy_setup(TestConnections& test)
{
    cout << "Easy case, no concurrent activity." << endl;

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", "server1", "server2");
    diff.status();
    diff.start();
    diff.status();
    diff.summary();
    diff.stop();
    diff.destroy();
}

void n_fast_selects(TestConnections* pTest, int n)
{
    auto& mxt = *pTest->maxscale;

    Connection c(mxt.ip4(), 4006, "skysql", "skysql");

    if (c.connect())
    {
        for (int i = 0; i < n; ++i)
        {
            c.query("SELECT 1");
        }
    }
    else
    {
        pTest->expect(false, "Could not connect to MaxScale.");
    }
}

void busy_fast_selects(TestConnections* pTest, std::atomic<bool>* pStop)
{
    auto& mxt = *pTest->maxscale;

    Connection c(mxt.ip4(), 4006, "skysql", "skysql");

    if (c.connect())
    {
        auto& stop = *pStop;

        while (!stop)
        {
            c.query("SELECT 1");
        }
    }
    else
    {
        pTest->expect(false, "Could not connect to MaxScale.");
    }
}

void busy_slow_selects(TestConnections* pTest, std::atomic<bool>* pStop)
{
    auto& mxt = *pTest->maxscale;

    Connection c(mxt.ip4(), 4006, "skysql", "skysql");

    if (c.connect())
    {
        auto& stop = *pStop;

        while (!stop)
        {
            c.query("BEGIN");
            c.query("SELECT SLEEP(5)");
            c.query("COMMIT");
        }
    }
    else
    {
        pTest->expect(false, "Could not connect to MaxScale.");
    }
}

bool wait_for_state(Diff& diff, mxb::Json& json,
                    std::string_view expected_state,
                    time_t max_wait = -1)
{
    cout << "Waiting for state '" << expected_state << "'." << endl;
    cout << "State: " << flush;

    bool reached = false;
    bool abort = false;

    time_t start = time(nullptr);

    do
    {
        time_t now = time(nullptr);

        abort = (max_wait != -1 && now - start >= max_wait);

        auto meta = json.get_object("meta");
        auto state = meta.get_string("state");

        cout << state << " " << flush;

        if (state == expected_state)
        {
            reached = true;
        }
        else if (!abort)
        {
            sleep(1);
            json = diff.status();
        }
    }
    while (!reached && !abort);

    cout << endl;

    return reached;
}

/**
 * Hard case, concurrent activity ongoing.
 */
void test_hard_setup(TestConnections& test)
{
    cout << "Hard case, concurrent activity ongoing." << endl;

    // Setup

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", "server1", "server2");

    std::atomic<bool> stop { false };
    vector<std::thread> clients;

    size_t i = 0;
    for (; i < 5; ++i)
    {
        clients.emplace_back(busy_fast_selects, &diff.test(), &stop);
    }

    for (; i < 10; ++i)
    {
        clients.emplace_back(busy_slow_selects, &diff.test(), &stop);
    }

    sleep(1);

    mxb::Json json;

    json = diff.start();

    wait_for_state(diff, json, "comparing");

    // Tear down

    json = diff.stop();

    sleep(1);

    stop = true;

    wait_for_state(diff, json, "created");

    for (i = 0; i < clients.size(); ++i)
    {
        clients[i].join();
    }

    diff.destroy();
}

/**
 * Hard case, abort setup.
 */
void test_abort_setup(TestConnections& test)
{
    cout << "Hard case, setup is aborted." << endl;

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", "server1", "server2");

    std::atomic<bool> stop { false };
    vector<std::thread> clients;

    size_t i = 0;
    for (; i < 5; ++i)
    {
        clients.emplace_back(busy_slow_selects, &diff.test(), &stop);
    }

    mxb::Json json;

    json = diff.start();

    // The slow selects take 5 seconds. Thus, waiting for the "comparing" state for
    // 2 seconds, should fail.
    bool started = wait_for_state(diff, json, "comparing", 2);

    test.expect(!started, "Diff should not have started.");

    diff.stop();

    json = diff.status();

    auto meta = json.get_object("meta");
    auto state = meta.get_string("state");

    test.expect(state == "created", "Should have been back at 'created' state.");

    diff.destroy();

    stop = true;

    for (i = 0; i < clients.size(); ++i)
    {
        clients[i].join();
    }
}

/**
 * Ensure that EXPLAINs are made by turning on 'explain_always'.
 */
void test_with_explain(TestConnections& test)
{
    Diff diff = Diff::create(&test, "DiffMyService", "MyService", "server1", "server2");

    int samples = 100;
    diff.set_samples(samples); // Less samples so that we dont have to wait for so long.
    diff.set_explain_always(true); // Always EXPLAIN
    diff.set_explain_period(std::chrono::milliseconds { 2000 }); // Short period, to trigger activities.

    mxb::Json json = diff.start();

    bool started = wait_for_state(diff, json, "comparing", 2);

    n_fast_selects(&test, samples + 1);

    std::atomic<bool> stop { false };
    vector<std::thread> clients;

    size_t i = 0;
    for (; i < 5; ++i)
    {
        clients.emplace_back(busy_fast_selects, &diff.test(), &stop);
    }

    sleep(5);

    stop = true;

    for (i = 0; i < clients.size(); ++i)
    {
        clients[i].join();
    }

    diff.stop();
    diff.destroy();
}

void test_main(TestConnections& test)
{
    test_easy_setup(test);
    test_hard_setup(test);
    test_abort_setup(test);
    test_with_explain(test);
}

}

ENTERPRISE_TEST_MAIN(test_main)
