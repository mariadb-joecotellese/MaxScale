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
        test.tprintf("%s\n", ss.str().c_str());

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

void test_easy_setup(TestConnections& test)
{
    // No concurrent clients.

    Diff diff = Diff::create(&test, "DiffMyService", "MyService", "server1", "server2");
    diff.status();
    diff.start();
    diff.status();
    diff.summary();
    diff.stop();
    diff.destroy();
}

void test_main(TestConnections& test)
{
    test_easy_setup(test);
}

}

ENTERPRISE_TEST_MAIN(test_main)
