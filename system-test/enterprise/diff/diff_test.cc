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

#include <maxbase/string.hh>
#include <maxtest/testconnections.hh>
#include <maxtest/maxrest.hh>
#include "../enterprise_test.hh"

namespace
{

class Diff
{
public:
    Diff(TestConnections* pTest)
        : m_test(*pTest)
        , m_maxrest(pTest)
    {
    }

    void prepare(const std::string& service,
                 const std::string& a_server,
                 const std::string& another_server) const
    {
        call_command(MaxRest::POST, "prepare", service, CallRepeatable::NO, { a_server, another_server });
    }

    void start(const std::string& service) const
    {
        call_command(MaxRest::POST, "start", service, CallRepeatable::NO);
    }

    void status(const std::string& service) const
    {
        call_command(MaxRest::POST, "status", service, CallRepeatable::YES);
    }

    void stop(const std::string& service) const
    {
        call_command(MaxRest::POST, "stop", service, CallRepeatable::NO);
    }

    void summary(const std::string& service) const
    {
        call_command(MaxRest::GET, "summary", service, CallRepeatable::YES, {"return"});
    }

    void unprepare(const std::string& service) const
    {
        call_command(MaxRest::POST, "unprepare", service, CallRepeatable::NO);
    }

private:
    enum class CallRepeatable
    {
        NO,
        YES
    };

    void call_command(MaxRest::Verb verb,
                      const std::string& command,
                      const std::string& instance,
                      CallRepeatable call_repeatable,
                      const std::vector<std::string>& params = std::vector<std::string>()) const
    {
        std::stringstream ss;
        ss << "diff " << command << " " << instance;
        auto args = mxb::join(params, " ");
        if (!args.empty())
        {
            ss << " " << args;
        }
        m_test.tprintf("%s\n", ss.str().c_str());
        m_maxrest.call_command(verb, "diff", command, instance, params);

        if (call_repeatable == CallRepeatable::NO)
        {
            auto fail_on_error = m_maxrest.fail_on_error();
            m_maxrest.fail_on_error(false);
            bool failed = false;

            try
            {
                // Since the call succeeded, the state has changed and it should not
                // be possible to call again using the same arguments.
                m_maxrest.call_command(verb, "diff", command, instance, params);
            }
            catch (const std::exception& x)
            {
                failed = true;
            }

            m_maxrest.fail_on_error(fail_on_error);

            m_test.expect(failed, "Command succeeded although it should not have: %s", ss.str().c_str());
        }
    }

    TestConnections& m_test;
    mutable MaxRest  m_maxrest;
};

void test_easy_setup(TestConnections& test)
{
    // No concurrent clients.

    Diff diff(&test);

    diff.prepare("MyService", "server1", "server2");
    diff.status("DiffMyService");
    diff.start("DiffMyService");
    diff.status("DiffMyService");
    diff.summary("DiffMyService");
    diff.stop("DiffMyService");
    diff.unprepare("DiffMyService");
}

void test_main(TestConnections& test)
{
    test_easy_setup(test);
}

}

ENTERPRISE_TEST_MAIN(test_main)
