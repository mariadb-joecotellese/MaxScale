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

    bool wait_for_state(mxb::Json current_status,
                        std::string_view expected_state,
                        time_t max_wait = -1)
    {
        std::cout << "Waiting for state '" << expected_state << "'." << std::endl;
        std::cout << "State: " << std::flush;

        bool reached = false;
        bool abort = false;

        time_t start = time(nullptr);

        do
        {
            time_t now = time(nullptr);

            abort = (max_wait != -1 && now - start >= max_wait);

            auto meta = current_status.get_object("meta");
            auto state = meta.get_string("state");

            std::cout << state << " " << std::flush;

            if (state == expected_state)
            {
                reached = true;
            }
            else if (!abort)
            {
                sleep(1);
                current_status = status();
            }
        }
        while (!reached && !abort);

        std::cout << std::endl;

        return reached;
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
