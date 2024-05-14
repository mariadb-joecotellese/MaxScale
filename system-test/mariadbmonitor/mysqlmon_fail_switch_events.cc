/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include <maxtest/testconnections.hh>
#include <string>
#include <maxbase/format.hh>
#include <maxtest/mariadb_connector.hh>

using std::string;

namespace
{
const char EVENT_NAME[] = "test_event";
const char EVENT_SHCEDULER[] = "SET GLOBAL event_scheduler = %s;";
const char USE_TEST[] = "USE test;";
const char SET_NAMES[] = "SET NAMES %s COLLATE %s";

const char EV_STATE_ENABLED[] = "ENABLED";
const char EV_STATE_DISABLED[] = "DISABLED";
const char EV_STATE_SLAVE_DISABLED[] = "SLAVESIDE_DISABLED";

const char def_charset[] = "latin1";
const char def_collation[] = "latin1_swedish_ci";

void expect_event_charset_collation(TestConnections& test, int node, const string& event_name,
                                    const string& client_charset, const string& collation_connection);

int read_incremented_field(TestConnections& test)
{
    int rval = -1;
    auto conn = test.maxscale->open_rwsplit_connection2();
    auto res = conn->query("SELECT * FROM test.t1;");
    if (res && res->get_col_count() == 1 && res->next_row())
    {
        rval = res->get_int(0);
    }
    else
    {
        test.add_failure("Could not read value from query result.");
    }
    return rval;
}

bool field_is_incrementing(TestConnections& test)
{
    int old_val = read_incremented_field(test);
    sleep(2);   // Should be enough to allow the event to run once.
    // Check that the event is running and increasing the value
    int new_val = read_incremented_field(test);
    return new_val > old_val;
}

void create_event(TestConnections& test)
{
    auto& mxs = *test.maxscale;

    // Create table, enable scheduler and add an event
    test.tprintf("Creating table, inserting data and scheduling an event.");

    auto conn = mxs.open_rwsplit_connection2_nodb();
    const char create_event_query[] = "CREATE EVENT %s ON SCHEDULE EVERY 1 SECOND "
                                      "DO UPDATE test.t1 SET c1 = c1 + 1;";

    if (conn->cmd_f(EVENT_SHCEDULER, "ON")
        && conn->cmd_f("CREATE OR REPLACE TABLE test.t1(c1 INT);")
        && conn->cmd(USE_TEST)
        && conn->cmd("INSERT INTO t1 VALUES (1);")
        && conn->cmd_f(SET_NAMES, def_charset, def_collation)
        && conn->cmd_f(create_event_query, EVENT_NAME))
    {
        mxs.wait_for_monitor();
        mxs.get_servers().print();
        test.repl->sync_slaves();
        // Check that the event is running and increasing the value
        test.expect(field_is_incrementing(test),
                    "Value in column did not increment. Current value %i.", read_incremented_field(test));
    }
}

void delete_event(TestConnections& test)
{
    auto conn = test.maxscale->open_rwsplit_connection2();
    conn->cmd_f(EVENT_SHCEDULER, "OFF");
    conn->cmd(USE_TEST);
    conn->cmd_f("DROP EVENT IF EXISTS %s;", EVENT_NAME);
    test.repl->sync_slaves();
}

bool expect_event_status(TestConnections& test, int node,
                         const string& event_name, const string& expected_state)
{
    bool rval = false;
    string query = "SELECT * FROM information_schema.EVENTS WHERE EVENT_NAME = '" + event_name + "';";
    auto be = test.repl->backend(node);
    be->ping_or_open_admin_connection();
    auto conn = be->admin_connection();
    auto res = conn->query(query);
    if (res && res->next_row())
    {
        string status = res->get_string("STATUS");
        if (status != expected_state)
        {
            test.add_failure("Wrong event status, found %s when %s was expected.",
                             status.c_str(), expected_state.c_str());
        }
        else
        {
            rval = true;
            test.tprintf("Event '%s' is '%s' on node %i as it should.",
                         event_name.c_str(), status.c_str(), node);
        }
    }
    return rval;
}

void set_event_state(TestConnections& test, const string& event_name, const string& new_state)
{
    auto conn = test.maxscale->open_rwsplit_connection2();
    const char alter_fmt[] = "ALTER EVENT %s %s;";

    if (conn->try_cmd(USE_TEST) && conn->try_cmd_f(SET_NAMES, def_charset, def_collation)
        && conn->try_cmd_f(alter_fmt, event_name.c_str(), new_state.c_str()))
    {
        test.tprintf("Event '%s' set to '%s'.", event_name.c_str(), new_state.c_str());
    }
    else
    {
        test.add_failure("ALTER EVENT failed");
    }
}

void switchover(TestConnections& test, const string& new_master)
{
    string switch_cmd = "call command mysqlmon switchover MariaDB-Monitor " + new_master;
    test.check_maxctrl(switch_cmd);
    test.maxscale->wait_for_monitor(2);
    // Check success.
    auto new_master_status = test.maxscale->get_servers().get(new_master);
    test.expect(new_master_status.status == mxt::ServerInfo::master_st,
                "%s is not master as expected. Status: %s.",
                new_master.c_str(), new_master_status.status_to_string().c_str());
}

void test_main(TestConnections& test)
{
    auto& mxs = *test.maxscale;
    auto& repl = *test.repl;

    auto servers = mxs.get_servers();
    servers.check_servers_status(mxt::ServersInfo::default_repl_states());

    int server1_ind = 0;
    int server2_ind = 1;
    auto server1_name = servers.get(server1_ind).name;
    auto server2_name = servers.get(server2_ind).name;

    if (test.ok())
    {
        delete_event(test);
        // Schedule a repeating event.
        create_event(test);

        int master_id_begin = test.get_master_server_id();

        if (test.ok())
        {
            // Part 1: Do a failover
            test.tprintf("Step 1: Stop master and wait for failover. Check that another server is promoted.");
            repl.stop_node(0);
            mxs.wait_for_monitor(3);
            int master_id_failover = mxs.get_master_server_id();
            test.tprintf("Master server id is %i.", master_id_failover);
            test.expect(master_id_failover > 0 && master_id_failover != master_id_begin,
                        "Master did not change or no master detected.");
            // Check that events are still running.
            test.expect(field_is_incrementing(test),
                        "Value in column did not increment. Current value %i.",
                        read_incremented_field(test));
        }

        if (test.ok())
        {
            // Part 2: Start node 0, let it join the cluster and check that the event is properly disabled.
            test.tprintf("Step 2: Restart server 1. It should join the cluster.");
            repl.start_node(0);
            mxs.wait_for_monitor(4);

            auto states = mxs.get_servers().get(0);
            test.expect(states.status == mxt::ServerInfo::slave_st,
                        "Old master is not a slave as expected. Status: %s",
                        states.status_to_string().c_str());
            if (test.ok())
            {
                // Old master joined as slave, check that event is disabled.
                expect_event_status(test, server1_ind, EVENT_NAME, EV_STATE_SLAVE_DISABLED);
            }
        }

        if (test.ok())
        {
            // Part 3: Switchover back to server1 as master. The event will most likely not run because the
            // old master doesn't have event scheduler on anymore.
            test.tprintf("Step 3: Switchover back to server1. Check that event is enabled on previous master. "
                         "Don't check that the event is running since the scheduler process is likely off.");
            switchover(test, server1_name);
            if (test.ok())
            {
                expect_event_status(test, server1_ind, EVENT_NAME, EV_STATE_ENABLED);
            }
        }

        if (test.ok())
        {
            // Part 4: Disable the event on master. The event should still be "SLAVESIDE_DISABLED" on slaves.
            // Check that after switchover, the event is not enabled.
            test.tprintf("Step 4: Disable event on master, switchover to server2. "
                         "Check that event is still disabled.");
            set_event_state(test, EVENT_NAME, "DISABLE");
            mxs.wait_for_monitor();     // Wait for the monitor to detect the change.
            expect_event_status(test, 0, EVENT_NAME, EV_STATE_DISABLED);
            expect_event_status(test, 1, EVENT_NAME, EV_STATE_SLAVE_DISABLED);

            if (test.ok())
            {
                test.tprintf("Event is disabled on master and slaveside-disabled on slave.");
                switchover(test, server2_name);
                if (test.ok())
                {
                    // Event should not have been touched.
                    expect_event_status(test, server2_ind, EVENT_NAME, EV_STATE_SLAVE_DISABLED);
                }

                // Switchover back.
                switchover(test, server1_name);
            }
            mxs.check_print_servers_status(mxt::ServersInfo::default_repl_states());
        }

        if (test.ok())
        {
            // MXS-3158 Check that monitor preserves the character set and collation of an event
            // when altering it.
            test.tprintf("Step 5: Check event handling with non-default charset and collation.");

            expect_event_charset_collation(test, server1_ind, EVENT_NAME, def_charset, def_collation);
            if (test.ok())
            {
                // Alter event charset to utf8.
                const char new_charset[] = "utf8mb4";
                const char new_collation[] = "utf8mb4_estonian_ci";

                auto conn = mxs.open_rwsplit_connection2("test");
                conn->cmd_f("SET NAMES %s COLLATE %s;", new_charset, new_collation);
                conn->cmd_f("ALTER EVENT %s ENABLE;", EVENT_NAME);
                repl.sync_slaves();

                expect_event_status(test, server1_ind, EVENT_NAME, EV_STATE_ENABLED);
                expect_event_charset_collation(test, server1_ind, EVENT_NAME, new_charset, new_collation);
                expect_event_status(test, server2_ind, EVENT_NAME, EV_STATE_SLAVE_DISABLED);
                expect_event_charset_collation(test, server2_ind, EVENT_NAME, new_charset, new_collation);

                if (test.ok())
                {
                    switchover(test, server2_name);
                    if (test.ok())
                    {
                        expect_event_status(test, server1_ind, EVENT_NAME, EV_STATE_SLAVE_DISABLED);
                        expect_event_charset_collation(test, server1_ind, EVENT_NAME, new_charset,
                                                       new_collation);
                        expect_event_status(test, server2_ind, EVENT_NAME, EV_STATE_ENABLED);
                        expect_event_charset_collation(test, server2_ind, EVENT_NAME, new_charset,
                                                       new_collation);
                    }

                    // Switchover back.
                    switchover(test, server1_name);
                }
            }
        }

        delete_event(test);
    }
}

void expect_event_charset_collation(TestConnections& test, int node, const string& event_name,
                                    const string& client_charset, const string& collation_connection)
{
    auto be = test.repl->backend(node);
    be->ping_or_open_admin_connection();
    auto conn = be->admin_connection();
    string query = mxb::string_printf("select CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION "
                                      "from information_schema.EVENTS where EVENT_NAME = '%s';",
                                      event_name.c_str());
    auto res = conn->query(query);
    if (res && res->next_row())
    {
        string found_charset = res->get_string(0);
        string found_collation = res->get_string(1);

        test.tprintf("Event '%s': CHARACTER_SET_CLIENT is '%s', COLLATION_CONNECTION is '%s'",
                     EVENT_NAME, found_charset.c_str(), found_collation.c_str());
        const char error_fmt[] = "Wrong %s. Found %s, expected %s.";
        test.expect(found_charset == client_charset, error_fmt, "CHARACTER_SET_CLIENT",
                    found_charset.c_str(), client_charset.c_str());
        test.expect(found_collation == collation_connection, error_fmt, "COLLATION_CONNECTION",
                    found_collation.c_str(), collation_connection.c_str());
    }
    else
    {
        test.add_failure("Query '%s' failed.", query.c_str());
    }
}
}

int main(int argc, char** argv)
{
    TestConnections test;
    return test.run_test(argc, argv, test_main);
}
