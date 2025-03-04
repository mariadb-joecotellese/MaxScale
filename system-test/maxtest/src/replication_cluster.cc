/*
 * Copyright (c) 2021 MariaDB Corporation Ab
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

#include <maxtest/replication_cluster.hh>

#include <iostream>
#include <thread>
#include <maxbase/format.hh>
#include <maxbase/stopwatch.hh>
#include <maxbase/string.hh>
#include <maxtest/log.hh>
#include <maxtest/mariadb_connector.hh>

using std::string;
using std::cout;
using std::endl;
using ServerArray = std::vector<mxt::MariaDBServer*>;

namespace
{
const string type_mariadb = "mariadb";
const string my_nwconf_prefix = "node";
const string my_name = "Master-Slave-cluster";

const char create_repl_user[] =
    "grant replication slave on *.* to repl@'%%' identified by 'repl'; "
    "FLUSH PRIVILEGES";
const char setup_slave[] =
    "change master to MASTER_HOST='%s', MASTER_PORT=%d, "
    "MASTER_USER='repl', MASTER_PASSWORD='repl', "
    "MASTER_USE_GTID=current_pos; "
    "start slave;";

const string sl_io = "Slave_IO_Running";
const string sl_sql = "Slave_SQL_Running";
const string show_slaves = "show all slaves status;";

const string repl_user = "repl";
const string repl_pw = "repl";

bool repl_thread_run_states_ok(const string& io, const string& sql)
{
    return (io == "Yes" || io == "Connecting" || io == "Preparing") && sql == "Yes";
}

bool repl_threads_running(const string& io, const string& sql)
{
    return io == "Yes" && sql == "Yes";
}

bool is_writable(mxt::MariaDB* conn)
{
    bool rval = false;
    auto res = conn->try_query("select @@read_only;");
    if (res && res->next_row() && res->get_bool(0) == false)
    {
        rval = true;
    }
    return rval;
}
}

namespace maxtest
{
ReplicationCluster::ReplicationCluster(SharedData* shared)
    : MariaDBCluster(shared, "server")
{
}

const std::string& ReplicationCluster::type_string() const
{
    return type_mariadb;
}

bool ReplicationCluster::setup_replication()
{
    const int n = N;
    // Generate test admin user on all backends and reset gtids.
    bool gtids_reset = true;
    for (int i = 0; i < n; i++)
    {
        if (create_admin_user(i))
        {
            // The servers now have conflicting gtids but identical data. Set gtids manually so
            // replication can start.
            auto conn = backend(i)->admin_connection();
            if (!conn->try_cmd("RESET MASTER;") || !conn->try_cmd("SET GLOBAL gtid_slave_pos='0-1-0'"))
            {
                gtids_reset = false;
                logger().log_msgf("Gtid reset failed on %s. Cannot setup replication.",
                                  backend(i)->vm_node().name());
            }
        }
        else
        {
            gtids_reset = false;
        }
    }

    bool rval = false;
    if (gtids_reset)
    {
        // Generate other users on master, then setup replication. The generated users should replicate.
        if (create_users())
        {
            bool repl_ok = true;
            // Finally, begin replication.
            string change_master = gen_change_master_cmd(backend(0));
            for (int i = 1; i < n; i++)
            {
                auto conn = backend(i)->admin_connection();
                if (!conn->try_cmd(change_master) || !conn->try_cmd("START SLAVE;"))
                {
                    logger().log_msgf("Failed to start replication on %s. Cannot setup replication.",
                                      backend(i)->vm_node().name());
                    repl_ok = false;
                }
            }

            if (repl_ok && sync_slaves(0, 5))
            {
                logger().log_msgf("Replication setup success on %s.", name().c_str());
                rval = true;
            }
        }
    }
    return rval;
}

bool ReplicationCluster::check_fix_replication()
{
    auto check_disable_read_only = [this](mxt::MariaDBServer* srv) {
        bool rval = false;
        auto conn = srv->admin_connection();
        if (is_writable(conn))
        {
            rval = true;
        }
        else
        {
            if (conn->try_cmd("set global read_only=0;") && is_writable(conn))
            {
                rval = true;
                logger().log_msgf("Read-only disabled on %s", srv->vm_node().m_name.c_str());
            }
            else
            {
                logger().log_msgf("Tried to disable read-only on %s but failed. Error: %s.",
                                  srv->vm_node().m_name.c_str(), conn->error());
            }
        }
        return rval;
    };

    const int n = N;
    bool all_writable = true;
    for (int i = 0; i < n; i++)
    {
        if (!check_disable_read_only(backend(i)))
        {
            all_writable = false;
        }
    }

    bool replication_ok = false;
    if (all_writable)
    {
        // Check that the supposed master is not replicating. If it is, remove the slave connection.
        auto master = backend(0);
        if (remove_all_slave_conns(master))
        {
            bool repl_set_up = true;
            // Master ok, check slaves.
            for (int i = 1; i < n; i++)
            {
                if (!check_fix_replication(backend(i), master))
                {
                    repl_set_up = false;
                }
            }

            if (repl_set_up)
            {
                // Replication should be ok, but test it by writing an event to master.
                if (master->admin_connection()->try_cmd("flush tables;") && sync_slaves())
                {
                    replication_ok = true;
                }
            }
        }
        else
        {
            logger().log_msgf("Failed to remove slave connections from %s.", master->cnf_name().c_str());
        }
    }

    logger().log_msgf("%s %s.", my_name.c_str(), replication_ok ? "replicating" : "not replicating.");
    return replication_ok;
}

bool ReplicationCluster::remove_all_slave_conns(MariaDBServer* server)
{
    bool rval = false;
    auto conn = server->admin_connection();
    auto name = server->vm_node().m_name.c_str();
    if (auto res = conn->try_query(show_slaves); res)
    {
        int rows = res->get_row_count();
        if (rows == 0)
        {
            rval = true;
        }
        else
        {
            logger().log_msgf("%s has %i slave connection(s), removing them.", name, rows);
            if (conn->try_cmd("stop all slaves;"))
            {
                while (res->next_row())
                {
                    string conn_name = res->get_string("Connection_name");
                    conn->try_cmd_f("reset slave '%s' all;", conn_name.c_str());
                }

                if (res = conn->try_query(show_slaves); res)
                {
                    rows = res->get_row_count();
                    if (rows == 0)
                    {
                        rval = true;
                        logger().log_msgf("Slave connection(s) removed from %s.", name);
                    }
                    else
                    {
                        logger().log_msgf("%i slave connection(s) remain on %s.", rows, name);
                    }
                }
            }
        }
    }
    return rval;
}

/**
 * Check replication connection status
 *
 * @return True if all is well
 */
bool ReplicationCluster::check_fix_replication(MariaDBServer* slave, MariaDBServer* master)
{
    auto is_replicating_from_master = [this, slave, master](mxb::QueryResult* res) {
        string host = res->get_string("Master_Host");
        int port = res->get_int("Master_Port");

        bool rval = false;
        if (host == master->vm_node().priv_ip() && port == master->port())
        {
            // Host and port ok, check some additional settings.
            string conn_name = res->get_string("Connection_name");
            int delay = res->get_int("SQL_Delay");
            string using_gtid = res->get_string("Using_Gtid");

            if (conn_name.empty() && delay == 0 && using_gtid == "Slave_Pos")
            {
                string io_running = res->get_string(sl_io);
                string sql_running = res->get_string(sl_sql);

                // Don't accept "Connecting" here as it could take a while before the slave actually
                // reconnects.
                if (repl_threads_running(io_running, sql_running))
                {
                    rval = true;
                }
                else
                {
                    logger().log_msgf("Replication connection from %s to %s is not running. IO: %s, SQL: %s",
                                      slave->cnf_name().c_str(), master->cnf_name().c_str(),
                                      io_running.c_str(), sql_running.c_str());
                }
            }
            else
            {
                logger().log_msgf("Replication connection from %s to %s is not in standard configuration. "
                                  "Conn name: '%s', Delay: %i, Using_Gtid: %s",
                                  slave->cnf_name().c_str(), master->cnf_name().c_str(),
                                  conn_name.c_str(), delay, using_gtid.c_str());
            }
        }
        else
        {
            logger().log_msgf("%s is not replicating from master %s.",
                              slave->cnf_name().c_str(), master->cnf_name().c_str());
        }
        return rval;
    };

    bool recreate = false;
    bool error = false;

    auto conn = slave->admin_connection();
    auto res = conn->try_query(show_slaves);
    if (res)
    {
        int rows = res->get_row_count();
        if (rows > 1)
        {
            // Multisource replication, remove connections.
            if (remove_all_slave_conns(slave))
            {
                recreate = true;
            }
            else
            {
                error = true;
            }
        }
        else if (rows == 1)
        {
            res->next_row();
            if (!is_replicating_from_master(res.get()))
            {
                if (remove_all_slave_conns(slave))
                {
                    recreate = true;
                }
                else
                {
                    error = true;
                }
            }
        }
        else
        {
            // No connection, create one.
            recreate = true;
        }
    }
    else
    {
        error = true;
    }

    bool rval = false;
    if (!error)
    {
        if (recreate)
        {
            string change_cmd = gen_change_master_cmd(master);
            if (conn->try_cmd(change_cmd) && conn->try_cmd("start slave;"))
            {
                // Replication should be starting. Give the slave some time to get started, then check that
                // replication is running.
                std::this_thread::sleep_for(std::chrono::milliseconds(50));

                bool still_connecting = true;
                for (int i = 0; i < 5 && still_connecting; i++)
                {
                    res = conn->try_query(show_slaves);
                    if (res && res->next_row())
                    {
                        string io_running = res->get_string(sl_io);
                        string sql_running = res->get_string(sl_sql);
                        if (repl_threads_running(io_running, sql_running))
                        {
                            rval = true;
                            still_connecting = false;
                        }
                        else if (repl_thread_run_states_ok(io_running, sql_running))
                        {
                            // Taking a bit longer than expected, sleep a bit and try again.
                            if (i < 4)
                            {
                                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            }
                            else
                            {
                                logger().log_msgf(
                                    "%s did not start to replicate from %s within the time limit.",
                                    slave->cnf_name().c_str(), master->cnf_name().c_str());
                            }
                        }
                        else
                        {
                            still_connecting = false;

                            string io_error = res->get_string("Last_IO_Error");
                            string sql_error = res->get_string("Last_SQL_Error");

                            logger().log_msgf(
                                "%s did not start to replicate from %s. IO Error: '%s', SQL Error: '%s'",
                                slave->cnf_name().c_str(), master->cnf_name().c_str(),
                                io_error.c_str(), sql_error.c_str());
                        }
                    }
                    else
                    {
                        still_connecting = false;
                    }
                }
            }
        }
        else
        {
            rval = true;
        }
    }

    return rval;
}

bool ReplicationCluster::sync_slaves()
{
    // Wait a maximum of 10 seconds for sync.
    return sync_slaves(0, 10);
}

bool ReplicationCluster::sync_slaves(int master_node_ind, int time_limit_s)
{
    struct Gtid
    {
        int64_t domain {-1};
        int64_t server_id {-1};
        int64_t seq_no {-1};

        bool operator==(const Gtid& rhs) const
        {
            return domain == rhs.domain && server_id == rhs.server_id && seq_no == rhs.seq_no;
        }
    };

    auto parse_gtid = [](const string& gtid_str) {
        Gtid rval;
        // Only reads the first gtid in case of a list.
        if (sscanf(gtid_str.c_str(), "%li-%li-%li", &rval.domain, &rval.server_id, &rval.seq_no) != 3)
        {
            rval = Gtid();
        }
        return rval;
    };

    struct ReplData
    {
        Gtid gtid;
        bool repl_configured {false};
        bool is_replicating {false};
    };

    auto update_one_server = [&parse_gtid](mxt::MariaDBServer* server, bool require_connected) {
        ReplData rval;
        auto conn = server->admin_connection();
        if (conn->is_open())
        {
            auto res = conn->multiquery({"select @@gtid_current_pos;", "show all slaves status;"});
            if (!res.empty())
            {
                // Got results. When parsing gtid, only consider the first triplet. Typically that's all
                // there is.
                auto& res_gtid = res[0];
                if (res_gtid->next_row())
                {
                    string gtid_current = res_gtid->get_string(0);
                    rval.gtid = parse_gtid(gtid_current);
                }

                auto& slave_ss = res[1];
                if (slave_ss->next_row())
                {
                    rval.repl_configured = true;
                    string io_state = slave_ss->get_string(sl_io);
                    string sql_state = slave_ss->get_string(sl_sql);
                    rval.is_replicating = require_connected ? repl_threads_running(io_state, sql_state) :
                        repl_thread_run_states_ok(io_state, sql_state);
                }
            }
        }
        return rval;
    };

    auto update_all = [this, &update_one_server](const ServerArray& servers, bool require_connected) {
        size_t n = servers.size();
        std::vector<ReplData> rval;
        rval.resize(n);

        mxt::BoolFuncArray funcs;
        funcs.reserve(n);

        for (size_t i = 0; i < n; i++)
        {
            auto func = [&rval, &servers, i, &update_one_server, require_connected]() {
                rval[i] = update_one_server(servers[i], require_connected);
                return true;
            };
            funcs.push_back(std::move(func));
        }
        m_shared.concurrent_run(funcs);
        return rval;
    };

    ping_or_open_admin_connections();
    Gtid master_gtid;
    auto master = backend(master_node_ind);
    auto res = master->admin_connection()->try_query("select @@gtid_current_pos;");
    if (res && res->next_row())
    {
        master_gtid = parse_gtid(res->get_string(0));
    }

    bool all_in_sync = false;
    if (master_gtid.server_id < 0)
    {
        logger().log_msgf("Could not read gtid from master %s when waiting for cluster sync.",
                          master->vm_node().m_name.c_str());
    }
    else
    {
        std::vector<MariaDBServer*> waiting_catchup;
        waiting_catchup.reserve(N - 1);
        for (int i = 0; i < N; i++)
        {
            auto srv = backend(i);
            if (srv != master && srv->admin_connection()->is_open())
            {
                waiting_catchup.push_back(srv);
            }
        }

        int wait_ms = 10;
        int expected_catchups = waiting_catchup.size();
        int successful_catchups = 0;
        mxb::StopWatch timer;
        auto limit = mxb::from_secs(time_limit_s);
        auto connect_limit = limit * 0.75;
        mxb::Duration connect_time {0s};

        int iter = 0;
        do
        {
            // Allow the slave connection to be in "Connecting"-status for 75% of the time limit.
            // If the situation persists after that, assume slave is not replicating and is broken.
            auto repl_data = update_all(waiting_catchup, connect_time >= connect_limit);
            if (verbose())
            {
                logger().log_msgf("Waiting for %zu servers to sync with master.", waiting_catchup.size());
            }

            for (size_t i = 0; i < waiting_catchup.size();)
            {
                auto& elem = repl_data[i];
                bool sync_possible = false;
                bool in_sync = false;

                if (elem.gtid.server_id < 0)
                {
                    // Query or connection failed. Cannot sync.
                }
                else if (elem.gtid == master_gtid)
                {
                    in_sync = true;
                }
                else if (!elem.repl_configured)
                {
                    m_shared.log.log_msgf(" Not in matching gtid and no replication configured. Cannot sync");
                }
                else if (elem.gtid.domain != master_gtid.domain)
                {
                    // If a test uses complicated gtid:s, it needs to handle it on its own.
                    m_shared.log.log_msgf(
                        "Found different gtid domain id:s (%s: %li and %s: %li) when waiting for "
                        "cluster sync.", waiting_catchup[i]->cnf_name().c_str(), elem.gtid.domain,
                        master->cnf_name().c_str(), master_gtid.domain);
                }
                else if (elem.is_replicating)
                {
                    sync_possible = true;
                }
                else
                {
                    m_shared.log.log_msgf("Server is not yet replicating.");
                }

                if (in_sync || !sync_possible)
                {
                    waiting_catchup.erase(waiting_catchup.begin() + i);
                    repl_data.erase(repl_data.begin() + i);
                    if (in_sync)
                    {
                        successful_catchups++;
                    }
                }
                else
                {
                    i++;
                }
            }

            if (!waiting_catchup.empty())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
            }

            wait_ms = std::min(wait_ms * 2, 500);
            iter++;
            connect_time = timer.split();
        }
        while (!waiting_catchup.empty() && connect_time < limit);

        if (successful_catchups == expected_catchups)
        {
            all_in_sync = true;
            if (verbose())
            {
                logger().log_msgf("Slave sync took %.1f seconds.", mxb::to_secs(timer.split()));
            }
        }
        else
        {
            std::vector<string> names;
            for (const auto* srv : waiting_catchup)
            {
                names.emplace_back(srv->cnf_name());
            }
            string list = mxb::create_list_string(names);
            logger().log_msgf("Only %i out of %i servers in the cluster got in sync within %.1f seconds. "
                              "Failed servers: %s",
                              successful_catchups, expected_catchups, mxb::to_secs(timer.split()),
                              list.c_str());
        }
    }
    return all_in_sync;
}

void ReplicationCluster::change_master(int NewMaster, int OldMaster)
{
    auto* new_master = backend(NewMaster);
    for (int i = 0; i < N; i++)
    {
        if (mysql_ping(nodes[i]) == 0)
        {
            execute_query(nodes[i], "STOP SLAVE");
        }
    }

    execute_query(nodes[NewMaster], "RESET SLAVE ALL");
    execute_query(nodes[NewMaster], "%s", create_repl_user);

    if (mysql_ping(nodes[OldMaster]) == 0)
    {
        execute_query(nodes[OldMaster], "RESET MASTER");
    }

    for (int i = 0; i < N; i++)
    {
        if (i != NewMaster && mysql_ping(nodes[i]) == 0)
        {
            string str = mxb::string_printf(setup_slave, new_master->ip_private(), new_master->port());
            execute_query(nodes[i], "%s", str.c_str());
        }
    }
}

void ReplicationCluster::replicate_from(int slave, int master)
{
    auto master_be = backend(master);
    replicate_from(slave, master_be->ip_private(), master_be->port());
}

void ReplicationCluster::replicate_from(int slave, const std::string& host, uint16_t port)
{
    replicate_from(slave, host, port, GtidType::CURRENT_POS, "", false);
}

void ReplicationCluster::replicate_from(int slave, const std::string& host, uint16_t port, GtidType type,
                                        const std::string& conn_name, bool reset)
{
    auto be = backend(slave);
    if (be->ping_or_open_admin_connection())
    {
        auto conn_namec = conn_name.c_str();
        auto conn = be->admin_connection();
        if (conn->cmd_f("STOP SLAVE '%s';", conn_namec))
        {
            if (reset)
            {
                conn->cmd_f("RESET SLAVE '%s' ALL;", conn_namec);
            }
            const char* gtid_str = (type == GtidType::CURRENT_POS) ? "current_pos" : "slave_pos";
            string change_master = mxb::string_printf(
                "CHANGE MASTER '%s' TO MASTER_HOST = '%s', MASTER_PORT = %i, "
                "MASTER_USER = '%s', MASTER_PASSWORD = '%s', MASTER_USE_GTID = %s;",
                conn_namec, host.c_str(), port, repl_user.c_str(), repl_pw.c_str(), gtid_str);
            conn->cmd(change_master);
            conn->cmd_f("START SLAVE '%s';", conn_namec);
        }
    }
}

const std::string& ReplicationCluster::nwconf_prefix() const
{
    return my_nwconf_prefix;
}

const std::string& ReplicationCluster::name() const
{
    return my_name;
}

std::string ReplicationCluster::get_srv_cnf_filename(int node)
{
    return mxb::string_printf("server%i.cnf", node + 1);
}

std::string ReplicationCluster::gen_change_master_cmd(MariaDBServer* master)
{
    return mxb::string_printf("change master to master_host='%s', master_port=%i, master_user='%s', "
                              "master_password='%s', master_use_gtid=slave_pos, master_delay=0;",
                              master->vm_node().priv_ip(), master->port(), "repl", "repl");
}

bool ReplicationCluster::create_users()
{
    bool rval = false;
    if (create_base_users())
    {
        auto be = backend(0);
        auto vrs = be->version();

        mxt::MariaDBUserDef mdbmon_user = {"mariadbmon", "%", "mariadbmon"};
        mdbmon_user.grants = {"SUPER, FILE, RELOAD, PROCESS, SHOW DATABASES, EVENT ON *.*",
                              "SELECT ON mysql.user"};
        auto version_num = vrs.as_number();
        if (version_num >= 10'05'00)
        {
            mdbmon_user.grants.emplace_back("REPLICATION SLAVE ADMIN ON *.*");
            mdbmon_user.grants.emplace_back("SELECT ON mysql.global_priv");
        }
        else
        {
            mdbmon_user.grants.emplace_back("REPLICATION CLIENT ON *.*");
        }

        if (vrs.as_number() >= 10'11'00)
        {
            mdbmon_user.grants.emplace_back("READ ONLY ADMIN ON *.*");
        }

        if (vrs.as_number() >= 11'00'00)
        {
            // MariaDB 11.0 no longer gives the following grants with SUPER (MDEV-29668)
            for (std::string grant : {
                    "SET USER", "FEDERATED ADMIN", "CONNECTION ADMIN", "REPLICATION SLAVE ADMIN",
                    "BINLOG ADMIN", "BINLOG REPLAY", "REPLICA MONITOR", "BINLOG MONITOR",
                    "REPLICATION MASTER ADMIN", "READ_ONLY ADMIN",
                })
            {
                mdbmon_user.grants.emplace_back(grant + " ON *.*");
            }
        }

        bool error = false;
        auto ssl = ssl_mode();
        bool sr = supports_require();
        if (!be->create_user(mdbmon_user, ssl, sr)
            || !be->create_user(service_user_def(), ssl, sr)
            || !be->admin_connection()->try_cmd("GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';"))
        {
            error = true;
        }

        if (vrs.as_number() >= 10'05'06)
        {
            if (!be->admin_connection()->try_cmd("GRANT SLAVE MONITOR ON *.* TO 'repl'@'%';"))
            {
                error = true;
            }
        }

        if (!error)
        {
            rval = true;
        }
    }

    return rval;
}

bool ReplicationCluster::sync_cluster()
{
    return sync_slaves(0, 5);
}
}
