/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * @file galera_mon.c - A MySQL Galera cluster monitor
 */

#define MXB_MODULE_NAME "galeramon"

#include "galeramon.hh"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include <mysqld_error.h>
#include <maxbase/alloc.hh>
#include <maxscale/config.hh>
#include <maxscale/dcb.hh>
#include <maxscale/modinfo.hh>
#include <maxscale/protocol/mariadb/maxscale.hh>
#include <maxscale/secrets.hh>
#include <maxsql/mariadb.hh>
#include <maxsql/mariadb_connector.hh>

#define DONOR_NODE_NAME_MAX_LEN 60
#define DONOR_LIST_SET_VAR      "SET GLOBAL wsrep_sst_donor = \""

namespace
{
const std::string grant_test_query = "SHOW STATUS LIKE 'wsrep_local_state';";

namespace cfg = mxs::config;

cfg::Specification s_spec(MXB_MODULE_NAME, cfg::Specification::MONITOR);

cfg::ParamBool s_disable_master_failback(
    &s_spec, "disable_master_failback", "Only change the master node if the current one fails",
    false, cfg::Param::AT_RUNTIME);

cfg::ParamBool s_available_when_donor(
    &s_spec, "available_when_donor", "Whether nodes are available when they are donors",
    false, cfg::Param::AT_RUNTIME);

cfg::ParamBool s_disable_master_role_setting(
    &s_spec, "disable_master_role_setting", "Don't assign Master or Slave status bits",
    false, cfg::Param::AT_RUNTIME);

cfg::ParamBool s_root_node_as_master(
    &s_spec, "root_node_as_master", "Always use node 0 as the master server",
    false, cfg::Param::AT_RUNTIME);

cfg::ParamBool s_use_priority(
    &s_spec, "use_priority", "Use server priority instead of cluster index for master selection",
    false, cfg::Param::AT_RUNTIME);

cfg::ParamBool s_set_donor_nodes(
    &s_spec, "set_donor_nodes", "Set preferred donor node list on all nodes",
    false, cfg::Param::AT_RUNTIME);
}

using maxscale::MonitorServer;

/** Log a warning when a bad 'wsrep_local_index' is found */
static bool warn_erange_on_local_index = true;

static GaleraServer* set_cluster_master(GaleraServer*, GaleraServer*, int);
static int           compare_node_index(const void*, const void*);
static int           compare_node_priority(const void*, const void*);
static bool          using_xtrabackup(GaleraServer* database, const char* server_string);

GaleraMonitor::Config::Config(const std::string& name, GaleraMonitor* monitor)
    : mxs::config::Configuration(name, &s_spec)
    , m_monitor(monitor)
{
    add_native(&Config::disable_master_failback, &s_disable_master_failback);
    add_native(&Config::available_when_donor, &s_available_when_donor);
    add_native(&Config::disable_master_role_setting, &s_disable_master_role_setting);
    add_native(&Config::root_node_as_master, &s_root_node_as_master);
    add_native(&Config::use_priority, &s_use_priority);
    add_native(&Config::set_donor_nodes, &s_set_donor_nodes);
}

bool GaleraMonitor::Config::post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params)
{
    return m_monitor->post_configure();
}

GaleraMonitor::GaleraMonitor(const std::string& name, const std::string& module)
    : SimpleMonitor(name, module)
    , m_config(name, this)
    , m_log_no_members(false)
    , m_cluster_size(0)
{
}

GaleraMonitor::~GaleraMonitor()
{
}

// static
GaleraMonitor* GaleraMonitor::create(const std::string& name, const std::string& module)
{
    return new GaleraMonitor(name, module);
}

mxs::config::Configuration& GaleraMonitor::configuration()
{
    return m_config;
}

json_t* GaleraMonitor::diagnostics() const
{
    json_t* rval = Monitor::diagnostics();
    json_object_set_new(rval, "disable_master_failback", json_boolean(m_config.disable_master_failback));
    json_object_set_new(rval, "disable_master_role_setting", json_boolean(m_config.disable_master_role_setting));
    json_object_set_new(rval, "root_node_as_master", json_boolean(m_config.root_node_as_master));
    json_object_set_new(rval, "use_priority", json_boolean(m_config.use_priority));
    json_object_set_new(rval, "set_donor_nodes", json_boolean(m_config.set_donor_nodes));

    if (!m_cluster_uuid.empty())
    {
        json_object_set_new(rval, "cluster_uuid", json_string(m_cluster_uuid.c_str()));
        json_object_set_new(rval, "cluster_size", json_integer(m_cluster_size));
    }

    json_t* arr = json_array();
    std::lock_guard<std::mutex> guard(m_lock);

    for (auto ptr : m_servers)
    {
        auto it = m_prev_info.find(ptr);

        if (it != m_prev_info.end())
        {
            json_t* obj = json_object();
            json_object_set_new(obj, "name", json_string(it->first->server->name()));
            json_object_set_new(obj, "gtid_current_pos", json_string(it->second.gtid_current_pos.c_str()));
            json_object_set_new(obj, "gtid_binlog_pos", json_string(it->second.gtid_binlog_pos.c_str()));
            json_object_set_new(obj, "read_only", json_boolean(it->second.read_only));
            json_object_set_new(obj, "server_id", json_integer(it->second.server_id));
            json_object_set_new(obj, "master_id", json_integer(it->second.master_id));
            json_array_append_new(arr, obj);
        }
    }

    json_object_set_new(rval, "server_info", arr);

    return rval;
}

json_t* GaleraMonitor::diagnostics(MonitorServer* server) const
{
    json_t* obj = json_object();

    std::lock_guard<std::mutex> guard(m_lock);
    auto it = m_prev_info.find(server);

    if (it != m_prev_info.end())
    {
        json_object_set_new(obj, "name", json_string(it->first->server->name()));
        json_object_set_new(obj, "gtid_current_pos", json_string(it->second.gtid_current_pos.c_str()));
        json_object_set_new(obj, "gtid_binlog_pos", json_string(it->second.gtid_binlog_pos.c_str()));
        json_object_set_new(obj, "read_only", json_boolean(it->second.read_only));
        json_object_set_new(obj, "server_id", json_integer(it->second.server_id));
        json_object_set_new(obj, "master_id", json_integer(it->second.master_id));

        std::vector<std::string> states;
        const auto& comment = it->second.comment;

        if (!comment.empty() && comment != "Synced")
        {
            // The Synced state is still functional in 6 as readconnroute has it as one of the values for
            // `router_options`. This should be changed so that the Running state is only assigned for Galera
            // nodes that can actually be used for routing.
            states.push_back(comment);
        }

        if (m_config.disable_master_failback && server->server->is_master() && it->second.local_index != 0)
        {
            states.push_back("Master Stickiness");
        }

        if (!states.empty())
        {
            json_object_set_new(obj, "state_details", json_string(mxb::join(states, ", ").c_str()));
        }
    }

    return obj;
}

bool GaleraMonitor::post_configure()
{
    m_info.clear();
    return true;
}

std::string GaleraMonitor::annotate_state_change(mxs::MonitorServer* server)
{
    std::ostringstream ss;
    auto prev = m_prev_info.find(server);
    auto next = m_info.find(server);

    if (prev != m_prev_info.end() && next != m_info.end() && server->server->is_running())
    {
        if (prev->second.local_state != next->second.local_state)
        {
            ss << "local_state: " << prev->second.local_state << " -> " << next->second.local_state << " ";
        }

        if (prev->second.local_index != next->second.local_index)
        {
            ss << "local_index: " << prev->second.local_index << " -> " << next->second.local_index << " ";
        }

        if (prev->second.server_id != next->second.server_id)
        {
            ss << "server_id: " << prev->second.server_id << " -> " << next->second.server_id << " ";
        }

        if (prev->second.joined != next->second.joined)
        {
            ss << "joined: " << prev->second.joined << " -> " << next->second.joined << " ";
        }

        if (prev->second.cluster_size != next->second.cluster_size)
        {
            ss << "cluster_size: " << prev->second.cluster_size << " -> " << next->second.cluster_size << " ";
        }

        if (prev->second.cluster_uuid != next->second.cluster_uuid)
        {
            ss << "cluster_uuid: '" << prev->second.cluster_uuid << "' -> '" << next->second.cluster_uuid << "' ";
        }

        if (prev->second.comment != next->second.comment)
        {
            ss << "state_comment: '" << prev->second.comment << "' -> '" << next->second.comment << "' ";
        }
    }

    return ss.str();
}

void get_gtid(GaleraServer* srv, GaleraNode* info)
{
    if (mxs_mysql_query(srv->con,
                        "SELECT @@gtid_current_pos, @@gtid_binlog_pos, @@read_only, @@server_id") == 0)
    {
        if (auto result = mysql_store_result(srv->con))
        {
            mxq::MariaDBQueryResult res(result);

            if (res.next_row())
            {
                info->gtid_current_pos = res.get_string(0);
                info->gtid_binlog_pos = res.get_string(1);
                info->read_only = res.get_bool(2);
                info->server_id = res.get_int(3);

                // The gtid_current_pos is not reliably updated in all cases (MDEV-26176). To make the MaxCtrl
                // output consistent, substitute it with gtid_binlog_pos if it's found.
                if (!info->gtid_binlog_pos.empty() && info->gtid_current_pos.empty())
                {
                    info->gtid_current_pos = info->gtid_binlog_pos;
                }
            }
        }
    }
}

void get_slave_status(GaleraServer* srv, GaleraNode* info)
{
    if (mxs_mysql_query(srv->con, "SHOW SLAVE STATUS") == 0)
    {
        if (auto result = mysql_store_result(srv->con))
        {
            mxq::MariaDBQueryResult res(result);

            if (res.next_row() && res.get_string("Slave_SQL_Running") == "Yes")
            {
                info->master_id = res.get_int("Master_Server_Id");
                srv->server->set_replication_lag(res.get_int("Seconds_Behind_Master"));
            }
        }
    }
}

void GaleraMonitor::update_server_status(MonitorServer* mon_server)
{
    auto monitored_server = static_cast<GaleraServer*>(mon_server);
    MYSQL_ROW row;
    MYSQL_RES* result;

    std::string server_string = monitored_server->server->info().version_string();

    /* Check if the the Galera FSM shows this node is joined to the cluster */
    const char* where =
        " WHERE Variable_name IN"
        " ('wsrep_cluster_state_uuid',"
        " 'wsrep_cluster_size',"
        " 'wsrep_local_index',"
        " 'wsrep_local_state',"
        " 'wsrep_local_state_comment',"
        " 'wsrep_desync',"
        " 'wsrep_ready',"
        " 'wsrep_sst_donor_rejects_queries',"
        " 'wsrep_reject_queries')";

    GaleraNode info = {};

    for (std::string cluster_member : {"SHOW STATUS", "SHOW VARIABLES"})
    {
        cluster_member += where;

        if (mxs_mysql_query(monitored_server->con, cluster_member.c_str()) == 0
            && (result = mysql_store_result(monitored_server->con)) != NULL)
        {
            if (mysql_field_count(monitored_server->con) < 2)
            {
                mysql_free_result(result);
                MXB_ERROR("Unexpected result for \"%s\". "
                          "Expected 2 columns. MySQL Version: %s",
                          cluster_member.c_str(),
                          server_string.c_str());
                return;
            }
            while ((row = mysql_fetch_row(result)))
            {
                if (strcasecmp(row[0], "wsrep_cluster_size") == 0)
                {
                    info.cluster_size = atoi(row[1]);
                }

                if (strcasecmp(row[0], "wsrep_local_index") == 0)
                {
                    char* endchar;
                    long local_index = strtol(row[1], &endchar, 10);
                    if (*endchar != '\0'
                        || (errno == ERANGE && (local_index == LONG_MAX || local_index == LONG_MIN)))
                    {
                        if (warn_erange_on_local_index)
                        {
                            MXB_WARNING("Invalid 'wsrep_local_index' on server '%s': %s",
                                        monitored_server->server->name(),
                                        row[1]);
                            warn_erange_on_local_index = false;
                        }
                        local_index = -1;
                        /* Force joined = 0 */
                        info.joined = 0;
                    }

                    info.local_index = local_index;
                }

                mxb_assert(row[0] && row[1]);

                if (strcasecmp(row[0], "wsrep_local_state") == 0)
                {
                    if (strcmp(row[1], "4") == 0)
                    {
                        info.joined = 1;
                    }
                    /* Check if the node is a donor and is using xtrabackup, in this case it can stay alive */
                    else if (strcmp(row[1], "2") == 0 && m_config.available_when_donor == 1
                             && using_xtrabackup(monitored_server, server_string.c_str()))
                    {
                        info.joined = 1;
                    }
                    else
                    {
                        /* Force joined = 0 */
                        info.joined = 0;
                    }

                    info.local_state = atoi(row[1]);
                }

                if (strcasecmp(row[0], "wsrep_local_state_comment") == 0)
                {
                    info.comment = row[1];

                    // The comment sometimes contains extra information. Leave that out as we're only
                    // interested in the string form of the local state.
                    auto pos = info.comment.find(':');

                    if (pos != std::string::npos)
                    {
                        info.comment.erase(pos);
                    }
                }

                /* Node is in desync - lets take it offline */
                if (strcasecmp(row[0], "wsrep_desync") == 0)
                {
                    if (config_truth_value(row[1]))
                    {
                        info.joined = 0;
                    }
                }

                /* Node rejects queries - lets take it offline */
                if (strcasecmp(row[0], "wsrep_reject_queries") == 0)
                {
                    if (strcasecmp(row[1], "ALL") == 0 || strcasecmp(row[1], "ALL_KILL") == 0)
                    {
                        info.joined = 0;
                    }
                }

                /* Node rejects queries - lets take it offline */
                if (strcasecmp(row[0], "wsrep_sst_donor_rejects_queries") == 0)
                {
                    if (config_truth_value(row[1]))
                    {
                        info.joined = 0;
                    }
                }

                /* Node is not ready - lets take it offline */
                if (strcasecmp(row[0], "wsrep_ready") == 0)
                {
                    if (!config_truth_value(row[1]))
                    {
                        info.joined = 0;
                    }
                }

                if (strcasecmp(row[0], "wsrep_cluster_state_uuid") == 0 && row[1] && *row[1])
                {
                    info.cluster_uuid = row[1];
                }
            }

            mysql_free_result(result);
        }
        else
        {
            monitored_server->report_query_error();
            return;
        }
    }

    get_gtid(monitored_server, &info);
    get_slave_status(monitored_server, &info);
    monitored_server->node_id = info.joined ? info.local_index : -1;

    m_info[monitored_server] = info;

    calculate_cluster();
}

void GaleraMonitor::calculate_cluster()
{
    std::unordered_map<std::string, int> clusters;

    for (const auto& a : m_info)
    {
        clusters[a.second.cluster_uuid]++;
    }

    auto it = std::max_element(
        clusters.begin(), clusters.end(),
        [](const typename decltype(clusters)::value_type& a,
           const typename decltype(clusters)::value_type& b) {
            return a.second == b.second ? a.first < b.first : a.second < b.second;
        });

    if (it != clusters.end())
    {
        m_cluster_uuid = it->first;
        m_cluster_size = it->second;
    }
}

void GaleraMonitor::pre_tick()
{
    // Store the info of the previous tick in case it's used for diagnostics
    std::lock_guard<std::mutex> guard(m_lock);
    m_prev_info = std::move(m_info);
    m_info.clear();
}

void GaleraMonitor::post_tick()
{
    int is_cluster = 0;

    /* Try to set a Galera cluster based on
     * UUID and cluster_size each node reports:
     * no multiple clusters UUID are allowed.
     */
    set_galera_cluster();

    /*
     * Let's select a master server:
     * it could be the candidate master following MXS_MIN(node_id) rule or
     * the server that was master in the previous monitor polling cycle
     * Decision depends on master_stickiness value set in configuration
     */

    /* get the candidate master, following MXS_MIN(node_id) rule */
    auto* candidate_master = get_candidate_master();

    m_master = set_cluster_master(m_master, candidate_master, m_config.disable_master_failback);

    for (auto ptr : m_servers)
    {
        // Although there's some replication lag in Galera, this isn't currently measured and having it be 0
        // seconds is better than having it as undefined. Otherwise, using max_slave_replication_lag in
        // readwritesplit causes the whole cluster to become unavailable.
        ptr->server->set_replication_lag(0);

        const int repl_bits = (SERVER_SLAVE | SERVER_MASTER);
        if (ptr->has_status(SERVER_JOINED) && !m_config.disable_master_role_setting)
        {
            if (ptr != m_master)
            {
                /* set the Slave role and clear master stickiness */
                ptr->clear_pending_status(repl_bits);
                ptr->set_pending_status(SERVER_SLAVE);
            }
            else
            {
                if (candidate_master
                    && m_master->node_id != candidate_master->node_id)
                {
                    /* set master role and master stickiness */
                    ptr->clear_pending_status(repl_bits);
                    ptr->set_pending_status(SERVER_MASTER);
                }
                else
                {
                    /* set master role and clear master stickiness */
                    ptr->clear_pending_status(repl_bits);
                    ptr->set_pending_status(SERVER_MASTER);
                }
            }

            is_cluster++;
        }
        else if (int master_id = m_info[ptr].master_id)
        {
            ptr->clear_pending_status(repl_bits);

            if (std::any_of(m_info.begin(), m_info.end(), [master_id](decltype(m_info)::const_reference r) {
                                return r.first->has_status(SERVER_JOINED)
                                       && r.second.server_id == master_id;
                            }))
            {
                ptr->set_pending_status(SERVER_SLAVE);
            }
        }
        else
        {
            ptr->clear_pending_status(repl_bits);
            ptr->set_pending_status(0);
        }
    }

    if (is_cluster == 0 && m_log_no_members)
    {
        MXB_ERROR("There are no cluster members");
        m_log_no_members = false;
    }
    else
    {
        if (is_cluster > 0 && m_log_no_members == 0)
        {
            MXB_NOTICE("Found cluster members");
            m_log_no_members = true;
        }
    }

    /* Set the global var "wsrep_sst_donor"
     * with a sorted list of "wsrep_node_name" for slave nodes
     */
    if (m_config.set_donor_nodes)
    {
        update_sst_donor_nodes(is_cluster);
    }
}

static bool using_xtrabackup(GaleraServer* database, const char* server_string)
{
    bool rval = false;
    MYSQL_RES* result;

    if (mxs_mysql_query(database->con, "SHOW VARIABLES LIKE 'wsrep_sst_method'") == 0
        && (result = mysql_store_result(database->con)) != NULL)
    {
        if (mysql_field_count(database->con) < 2)
        {
            mysql_free_result(result);
            MXB_ERROR("Unexpected result for \"SHOW VARIABLES LIKE "
                      "'wsrep_sst_method'\". Expected 2 columns."
                      " MySQL Version: %s",
                      server_string);
            return false;
        }

        MYSQL_ROW row;

        while ((row = mysql_fetch_row(result)))
        {
            if (row[1] && (strcmp(row[1], "xtrabackup") == 0
                           || strcmp(row[1], "mariabackup") == 0
                           || strcmp(row[1], "xtrabackup-v2") == 0))
            {
                rval = true;
            }
        }
        mysql_free_result(result);
    }
    else
    {
        database->report_query_error();
    }

    return rval;
}

/**
 * get candidate master from all nodes
 *
 * The current available rule: get the server with MXS_MIN(node_id)
 * node_id comes from 'wsrep_local_index' variable
 *
 * @param   servers The monitored servers list
 * @return  The candidate master on success, NULL on failure
 */
GaleraServer* GaleraMonitor::get_candidate_master()
{
    GaleraServer* candidate_master = NULL;
    long min_id = -1;
    int minval = INT_MAX;

    /* set min_id to the lowest value of moitor_servers->server->node_id */
    for (auto moitor_servers : m_servers)
    {
        if (!moitor_servers->server->is_in_maint()
            && (moitor_servers->has_status(SERVER_JOINED)))
        {
            int64_t priority = moitor_servers->server->priority();

            if (m_config.use_priority && priority != 0)
            {
                /** The priority is valid */
                if (priority > 0 && priority < minval)
                {
                    minval = priority;
                    candidate_master = moitor_servers;
                }
            }
            else if (moitor_servers->node_id >= 0)
            {
                if (m_config.use_priority && candidate_master
                    && candidate_master->server->priority() > 0)
                {
                    // Current candidate has priority but this node doesn't, current candidate is better
                    continue;
                }

                // Server priorities are not in use or no candidate has been found
                if (min_id < 0 || moitor_servers->node_id < min_id)
                {
                    min_id = moitor_servers->node_id;
                    candidate_master = moitor_servers;
                }
            }
        }
    }

    if (!m_config.use_priority && !m_config.disable_master_failback
        && m_config.root_node_as_master && min_id > 0)
    {
        /** The monitor couldn't find the node with wsrep_local_index of 0.
         * This means that we can't connect to the root node of the cluster.
         *
         * If the node is down, the cluster would recalculate the index values
         * and we would find it. In this case, we just can't connect to it.
         */

        candidate_master = NULL;
    }

    return candidate_master;
}

/**
 * set the master server in the cluster
 *
 * master could be the last one from previous monitor cycle Iis running) or
 * the candidate master.
 * The selection is based on the configuration option mapped to master_stickiness
 * The candidate master may change over time due to
 * 'wsrep_local_index' value change in the Galera Cluster
 * Enabling master_stickiness will avoid master change unless a failure is spotted
 *
 * @param   current_master Previous master server
 * @param   candidate_master The candidate master server accordingly to the selection rule
 * @return  The  master node pointer (could be NULL)
 */
static GaleraServer* set_cluster_master(GaleraServer* current_master, GaleraServer* candidate_master,
                                        int master_stickiness)
{
    /*
     * if current master is not set or master_stickiness is not enable
     * just return candidate_master.
     */
    if (current_master == NULL || master_stickiness == 0)
    {
        return candidate_master;
    }
    else
    {
        // If current_master is still a cluster member use it
        if ((current_master->has_status(SERVER_JOINED))
            && (!current_master->server->is_in_maint()))
        {
            return current_master;
        }
        else
        {
            return candidate_master;
        }
    }
}

/**
 * Set the global variable wsrep_sst_donor in the cluster
 *
 * The monitor user must have the privileges for setting global vars.
 *
 * Galera monitor fetches from each joined slave node the var 'wsrep_node_name'
 * A list of nodes is automatically build and it's sorted by wsrep_local_index DESC
 * or by priority ASC if use_priority option is set.
 *
 * The list is then added to SET GLOBAL VARIABLE wrep_sst_donor =
 * The variable must be sent to all slave nodes.
 *
 * All slave nodes have a sorted list of nodes tht can be used as donor nodes.
 *
 * If there is only one node the funcion returns,
 *
 * @param   mon        The monitor handler
 * @param   is_cluster The number of joined nodes
 */
void GaleraMonitor::update_sst_donor_nodes(int is_cluster)
{
    MYSQL_ROW row;
    MYSQL_RES* result;
    bool ignore_priority = true;

    if (is_cluster == 1)
    {
        return;     // Only one server in the cluster: update_sst_donor_nodes is not performed
    }

    unsigned int found_slaves = 0;
    GaleraServer* node_list[is_cluster - 1];
    /* Donor list size = DONOR_LIST_SET_VAR + n_hosts * max_host_len + n_hosts + 1 */

    char* donor_list = static_cast<char*>(MXB_CALLOC(1,
                                                     strlen(DONOR_LIST_SET_VAR)
                                                     + is_cluster * DONOR_NODE_NAME_MAX_LEN
                                                     + is_cluster + 1));

    if (donor_list == NULL)
    {
        MXB_ERROR("can't execute update_sst_donor_nodes() due to memory allocation error");
        return;
    }

    strcpy(donor_list, DONOR_LIST_SET_VAR);

    /* Create an array of slave nodes */
    for (auto ptr : m_servers)
    {
        if ((ptr->has_status(SERVER_JOINED | SERVER_SLAVE)))
        {
            node_list[found_slaves] = ptr;
            found_slaves++;

            /* Check the server parameter "priority"
             * If no server has "priority" set, then
             * the server list will be order by default method.
             */

            if (m_config.use_priority && ptr->server->priority() > 0)
            {
                ignore_priority = false;
            }
        }
    }

    /* Set order type */
    bool sort_order = !ignore_priority && m_config.use_priority;

    /* Sort the array */
    qsort(node_list,
          found_slaves,
          sizeof(GaleraServer*),
          sort_order ? compare_node_priority : compare_node_index);

    /* Select nodename from each server and append it to node_list */
    for (unsigned int k = 0; k < found_slaves; k++)
    {
        auto* ptr = node_list[k];

        /* Get the Galera node name */
        if (mxs_mysql_query(ptr->con, "SHOW VARIABLES LIKE 'wsrep_node_name'") == 0
            && (result = mysql_store_result(ptr->con)) != NULL)
        {
            if (mysql_field_count(ptr->con) == 2)
            {
                while ((row = mysql_fetch_row(result)))
                {
                    strncat(donor_list, row[1], DONOR_NODE_NAME_MAX_LEN);
                    strcat(donor_list, ",");
                }
            }
            else
            {
                MXB_ERROR("Unexpected result for \"SHOW VARIABLES LIKE 'wsrep_node_name'\". "
                          "Expected 2 columns");
            }

            mysql_free_result(result);
        }
        else
        {
            ptr->report_query_error();
        }
    }

    int donor_list_size = strlen(donor_list);
    if (donor_list[donor_list_size - 1] == ',')
    {
        donor_list[donor_list_size - 1] = '\0';
    }

    strcat(donor_list, "\"");

    /* Set now rep_sst_donor in each slave node */
    for (unsigned int k = 0; k < found_slaves; k++)
    {
        auto* ptr = node_list[k];
        if (mxs_mysql_query(ptr->con, donor_list) != 0)
        {
            ptr->report_query_error();
        }
    }

    MXB_FREE(donor_list);
}

/**
 * Compare routine for slave nodes sorted by 'wsrep_local_index'
 *
 * The default order is DESC.
 *
 * Nodes with lowest 'wsrep_local_index' value
 * are at the end of the list.
 *
 * @param   a        Pointer to array value
 * @param   b        Pointer to array value
 * @return  A number less than, threater than or equal to 0
 */

static int compare_node_index(const void* a, const void* b)
{
    const MonitorServer* s_a = *(MonitorServer* const*)a;
    const MonitorServer* s_b = *(MonitorServer* const*)b;

    // Order is DESC: b - a
    return s_b->node_id - s_a->node_id;
}

/**
 * Compare routine for slave nodes sorted by node priority
 *
 * The default order is DESC.
 *
 * Some special cases, i.e: no give priority, or 0 value
 * are handled.
 *
 * Note: the master selection algorithm is:
 * node with lowest priority value and > 0
 *
 * This sorting function will add master candidates
 * at the end of the list.
 *
 * @param   a        Pointer to array value
 * @param   b        Pointer to array value
 * @return  A number less than, threater than or equal to 0
 */

static int compare_node_priority(const void* a, const void* b)
{
    auto s_a = *(GaleraServer* const*)a;
    auto s_b = *(GaleraServer* const*)b;
    int pri_val_a = s_a->server->priority();
    int pri_val_b = s_b->server->priority();
    bool have_a = pri_val_a > 0;
    bool have_b = pri_val_b > 0;

    /**
     * Check priority parameter:
     *
     * Return a - b in case of issues
     */
    if (!have_a && have_b)
    {
        return -(INT_MAX - 1);
    }
    else if (have_a && !have_b)
    {
        return INT_MAX - 1;
    }
    else if (!have_a && !have_b)
    {
        return 0;
    }

    /* Return a - b in case of issues */
    if ((pri_val_a < INT_MAX && pri_val_a > 0) && !(pri_val_b < INT_MAX && pri_val_b > 0))
    {
        return pri_val_a;
    }
    else if (!(pri_val_a < INT_MAX && pri_val_a > 0) && (pri_val_b < INT_MAX && pri_val_b > 0))
    {
        return -pri_val_b;
    }
    else if (!(pri_val_a < INT_MAX && pri_val_a > 0) && !(pri_val_b < INT_MAX && pri_val_b > 0))
    {
        return 0;
    }

    // The order is DESC: b -a
    return pri_val_b - pri_val_a;
}

/**
 * Only set the servers as joined if they are a part of the largest cluster
 */
void GaleraMonitor::set_galera_cluster()
{
    for (auto it = m_info.begin(); it != m_info.end(); it++)
    {
        if (it->second.joined && it->second.cluster_uuid == m_cluster_uuid)
        {
            it->first->set_pending_status(SERVER_JOINED);
        }
        else
        {
            it->first->clear_pending_status(SERVER_JOINED);
        }
    }
}

bool GaleraMonitor::can_be_disabled(const MonitorServer& server, DisableType type,
                                    std::string* errmsg_out) const
{
    // If the server is the master, it cannot be drained. It can be set to maintenance, though.
    bool rval = true;
    if (type == DisableType::DRAIN && status_is_master(server.server->status()))
    {
        rval = false,
        *errmsg_out = "The server is master, so it cannot be set to draining mode.";
    }
    return rval;
}

std::string GaleraMonitor::permission_test_query() const
{
    return "SHOW STATUS LIKE 'wsrep_local_state'";
}

void GaleraMonitor::configured_servers_updated(const std::vector<SERVER*>& servers)
{
    for (auto srv : m_servers)
    {
        delete srv;
    }

    auto& shared_settings = settings().shared;
    m_servers.resize(servers.size());
    for (size_t i = 0; i < servers.size(); i++)
    {
        m_servers[i] = new GaleraServer(servers[i], shared_settings);
    }

    // The configured servers and the active servers are the same.
    set_active_servers(std::vector<MonitorServer*>(m_servers.begin(), m_servers.end()));
}

void GaleraMonitor::pre_loop()
{
    m_master = nullptr;
    SimpleMonitor::pre_loop();
}

GaleraServer::GaleraServer(SERVER* server, const MonitorServer::SharedSettings& shared)
    : MariaServer(server, shared)
{
}

void GaleraServer::report_query_error()
{
    MXB_ERROR("Failed to execute query on server '%s' ([%s]:%d): %s",
              server->name(), server->address(), server->port(), mysql_error(con));
}

const std::string& GaleraServer::permission_test_query() const
{
    return grant_test_query;
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::MONITOR,
        mxs::ModuleStatus::GA,
        MXS_MONITOR_VERSION,
        "A Galera cluster monitor",
        "V2.0.0",
        MXS_NO_MODULE_CAPABILITIES,
        &maxscale::MonitorApi<GaleraMonitor>::s_api,
        NULL,
        NULL,
        NULL,
        NULL,
        &s_spec
    };

    return &info;
}
