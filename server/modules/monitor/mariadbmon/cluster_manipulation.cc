/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2029-02-28
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "mariadbmon.hh"
#include <memory>
#include <set>
#include <cinttypes>
#include <mysql.h>
#include <maxbase/stopwatch.hh>
#include <maxbase/format.hh>
#include <maxscale/clock.hh>
#include <maxscale/protocol/mariadb/maxscale.hh>

using std::string;
using std::move;
using std::unique_ptr;
using maxbase::string_printf;
using maxbase::StopWatch;
using maxbase::Duration;
using GtidMode = SlaveStatus::Settings::GtidMode;
using LockType = MariaDBServer::LockType;
using namespace std::chrono_literals;

namespace
{
void print_no_locks_error(mxb::Json& error_out)
{
    const char locks_taken[] =
        "Cannot perform cluster operation because this MaxScale does not have exclusive locks "
        "on a majority of servers. Run \"SELECT IS_USED_LOCK('%s');\" on the servers to find out "
        "which connection id has a lock.";
    auto err_msg = string_printf(locks_taken, SERVER_LOCK_NAME);
    PRINT_JSON_ERROR(error_out, "%s", err_msg.c_str());
}

void fix_gtid_mode(GtidMode& gtid_mode)
{
    if (gtid_mode == GtidMode::NONE)
    {
        // Usually getting here is unlikely if not impossible as slaves without gtid are not valid for
        // monitor operations. Cannot be 100% sure though, as dba could disable gtid-mode just before
        // a monitor operation. In any case, forcing Current_Pos matches previous version behavior.
        gtid_mode = GtidMode::CURRENT;
    }
}

const char NO_SERVER[] = "Server '%s' is not monitored by '%s'.";
const char FAILOVER_OK[] = "Failover '%s' -> '%s' performed.";
const char FAILOVER_FAIL[] = "Failover '%s' -> '%s' failed.";
const char SWITCHOVER_OK[] = "Switchover '%s' -> '%s' performed.";
const char SWITCHOVER_FAIL[] = "Switchover %s -> %s failed.";
}


/**
 * Run a manual switchover, promoting a new master server and demoting the existing master.
 *
 * @param new_master The server which should be promoted. If null, monitor will autoselect.
 * @param current_master The server which should be demoted. Can be null for autoselect, in which case
 * monitor will select the cluster master server. Otherwise must be a valid master server or a relay.
 * @return Result structure
 */
mon_op::Result MariaDBMonitor::manual_switchover(SwitchoverType type, SERVER* new_master,
                                                 SERVER* current_master)
{
    // Manual commands should only run in the main monitor thread.
    mxb_assert(mxb::Worker::get_current()->id() == m_worker->id());
    mxb_assert(m_op_info.exec_state == mon_op::ExecState::RUNNING);

    mon_op::Result rval;
    auto& output = rval.output;
    if (!lock_status_is_ok())
    {
        print_no_locks_error(output);
        return rval;
    }

    bool switchover_done = false;
    auto op = switchover_prepare(type, new_master, current_master, Log::ON, OpStart::MANUAL, output);
    if (op)
    {
        switchover_done = switchover_perform(*op);
        if (switchover_done)
        {
            MXB_NOTICE(SWITCHOVER_OK, op->demotion.target->name(), op->promotion.target->name());
        }
        else
        {
            string msg = string_printf(SWITCHOVER_FAIL,
                                       op->demotion.target->name(), op->promotion.target->name());
            PRINT_JSON_ERROR(output, "%s", msg.c_str());
            delay_auto_cluster_ops();
        }
    }
    else
    {
        PRINT_JSON_ERROR(output, "Switchover cancelled.");
    }
    rval.success = switchover_done;
    return rval;
}

mon_op::Result MariaDBMonitor::manual_failover(FailoverType fo_type)
{
    // Manual commands should only run in the main monitor thread.
    mxb_assert(mxb::Worker::get_current()->id() == m_worker->id());
    mxb_assert(m_op_info.exec_state == mon_op::ExecState::RUNNING);

    mon_op::Result rval;
    auto& output = rval.output;
    if (!lock_status_is_ok())
    {
        print_no_locks_error(output);
        return rval;
    }

    bool failover_done = false;
    auto op = failover_prepare(fo_type, Log::ON, OpStart::MANUAL, output);
    if (op)
    {
        failover_done = failover_perform(*op);
        if (failover_done)
        {
            MXB_NOTICE(FAILOVER_OK, op->demotion_target->name(), op->promotion.target->name());
        }
        else
        {
            PRINT_JSON_ERROR(output, FAILOVER_FAIL, op->demotion_target->name(),
                             op->promotion.target->name());
        }
    }
    else
    {
        PRINT_JSON_ERROR(output, "Failover cancelled.");
    }
    rval.success = failover_done;
    return rval;
}

mon_op::Result MariaDBMonitor::manual_rejoin(SERVER* rejoin_cand_srv)
{
    // Manual commands should only run in the main monitor thread.
    mxb_assert(mxb::Worker::get_current()->id() == m_worker->id());
    mxb_assert(m_op_info.exec_state == mon_op::ExecState::RUNNING);

    mon_op::Result rval;
    auto& output = rval.output;
    if (!lock_status_is_ok())
    {
        print_no_locks_error(output);
        return rval;
    }

    maxbase::Duration time_limit(m_settings.shared.switchover_timeout);
    GeneralOpData op(OpStart::MANUAL, output, time_limit);

    bool rejoin_done = false;
    if (cluster_can_be_joined())
    {
        MariaDBServer* rejoin_cand = get_server(rejoin_cand_srv);
        if (rejoin_cand)
        {
            if (server_is_rejoin_suspect(op, rejoin_cand))
            {
                string gtid_update_error;
                if (m_master->update_gtids(&gtid_update_error))
                {
                    // The manual version of rejoin does not need to be as careful as the automatic one.
                    // The rules are mostly the same, the only difference is that a server with empty gtid:s
                    // can be rejoined manually.
                    // TODO: Add the warning to JSON output.
                    string no_rejoin_reason;
                    bool safe_rejoin = rejoin_cand->can_replicate_from(m_master, &no_rejoin_reason);
                    bool empty_gtid = rejoin_cand->m_gtid_current_pos.empty();
                    bool rejoin_allowed = false;
                    if (safe_rejoin)
                    {
                        rejoin_allowed = true;
                    }
                    else
                    {
                        if (empty_gtid)
                        {
                            rejoin_allowed = true;
                            MXB_WARNING("gtid_curren_pos of '%s' is empty. Manual rejoin is unsafe "
                                        "but allowed.", rejoin_cand->name());
                        }
                        else
                        {
                            PRINT_JSON_ERROR(output, "'%s' cannot replicate from primary server '%s': %s",
                                             rejoin_cand->name(), m_master->name(),
                                             no_rejoin_reason.c_str());
                        }
                    }

                    if (rejoin_allowed)
                    {
                        ServerArray joinable_server = {rejoin_cand};
                        if (do_rejoin(op, joinable_server) == 1)
                        {
                            rejoin_done = true;
                            MXB_NOTICE("Rejoin performed.");
                        }
                        else
                        {
                            PRINT_JSON_ERROR(output, "Rejoin attempted but failed.");
                        }
                    }
                }
                else
                {
                    PRINT_JSON_ERROR(output, "The GTIDs of primary server '%s' could not be updated: %s",
                                     m_master->name(), gtid_update_error.c_str());
                }
            }   // server_is_rejoin_suspect has added any error messages to the output, no need to print here
        }
        else
        {
            PRINT_JSON_ERROR(output, "%s is not monitored by %s, cannot rejoin.",
                             rejoin_cand_srv->name(), name());
        }
    }
    else
    {
        const char BAD_CLUSTER[] = "The server cluster of monitor %s is not in a valid state for joining. "
                                   "Either it has no primary or its gtid domain is unknown.";
        PRINT_JSON_ERROR(output, BAD_CLUSTER, name());
    }
    rval.success = rejoin_done;
    return rval;
}

/**
 * Reset replication of the cluster. Removes all slave connections and deletes binlogs. Then resets the
 * gtid sequence of the cluster to 0 and directs all servers to replicate from the given master.
 *
 * @param master_server Server to promote to master. If null, autoselect.
 * @return Result structure
 */
mon_op::Result MariaDBMonitor::manual_reset_replication(SERVER* master_server)
{
    // This command is a last-resort type, so no need to be that careful. Users are only supposed to run this
    // when replication is broken and they know the cluster is in sync.

    // Manual commands should only run in the main monitor thread.
    mxb_assert(mxb::Worker::get_current()->id() == m_worker->id());
    mxb_assert(m_op_info.exec_state == mon_op::ExecState::RUNNING);

    mon_op::Result rval;
    auto& error_out = rval.output;
    if (!lock_status_is_ok())
    {
        print_no_locks_error(error_out);
        return rval;
    }

    MariaDBServer* new_master = nullptr;
    if (master_server)
    {
        MariaDBServer* new_master_cand = get_server(master_server);
        if (new_master_cand == nullptr)
        {
            PRINT_JSON_ERROR(error_out, NO_SERVER, master_server->name(), name());
        }
        else if (!new_master_cand->is_usable())
        {
            PRINT_JSON_ERROR(error_out, "Server '%s' is down or in maintenance and cannot be used as primary.",
                             new_master_cand->name());
        }
        else
        {
            new_master = new_master_cand;
        }
    }
    else
    {
        const char BAD_MASTER[] = "Could not autoselect new master for replication reset because %s";
        if (m_master == nullptr)
        {
            PRINT_JSON_ERROR(error_out, BAD_MASTER, "the cluster has no primary.");
        }
        else if (!m_master->is_usable())
        {
            PRINT_JSON_ERROR(error_out, BAD_MASTER, "the primary is down or in maintenance.");
        }
        else
        {
            new_master = m_master;
        }
    }

    m_state = State::RESET_REPLICATION;
    // Also record the previous master, needed for scheduled events.
    MariaDBServer* old_master = (m_master && m_master->is_master()) ? m_master : nullptr;

    bool success = false;
    if (new_master)
    {
        bool error = false;
        // Step 1: Gather the list of affected servers. If any operation on the servers fails,
        // the reset fails as well.
        ServerArray targets;
        for (MariaDBServer* server : m_servers)
        {
            if (server->is_usable())
            {
                targets.push_back(server);
            }
        }

        // reset-replication has no specific timeout setting as it's a manual operation. Base the guess
        // on switchover_timeout.
        maybe_set_wait_timeout_all_servers(targets.size() * m_settings.shared.switchover_timeout);

        // The 'targets'-array cannot be empty, at least 'new_master' is there.
        MXB_NOTICE("Reseting replication on the following servers: %s. '%s' will be the new primary.",
                   monitored_servers_to_string(targets).c_str(), new_master->name());

        // Helper function for running a command on all servers in the list.
        auto exec_cmd_on_array = [&error](const ServerArray& tgts, const string& query,
                                          mxb::Json& err_out) {
            if (!error)
            {
                for (MariaDBServer* server : tgts)
                {
                    string error_msg;
                    if (!server->execute_cmd(query, &error_msg))
                    {
                        error = true;
                        PRINT_JSON_ERROR(err_out, "%s", error_msg.c_str());
                        break;
                    }
                }
            }
        };

        // Step 2: Stop and reset all slave connections, even external ones.
        for (MariaDBServer* server : targets)
        {
            if (!server->reset_all_slave_conns(error_out))
            {
                error = true;
                break;
            }
        }

        // In theory, this is wrong if there are no slaves. Cluster is modified soon anyway.
        m_cluster_modified = true;

        // Step 3: Set read_only and disable events.
        exec_cmd_on_array(targets, "SET GLOBAL read_only=1;", error_out);
        if (!error)
        {
            MXB_NOTICE("read_only set on affected servers.");
            if (m_settings.shared.handle_event_scheduler)
            {
                for (MariaDBServer* server : targets)
                {
                    if (!server->disable_events(MariaDBServer::BinlogMode::BINLOG_OFF, error_out))
                    {
                        error = true;
                        break;
                    }
                }
            }
        }

        // Step 4: delete binary logs.
        exec_cmd_on_array(targets, "RESET MASTER;", error_out);
        if (!error)
        {
            MXB_NOTICE("Binary logs deleted (RESET MASTER) on affected servers.");
        }

        // Step 5: Set gtid_slave_pos on all servers. This is also sets gtid_current_pos since binary logs
        // have been deleted.
        if (!error)
        {
            string slave_pos = string_printf("%" PRIi64 "-%" PRIi64 "-0",
                                             new_master->m_gtid_domain_id, new_master->m_server_id);

            string set_slave_pos = string_printf("SET GLOBAL gtid_slave_pos='%s';", slave_pos.c_str());
            exec_cmd_on_array(targets, set_slave_pos, error_out);
            if (!error)
            {
                MXB_NOTICE("gtid_slave_pos set to '%s' on affected servers.", slave_pos.c_str());
            }
        }

        if (!error)
        {
            // Step 6: Enable writing and events on new master, add gtid event.
            string error_msg;
            if (new_master->execute_cmd("SET GLOBAL read_only=0;", &error_msg))
            {
                // Point of no return, perform later steps even if an error occurs.
                m_next_master = new_master;

                if (m_settings.shared.handle_event_scheduler)
                {
                    if (old_master)
                    {
                        if (!new_master->enable_events(MariaDBServer::BinlogMode::BINLOG_ON,
                                                       old_master->m_enabled_events, error_out))
                        {
                            error = true;
                            PRINT_JSON_ERROR(error_out, "Could not enable events on '%s': %s",
                                             new_master->name(), error_msg.c_str());
                        }
                    }
                    else
                    {
                        MXB_WARNING("No scheduled events were enabled on '%s' because previous primary is "
                                    "unknown. Check events manually.", new_master->name());
                    }
                }

                // Add an event to the new master so that it has a non-empty gtid_current_pos.
                if (!new_master->execute_cmd("FLUSH TABLES;", &error_msg))
                {
                    error = true;
                    PRINT_JSON_ERROR(error_out, "Could not add event to '%s': %s",
                                     new_master->name(), error_msg.c_str());
                }

                // Step 7: Set all slaves to replicate from the master.
                // The following commands are only sent to slaves.
                ServerArray slaves;
                for (auto target : targets)
                {
                    if (target != new_master)
                    {
                        slaves.push_back(target);
                    }
                }

                if (!slaves.empty())
                {
                    SERVER* new_master_srv = new_master->server;
                    // Using Slave_Pos here since gtid_slave_pos was set earlier.
                    SlaveStatus::Settings new_conn("", new_master_srv, GtidMode::SLAVE);
                    // Expect this to complete quickly.
                    GeneralOpData general(OpStart::MANUAL, rval.output, 0s);
                    size_t slave_conns_started = 0;
                    for (auto slave : slaves)
                    {
                        if (slave->create_start_slave(general, new_conn))
                        {
                            slave_conns_started++;
                        }
                    }

                    if (slave_conns_started == slaves.size())
                    {
                        // TODO: Properly check slave IO/SQL threads.
                        MXB_NOTICE("All replicas redirected successfully.");
                    }
                    else
                    {
                        error = true;
                        PRINT_JSON_ERROR(error_out, "Some servers were not redirected to '%s'.",
                                         new_master->name());
                    }
                }
            }
            else
            {
                error = true;
                PRINT_JSON_ERROR(error_out, "Could not enable writes on '%s': %s",
                                 new_master->name(), error_msg.c_str());
            }
        }

        if (error)
        {
            PRINT_JSON_ERROR(error_out, "Replication reset failed or succeeded only partially. "
                                        "Server cluster may be in an invalid state for replication.");
        }
        success = !error;

        reset_wait_timeout_all_servers();
    }
    m_state = State::IDLE;
    rval.success = success;
    return rval;
}

/**
 * Redirect slave connections from the promotion target to replicate from the demotion target and vice versa.
 *
 * @param op Operation descriptor
 * @param redirected_to_promo Output for slaves successfully redirected to promotion target
 * @param redirected_to_demo Output for slaves successfully redirected to demotion target
 * @return The number of slaves successfully redirected
 */
int MariaDBMonitor::redirect_slaves_ex(GeneralOpData& general, OperationType type,
                                       const MariaDBServer* promotion_target,
                                       const MariaDBServer* demotion_target,
                                       ServerArray* redirected_to_promo, ServerArray* redirected_to_demo)
{
    const bool is_switchover = (type == OperationType::SWITCHOVER || type == OperationType::SWITCHOVER_FORCE);
    mxb_assert(is_switchover || type == OperationType::FAILOVER || type == OperationType::FAILOVER_SAFE);

    // Slaves of demotion target are redirected to promotion target.
    // Try to redirect even disconnected slaves.
    ServerArray redirect_to_promo_target = get_redirectables(demotion_target, promotion_target);
    // Slaves of promotion target are redirected to demotion target in case of switchover.
    // This list contains elements only when promoting a relay in switchover.
    ServerArray redirect_to_demo_target;
    if (is_switchover)
    {
        redirect_to_demo_target = get_redirectables(promotion_target, demotion_target);
    }
    if (redirect_to_promo_target.empty() && redirect_to_demo_target.empty())
    {
        // This is ok, nothing to do.
        return 0;
    }

    /* In complicated topologies, this redirection can get tricky. It's possible that a slave is
     * replicating from both promotion and demotion targets and with different settings. This leads
     * to a somewhat similar situation as in promotion (connection copy/merge).
     *
     * Neither slave connection can be redirected since they would be conflicting. As a temporary
     * solution, such duplicate slave connections are for now avoided by not redirecting them. If this
     * becomes an issue (e.g. connection settings need to be properly preserved), add code which:
     * 1) In switchover, swaps the connections by first deleting or redirecting the other to a nonsensial
     * host to avoid host:port conflict.
     * 2) In failover, deletes the connection to promotion target and redirects the one to demotion target,
     * or does the same as in 1.
     */

    const char redir_fmt[] = "Redirecting %s to replicate from '%s' instead of '%s'.";
    string slave_names_to_promo = monitored_servers_to_string(redirect_to_promo_target);
    string slave_names_to_demo = monitored_servers_to_string(redirect_to_demo_target);
    mxb_assert(slave_names_to_demo.empty() || is_switchover);

    // Print both name lists if both have items, otherwise just the one with items.
    if (!slave_names_to_promo.empty() && !slave_names_to_demo.empty())
    {
        MXB_NOTICE("Redirecting %s to replicate from '%s' instead of '%s', and %s to replicate from "
                   "'%s' instead of '%s'.",
                   slave_names_to_promo.c_str(), promotion_target->name(), demotion_target->name(),
                   slave_names_to_demo.c_str(), demotion_target->name(), promotion_target->name());
    }
    else if (!slave_names_to_promo.empty())
    {
        MXB_NOTICE(redir_fmt,
                   slave_names_to_promo.c_str(), promotion_target->name(), demotion_target->name());
    }
    else if (!slave_names_to_demo.empty())
    {
        MXB_NOTICE(redir_fmt,
                   slave_names_to_demo.c_str(), demotion_target->name(), promotion_target->name());
    }

    int successes = 0;
    int fails = 0;
    int conflicts = 0;
    auto redirection_helper =
        [&general, &conflicts, &successes, &fails](ServerArray& redirect_these, const MariaDBServer* from,
                                                   const MariaDBServer* to, ServerArray* redirected) {
        for (MariaDBServer* redirectable : redirect_these)
        {
            mxb_assert(redirected);
            /* If the connection exists, even if disconnected, don't redirect.
             * Compare host:port, since that is how server detects duplicate connections.
             * Ignore for now the possibility of different host:ports having same server id:s
             * etc as such setups shouldn't try failover/switchover anyway. */
            auto existing_conn = redirectable->slave_connection_status_host_port(to);
            if (existing_conn)
            {
                // Already has a connection to redirect target.
                conflicts++;
                MXB_WARNING("'%s' already has a replica connection to '%s', connection to '%s' was "
                            "not redirected.",
                            redirectable->name(), to->name(), from->name());
            }
            else
            {
                // No conflict, redirect as normal.
                auto old_conn = redirectable->slave_connection_status(from);
                auto old_settings = old_conn->settings;
                fix_gtid_mode(old_settings.gtid_mode);
                if (redirectable->redirect_existing_slave_conn(general, old_settings, to))
                {
                    successes++;
                    redirected->push_back(redirectable);
                }
                else
                {
                    fails++;
                }
            }
        }
    };

    redirection_helper(redirect_to_promo_target, demotion_target, promotion_target, redirected_to_promo);
    redirection_helper(redirect_to_demo_target, promotion_target, demotion_target, redirected_to_demo);

    // Redirection may have caused errors. Since redirect_slaves_ex is only ran when failover/switchover
    // is considered a success, remove any errors from the output. The errors have already been written to
    // log.
    auto& error_out = general.error_out;
    if (error_out.object_size() > 0)
    {
        error_out = mxb::Json(mxb::Json::Type::OBJECT);
    }

    if (fails == 0 && conflicts == 0)
    {
        MXB_NOTICE("All redirects successful.");
    }
    else if (fails == 0)
    {
        MXB_NOTICE("%i slave connections were redirected while %i connections were ignored.",
                   successes, conflicts);
    }
    else
    {
        int total = fails + conflicts + successes;
        MXB_WARNING("%i redirects failed, %i slave connections ignored and %i redirects successful "
                    "out of %i.", fails, conflicts, successes, total);
    }
    return successes;
}

/**
 * (Re)join given servers to the cluster. The servers in the array are assumed to be joinable.
 * Usually the list is created by get_joinable_servers().
 *
 * @param joinable_servers Which servers to rejoin
 * @return The number of servers successfully rejoined
 */
uint32_t MariaDBMonitor::do_rejoin(GeneralOpData& op, const ServerArray& joinable_servers)
{
    SERVER* master_server = m_master->server;
    const char* master_name = master_server->name();
    uint32_t servers_joined = 0;
    bool rejoin_error = false;
    m_state = State::REJOIN;
    if (!joinable_servers.empty())
    {
        // Usually rejoin should be fast, just a "change master to ...", so changing wait_timeouts would not
        // be required. However, old master demotion may contain custom commands that take some time, so
        // be on the safe side here.
        maybe_set_wait_timeout_all_servers(joinable_servers.size() * m_settings.shared.switchover_timeout);

        for (MariaDBServer* joinable : joinable_servers)
        {
            const char* name = joinable->name();
            bool op_success = false;

            if (joinable->m_slave_status.empty())
            {
                // Assume that server is an old master which was failed over. Even if this is not really
                // the case, the following is unlikely to do damage.
                ServerOperation demotion(joinable, ServerOperation::TargetType::MASTER);
                if (joinable->demote(op, demotion, OperationType::REJOIN))
                {
                    MXB_NOTICE("Directing standalone server '%s' to replicate from '%s'.", name, master_name);
                    // A slave connection description is required. As this is the only connection, no name
                    // is required.
                    SlaveStatus::Settings new_conn("", master_server, GtidMode::CURRENT);
                    op_success = joinable->create_start_slave(op, new_conn);
                }
                else
                {
                    PRINT_JSON_ERROR(op.error_out, "Failed to prepare (demote) standalone "
                                                   "server '%s' for rejoin.", name);
                }
            }
            else
            {
                MXB_NOTICE("Server '%s' is replicating from a server other than '%s', "
                           "redirecting it to '%s'.",
                           name, master_name, master_name);
                // Multisource replication does not get to this point unless enforce_simple_topology is
                // enabled. If multisource replication is used, we must remove the excess connections.
                mxb_assert(joinable->m_slave_status.size() == 1 || m_settings.enforce_simple_topology);

                if (joinable->m_slave_status.size() > 1)
                {
                    SlaveStatusArray extra_conns(std::next(joinable->m_slave_status.begin()),
                                                 joinable->m_slave_status.end());

                    MXB_NOTICE("Erasing %lu replication connections(s) from server '%s'.",
                               extra_conns.size(), name);
                    joinable->remove_slave_conns(op, extra_conns);
                }

                auto slave_settings = joinable->m_slave_status[0].settings;
                fix_gtid_mode(slave_settings.gtid_mode);
                op_success = joinable->redirect_existing_slave_conn(op, slave_settings, m_master);
            }

            if (op_success)
            {
                servers_joined++;
                m_cluster_modified = true;
            }
            else
            {
                rejoin_error = true;
            }
        }

        reset_wait_timeout_all_servers();
    }

    m_state = State::IDLE;
    if (rejoin_error)
    {
        delay_auto_cluster_ops();
    }
    return servers_joined;
}

/**
 * Check if the cluster is a valid rejoin target.
 *
 * @return True if master and gtid domain are known
 */
bool MariaDBMonitor::cluster_can_be_joined()
{
    return m_master && m_master->is_master() && m_master_gtid_domain != GTID_DOMAIN_UNKNOWN;
}

/**
 * Scan the servers in the cluster and add (re)joinable servers to an array.
 *
 * @param output Array to save results to. Each element is a valid (re)joinable server according
 * to latest data.
 * @return False, if there were possible rejoinable servers but communications error to master server
 * prevented final checks.
 */
bool MariaDBMonitor::get_joinable_servers(GeneralOpData& op, ServerArray* output)
{
    mxb_assert(output);

    // Whether a join operation should be attempted or not depends on several criteria. Start with the ones
    // easiest to test. Go though all slaves and construct a preliminary list.
    ServerArray suspects;
    for (MariaDBServer* server : m_servers)
    {
        if (server_is_rejoin_suspect(op, server))
        {
            suspects.push_back(server);
        }
    }

    // Update Gtid of master for better info.
    bool comm_ok = true;
    if (!suspects.empty())
    {
        string gtid_update_error;
        if (m_master->update_gtids(&gtid_update_error))
        {
            for (auto* suspect : suspects)
            {
                string rejoin_err_msg;
                if (suspect->can_replicate_from(m_master, &rejoin_err_msg))
                {
                    output->push_back(suspect);
                }
                else if (m_warn_cannot_rejoin)
                {
                    // Print a message explaining why an auto-rejoin is not done. Suppress printing.
                    MXB_WARNING("Automatic rejoin was not attempted on server '%s' even though it is a "
                                "valid candidate. Will keep retrying with this message suppressed for all "
                                "servers. Errors: \n%s", suspect->name(), rejoin_err_msg.c_str());
                    m_warn_cannot_rejoin = false;
                }
            }
        }
        else
        {
            MXB_ERROR("The GTIDs of primary server '%s' could not be updated while attempting an automatic "
                      "rejoin: %s", m_master->name(), gtid_update_error.c_str());
            comm_ok = false;
        }
    }
    else
    {
        m_warn_cannot_rejoin = true;
    }
    return comm_ok;
}

/**
 * Checks if a server is a possible rejoin candidate. A true result from this function is not yet sufficient
 * criteria and another call to can_replicate_from() should be made.
 *
 * @param rejoin_cand Server to check
 * @param output Error output. If NULL, no error is printed to log.
 * @return True, if server is a rejoin suspect.
 */
bool MariaDBMonitor::server_is_rejoin_suspect(GeneralOpData& op, MariaDBServer* rejoin_cand)
{
    bool is_suspect = false;
    auto output = op.error_out;
    if (rejoin_cand->is_usable() && !rejoin_cand->is_master())
    {
        // Has no slave connection, yet is not a master.
        if (rejoin_cand->m_slave_status.empty())
        {
            is_suspect = true;
        }
        // Or has existing slave connection ...
        else if (rejoin_cand->m_slave_status.size() == 1)
        {
            SlaveStatus* slave_status = &rejoin_cand->m_slave_status[0];

            // which is connected to master but it's the wrong one
            if (slave_status->slave_io_running == SlaveStatus::SLAVE_IO_YES
                && slave_status->master_server_id != m_master->m_server_id)
            {
                is_suspect = true;
            }
            // or is disconnected but master host or port is wrong.
            else if (slave_status->slave_io_running == SlaveStatus::SLAVE_IO_CONNECTING
                     && slave_status->slave_sql_running)
            {
                if (!slave_status->settings.master_endpoint.points_to_server(*m_master->server))
                {
                    is_suspect = true;
                }
            }
        }
        else if (m_settings.enforce_simple_topology)
        {
            // If enforce_simple_topology is enabled, the presence of multiple slave connections always
            // triggers a rejoin as only one must be configured.
            is_suspect = true;
        }

        if (op.start == OpStart::MANUAL && !is_suspect)
        {
            /* User has requested a manual rejoin but with a server which has multiple slave connections or
             * is already connected or trying to connect to the correct master. TODO: Slave IO stopped is
             * not yet handled perfectly. */
            if (rejoin_cand->m_slave_status.size() > 1)
            {
                const char MULTI_SLAVE[] = "Server '%s' has multiple slave connections, cannot rejoin.";
                PRINT_JSON_ERROR(output, MULTI_SLAVE, rejoin_cand->name());
            }
            else
            {
                const char CONNECTED[] = "Server '%s' is already connected or trying to connect to the "
                                         "correct primary server.";
                PRINT_JSON_ERROR(output, CONNECTED, rejoin_cand->name());
            }
        }
    }
    else if (op.start == OpStart::MANUAL)
    {
        PRINT_JSON_ERROR(output, "Server '%s' is primary or not running.", rejoin_cand->name());
    }
    return is_suspect;
}

/**
 * Performs switchover for a simple topology (1 master, N slaves, no intermediate masters). If an
 * intermediate step fails, the cluster may be left without a master and manual intervention is
 * required to fix things.
 *
 * @param op Operation descriptor
 * @return True if successful. If false, replication may be broken.
 */
bool MariaDBMonitor::switchover_perform(SwitchoverParams& op)
{
    using std::chrono::seconds;
    using std::chrono::duration_cast;
    mxb_assert(op.demotion.target && op.promotion.target);
    maybe_set_wait_timeout_all_servers(m_settings.shared.switchover_timeout);

    const OperationType type = (op.type == SwitchoverType::NORMAL || op.type == SwitchoverType::AUTO) ?
        OperationType::SWITCHOVER : OperationType::SWITCHOVER_FORCE;
    MariaDBServer* const promotion_target = op.promotion.target;
    MariaDBServer* const demotion_target = op.demotion.target;

    bool rval = false;
    m_state = State::DEMOTE;

    // Step 0: Prepare connection to old master
    // Some of the following commands (e.g. set read_only=1) can take a while. The basic monitor timeouts
    // may be too small, so reconnect with larger. To retain any exclusive locks held by the monitor,
    // backup the old connection.
    bool ok_to_demote = false;
    StopWatch timer;
    auto new_conn_timeout = round_to_seconds(op.general.time_remaining) * 1s;
    if (demotion_target->relax_connector_timeouts(new_conn_timeout))
    {
        ok_to_demote = true;
    }
    op.general.time_remaining -= timer.lap();

    // Step 1: Set read-only to on, flush logs, update gtid:s.
    if (ok_to_demote && demotion_target->demote(op.general, op.demotion, type))
    {
        m_cluster_modified = true;
        bool catchup_and_promote_success = false;
        timer.restart();
        // Step 2: Wait for the promotion target to catch up with the demotion target. Disregard the other
        // slaves of the promotion target to avoid needless waiting.
        // The gtid:s of the demotion target were updated at the end of demotion.
        // If forcing a switch, do not require the catchup to succeed as old master may not be frozen and
        // could send events continuously.
        m_state = State::WAIT_FOR_TARGET_CATCHUP;
        if (promotion_target->catchup_to_master(op.general, demotion_target->m_gtid_binlog_pos)
            || type == OperationType::SWITCHOVER_FORCE)
        {
            MXB_INFO("Switchover: Catchup took %.1f seconds.", mxb::to_secs(timer.lap()));
            // Step 3: On new master: remove slave connections, set read-only to OFF etc. This needs to
            // succeed even in switchover-force, as otherwise the operation makes no sense.
            m_state = State::PROMOTE_TARGET;
            if (promotion_target->promote(op.general, op.promotion, type, demotion_target))
            {
                // Point of no return. Even if following steps fail, do not try to undo.
                // Switchover considered at least partially successful.
                catchup_and_promote_success = true;
                rval = true;
                if (op.promotion.target_type == ServerOperation::TargetType::MASTER)
                {
                    // Force a master swap on next tick.
                    m_next_master = promotion_target;
                }

                // Step 4: Start replication on old master and redirect slaves. Using Current_Pos since
                // Slave_Pos is likely obsolete or empty.
                m_state = State::REJOIN;
                ServerArray redirected_to_promo_target;
                if (demotion_target->copy_slave_conns(op.general, op.demotion.conns_to_copy,
                                                      promotion_target, GtidMode::CURRENT))
                {
                    redirected_to_promo_target.push_back(demotion_target);
                }
                else
                {
                    MXB_WARNING("Could not copy slave connections from '%s' to '%s'.",
                                promotion_target->name(), demotion_target->name());
                }
                ServerArray redirected_to_demo_target;
                redirect_slaves_ex(op.general, type, promotion_target, demotion_target,
                                   &redirected_to_promo_target, &redirected_to_demo_target);

                if (!redirected_to_promo_target.empty() || !redirected_to_demo_target.empty())
                {
                    timer.restart();
                    // Step 5: Finally, check that slaves are replicating.
                    m_state = State::CONFIRM_REPLICATION;
                    wait_cluster_stabilization(op.general, redirected_to_promo_target, promotion_target);
                    wait_cluster_stabilization(op.general, redirected_to_demo_target, demotion_target);
                    auto step6_duration = timer.lap();
                    MXB_INFO("Switchover: slave replication confirmation took %.1f seconds with "
                             "%.1f seconds to spare.",
                             mxb::to_secs(step6_duration), mxb::to_secs(op.general.time_remaining));
                }
            }
        }

        if (!catchup_and_promote_success)
        {
            // Step 2 or 3 failed, try to undo step 1 by promoting the demotion target back to master.
            // Reset the time limit since the last part may have used it all.
            MXB_NOTICE("Attempting to undo changes to '%s'.", demotion_target->name());
            const mxb::Duration demotion_undo_time_limit(m_settings.shared.switchover_timeout);
            GeneralOpData general_undo(op.general.start, op.general.error_out, demotion_undo_time_limit);
            if (demotion_target->promote(general_undo, op.promotion, OperationType::UNDO_DEMOTION, nullptr))
            {
                MXB_NOTICE("'%s' restored to original status.", demotion_target->name());
            }
            else
            {
                PRINT_JSON_ERROR(op.general.error_out,
                                 "Restoring of '%s' failed, cluster may be in an invalid state.",
                                 demotion_target->name());
            }
        }
    }

    demotion_target->restore_connector_timeouts();
    m_state = State::IDLE;
    reset_wait_timeout_all_servers();
    return rval;
}

/**
 * Performs failover for a simple topology (1 master, N slaves, no intermediate masters).
 *
 * @param op Operation descriptor
 * @return True if successful
 */
bool MariaDBMonitor::failover_perform(FailoverParams& op)
{
    mxb_assert(op.promotion.target && op.demotion_target);
    maybe_set_wait_timeout_all_servers(m_settings.failover_timeout);

    const OperationType type = OperationType::FAILOVER;
    MariaDBServer* const promotion_target = op.promotion.target;
    auto const demotion_target = op.demotion_target;

    bool rval = false;
    // Step 1: Stop and reset slave, set read-only to OFF.
    m_state = State::PROMOTE_TARGET;
    if (promotion_target->promote(op.general, op.promotion, type, demotion_target))
    {
        // Point of no return. Even if following steps fail, do not try to undo. Failover considered
        // at least partially successful.
        rval = true;
        m_cluster_modified = true;
        if (op.promotion.target_type == ServerOperation::TargetType::MASTER)
        {
            // Force a master swap on next tick.
            m_next_master = promotion_target;
        }

        // Step 2: Redirect slaves.
        m_state = State::REJOIN;
        ServerArray redirected_slaves;
        redirect_slaves_ex(op.general, type, promotion_target, demotion_target, &redirected_slaves, nullptr);
        if (!redirected_slaves.empty())
        {
            StopWatch timer;
            /* Step 3: Finally, check that slaves are connected to the new master. Even if
             * time is out at this point, wait_cluster_stabilization() will check the slaves
             * once so that latest status is printed. */
            m_state = State::CONFIRM_REPLICATION;
            wait_cluster_stabilization(op.general, redirected_slaves, promotion_target);
            MXB_INFO("Failover: slave replication confirmation took %.1f seconds with "
                     "%.1f seconds to spare.",
                     mxb::to_secs(timer.lap()), mxb::to_secs(op.general.time_remaining));
        }
    }
    m_state = State::IDLE;
    reset_wait_timeout_all_servers();
    return rval;
}

/**
 * Check that the given slaves are connected and replicating from the new master. Only checks
 * the SLAVE STATUS of the slaves.
 *
 * @param op Operation descriptor
 * @param redirected_slaves Slaves to check
 * @param new_master The target server of the slave connections
 */
void MariaDBMonitor::wait_cluster_stabilization(GeneralOpData& op, const ServerArray& redirected_slaves,
                                                const MariaDBServer* new_master)
{
    if (redirected_slaves.empty())
    {
        // No need to check anything or print messages.
        return;
    }

    maxbase::Duration& time_remaining = op.time_remaining;
    StopWatch timer;
    // Check all the servers in the list. Using a set because erasing from container.
    std::set<MariaDBServer*> unconfirmed(redirected_slaves.begin(), redirected_slaves.end());
    ServerArray successes;
    ServerArray repl_fails;
    ServerArray query_fails;
    bool time_is_up = false;    // Try at least once, even if time is up.

    while (!unconfirmed.empty() && !time_is_up)
    {
        auto iter = unconfirmed.begin();
        while (iter != unconfirmed.end())
        {
            MariaDBServer* slave = *iter;
            if (slave->do_show_slave_status())
            {
                auto slave_conn = slave->slave_connection_status_host_port(new_master);
                if (slave_conn == nullptr)
                {
                    // Highly unlikely. Maybe someone just removed the slave connection after it was created.
                    MXB_WARNING("'%s' does not have a slave connection to '%s' although one should have "
                                "been created.",
                                slave->name(), new_master->name());
                    repl_fails.push_back(*iter);
                    iter = unconfirmed.erase(iter);
                }
                else if (slave_conn->slave_io_running == SlaveStatus::SLAVE_IO_YES
                         && slave_conn->slave_sql_running == true)
                {
                    // This slave has connected to master and replication seems to be ok.
                    successes.push_back(*iter);
                    iter = unconfirmed.erase(iter);
                }
                else if (slave_conn->slave_io_running == SlaveStatus::SLAVE_IO_NO)
                {
                    // IO error on slave
                    MXB_WARNING("%s cannot start replication because of IO thread error: '%s'.",
                                slave_conn->settings.to_string().c_str(), slave_conn->last_io_error.c_str());
                    repl_fails.push_back(*iter);
                    iter = unconfirmed.erase(iter);
                }
                else if (slave_conn->slave_sql_running == false)
                {
                    // SQL error on slave
                    MXB_WARNING("%s cannot start replication because of SQL thread error: '%s'.",
                                slave_conn->settings.to_string().c_str(), slave_conn->last_sql_error.c_str());
                    repl_fails.push_back(*iter);
                    iter = unconfirmed.erase(iter);
                }
                else
                {
                    // Slave IO is still connecting, must wait.
                    ++iter;
                }
            }
            else
            {
                query_fails.push_back(*iter);
                iter = unconfirmed.erase(iter);
            }
        }

        time_remaining -= timer.lap();
        if (!unconfirmed.empty())
        {
            if (time_remaining.count() > 0)
            {
                maxbase::Duration standard_sleep = 500ms;
                // If we have unconfirmed slaves and have time remaining, sleep a bit and try again.
                /* TODO: This sleep is kinda pointless, because whether or not replication begins,
                 * all operations for failover/switchover are complete. The sleep is only required to
                 * get correct messages to the user. Think about removing it, or shortening the maximum
                 * time of this function. */
                auto sleep_time = std::min(time_remaining, standard_sleep);
                std::this_thread::sleep_for(sleep_time);
            }
            else
            {
                // Have undecided slaves and is out of time.
                time_is_up = true;
            }
        }
    }

    if (successes.size() == redirected_slaves.size())
    {
        // Complete success.
        MXB_NOTICE("All redirected slaves successfully started replication from '%s'.", new_master->name());
    }
    else
    {
        if (!successes.empty())
        {
            MXB_NOTICE("%s successfully started replication from '%s'.",
                       monitored_servers_to_string(successes).c_str(), new_master->name());
        }
        // Something went wrong.
        auto fails = query_fails.size() + repl_fails.size() + unconfirmed.size();
        const char MSG[] = "%lu slaves did not start replicating from '%s'. "
                           "%lu encountered an I/O or SQL error, %lu failed to reply and %lu did not "
                           "connect to '%s' within the time limit.";
        MXB_WARNING(MSG, fails, new_master->name(), repl_fails.size(), query_fails.size(),
                    unconfirmed.size(), new_master->name());

        // If any of the unconfirmed slaves have error messages in their slave status, print them. They
        // may explain what went wrong.
        for (auto failed_slave : unconfirmed)
        {
            auto slave_conn = failed_slave->slave_connection_status_host_port(new_master);
            if (slave_conn && !slave_conn->last_io_error.empty())
            {
                MXB_WARNING("%s did not connect because of error: '%s'",
                            slave_conn->settings.to_string().c_str(), slave_conn->last_io_error.c_str());
            }
        }
    }
    time_remaining -= timer.lap();
}

/**
 * Select a promotion target for failover/switchover. Looks at the slaves of 'demotion_target' and selects
 * the server with the most up-do-date event or, if events are equal, the one with the best settings and
 * status. When comparing, also selects the domain id used for comparing gtids.
 *
 * @param demotion_target The former master server/relay
 * @param op Switchover or failover
 * @param log_mode Print log or operate silently
 * @param gtid_domain_out Output for selected gtid domain id guess
 * @param error_out Error output
 * @return The selected promotion target or NULL if no valid candidates
 */
MariaDBServer*
MariaDBMonitor::select_promotion_target(MariaDBServer* demotion_target, OperationType op, Log log_mode,
                                        int64_t* gtid_domain_out, mxb::Json& error_out)
{
    /* Select a new master candidate. Selects the one with the latest event in relay log.
     * If multiple slaves have same number of events, select the one with most processed events. */

    if (!demotion_target->m_node.children.empty())
    {
        if (log_mode == Log::ON)
        {
            MXB_NOTICE("Selecting a server to promote and replace '%s'. Candidates are: %s.",
                       demotion_target->name(),
                       monitored_servers_to_string(demotion_target->m_node.children).c_str());
        }
    }
    else
    {
        PRINT_ERROR_IF(log_mode, error_out, "'%s' does not have any slaves to promote.",
                       demotion_target->name());
        return nullptr;
    }

    // Servers that cannot be selected because of exclusion, but seem otherwise ok.
    ServerArray valid_but_excluded;

    string all_reasons;
    DelimitedPrinter printer("\n");
    // The valid promotion candidates are the slaves replicating directly from the demotion target.
    ServerArray candidates;
    for (MariaDBServer* cand : demotion_target->m_node.children)
    {
        string reason;
        if (!cand->can_be_promoted(op, demotion_target, &reason))
        {
            string msg = string_printf("'%s' cannot be selected because %s", cand->name(), reason.c_str());
            printer.cat(all_reasons, msg);
        }
        else if (server_is_excluded(cand))
        {
            valid_but_excluded.push_back(cand);
            string msg = string_printf("'%s' cannot be selected because it is excluded.", cand->name());
            printer.cat(all_reasons, msg);
        }
        else
        {
            candidates.push_back(cand);
            // Print some warnings about the candidate server.
            if (log_mode == Log::ON)
            {
                cand->warn_replication_settings();
            }
        }
    }

    MariaDBServer* current_best = nullptr;
    string current_best_reason;
    int64_t gtid_domain = m_master_gtid_domain;
    if (candidates.empty())
    {
        PRINT_ERROR_IF(log_mode, error_out, "No suitable promotion candidate found:\n%s",
                       all_reasons.c_str());
    }
    else
    {
        if (gtid_domain == GTID_DOMAIN_UNKNOWN && m_settings.enforce_simple_topology)
        {
            // Need to guess the domain id. This only happens when failovering without having seen
            // the master running.
            int id_missing_count = 0;
            // Guaranteed to give a value if candidates are ok.
            gtid_domain = guess_gtid_domain(demotion_target, candidates, &id_missing_count);
            mxb_assert(gtid_domain != GTID_DOMAIN_UNKNOWN);
            if (log_mode == Log::ON)
            {
                MXB_WARNING("Gtid-domain id of '%s' is unknown, attempting to guess it by looking at "
                            "gtid:s of candidates.", m_master->name());
                if (id_missing_count > 0)
                {
                    MXB_WARNING("Guessed domain id %li, which is missing on %i candidates. This may cause "
                                "faulty promotion target selection.", gtid_domain, id_missing_count);
                }
                else
                {
                    MXB_WARNING("Guessed domain id %li, which is on all candidates.", gtid_domain);
                }
            }
        }

        // Check which candidate is best. Default select the first.
        current_best = candidates.front();
        candidates.erase(candidates.begin());
        if (!all_reasons.empty() && log_mode == Log::ON)
        {
            MXB_WARNING("Some servers were disqualified for promotion:\n%s", all_reasons.c_str());
        }
        for (MariaDBServer* cand : candidates)
        {
            if (is_candidate_better(cand, current_best, demotion_target, gtid_domain, &current_best_reason))
            {
                // Select the server for promotion, for now.
                current_best = cand;
            }
        }
    }

    // Check if any of the excluded servers would be better than the best candidate. Only print one item.
    if (log_mode == Log::ON)
    {
        for (MariaDBServer* excluded : valid_but_excluded)
        {
            const char* excluded_name = excluded->name();
            if (current_best == nullptr)
            {
                const char EXCLUDED_ONLY_CAND[] = "Server '%s' is a viable choice for new primary, "
                                                  "but cannot be selected as it's excluded.";
                MXB_WARNING(EXCLUDED_ONLY_CAND, excluded_name);
                break;
            }
            else if (is_candidate_better(excluded, current_best, demotion_target, gtid_domain))
            {
                // Print a warning if this server is actually a better candidate than the previous best.
                const char EXCLUDED_CAND[] = "Server '%s' is superior to current best candidate '%s', "
                                             "but cannot be selected as it's excluded. This may lead to "
                                             "loss of data if '%s' is ahead of other servers.";
                MXB_WARNING(EXCLUDED_CAND, excluded_name, current_best->name(), excluded_name);
                break;
            }
        }
    }

    if (current_best)
    {
        if (gtid_domain_out)
        {
            *gtid_domain_out = gtid_domain;
        }

        if (log_mode == Log::ON)
        {
            // If there was a specific reason this server was selected, print it now.
            // If the first candidate was chosen (likely all servers were equally good), do not print.
            string msg = string_printf("Selected '%s'", current_best->name());
            msg += current_best_reason.empty() ? "." : (" because " + current_best_reason);
            MXB_NOTICE("%s", msg.c_str());
        }
    }
    return current_best;
}

/**
 * Is the server in the excluded list
 *
 * @param server Server to test
 * @return True if server is in the excluded-list of the monitor.
 */
bool MariaDBMonitor::server_is_excluded(const MariaDBServer* server)
{
    for (MariaDBServer* excluded : m_excluded_servers)
    {
        if (excluded == server)
        {
            return true;
        }
    }
    return false;
}

/**
 * Is the candidate a better choice for master than the previous best?
 *
 * @param candidate_info Server info of new candidate
 * @param current_best_info Server info of current best choice
 * @param demotion_target Server which will be demoted
 * @param gtid_domain Which domain to compare
 * @param reason_out Why is the candidate better than current_best
 * @return True if candidate is better
 */
bool MariaDBMonitor::is_candidate_better(const MariaDBServer* candidate, const MariaDBServer* current_best,
                                         const MariaDBServer* demotion_target, uint32_t gtid_domain,
                                         std::string* reason_out)
{
    const SlaveStatus* cand_slave_conn = candidate->slave_connection_status(demotion_target);
    const SlaveStatus* curr_best_slave_conn = current_best->slave_connection_status(demotion_target);
    mxb_assert(cand_slave_conn && curr_best_slave_conn);

    uint64_t cand_io = cand_slave_conn->gtid_io_pos.get_gtid(gtid_domain).m_sequence;
    uint64_t curr_io = curr_best_slave_conn->gtid_io_pos.get_gtid(gtid_domain).m_sequence;
    string reason;
    bool is_better = false;
    // A slave with a later event in relay log is always preferred.
    if (cand_io > curr_io)
    {
        is_better = true;
        reason = "it has received more events.";
    }
    // If io sequences are identical ...
    else if (cand_io == curr_io)
    {
        uint64_t cand_processed = candidate->m_gtid_current_pos.get_gtid(gtid_domain).m_sequence;
        uint64_t curr_processed = current_best->m_gtid_current_pos.get_gtid(gtid_domain).m_sequence;
        // ... the slave with more events processed wins.
        if (cand_processed > curr_processed)
        {
            is_better = true;
            reason = "it has processed more events.";
        }
        // If gtid positions are identical ...
        else if (cand_processed == curr_processed)
        {
            bool cand_updates = candidate->m_rpl_settings.log_slave_updates;
            bool curr_updates = current_best->m_rpl_settings.log_slave_updates;
            // ... prefer a slave with log_slave_updates.
            if (cand_updates && !curr_updates)
            {
                is_better = true;
                reason = "it has 'log_slave_updates' on.";
            }
            // If both have log_slave_updates on ...
            else if (cand_updates && curr_updates)
            {
                bool cand_disk_ok = !candidate->server->is_low_on_disk_space();
                bool curr_disk_ok = !current_best->server->is_low_on_disk_space();
                // ... prefer a slave without disk space issues.
                if (cand_disk_ok && !curr_disk_ok)
                {
                    is_better = true;
                    reason = "it is not low on disk space.";
                }
            }
        }
    }

    if (reason_out && is_better)
    {
        *reason_out = reason;
    }
    return is_better;
}

/**
 * Check cluster and parameters for suitability to failover. Also writes found servers to output pointers.
 *
 * @param log_mode Logging mode
 * @param error_out Error output
 * @return Operation object if cluster is suitable and failover may proceed, or NULL on error
 */
unique_ptr<MariaDBMonitor::FailoverParams>
MariaDBMonitor::failover_prepare(FailoverType fo_type, Log log_mode, OpStart start, mxb::Json& error_out)
{
    // This function resembles 'switchover_prepare', but does not yet support manual selection.

    // Check that the cluster has a non-functional master server and that one of the slaves of
    // that master can be promoted. TODO: add support for demoting a relay server.
    MariaDBServer* demotion_target = nullptr;
    auto binlog_policy = MariaDBServer::FOBinlogPosPolicy::FAIL_UNKNOWN;
    if ((start == OpStart::AUTO && m_settings.enforce_simple_topology)
        || (start == OpStart::MANUAL && fo_type == FailoverType::ALLOW_TRX_LOSS))
    {
        binlog_policy = MariaDBServer::FOBinlogPosPolicy::ALLOW_UNKNOWN;
    }

    // Autoselect current master as demotion target.
    string demotion_msg;
    if (m_master == nullptr)
    {
        const char msg[] = "Can not select a demotion target for failover: cluster does not have a primary.";
        PRINT_ERROR_IF(log_mode, error_out, msg);
    }
    else if (!m_master->can_be_demoted_failover(binlog_policy, &demotion_msg))
    {
        const char msg[] = "Can not select '%s' as a demotion target for failover because %s";
        PRINT_ERROR_IF(log_mode, error_out, msg, m_master->name(), demotion_msg.c_str());
    }
    else
    {
        demotion_target = m_master;
    }

    MariaDBServer* promotion_target = nullptr;
    int64_t gtid_domain_id = GTID_DOMAIN_UNKNOWN;
    if (demotion_target)
    {
        // Autoselect best server for promotion.
        auto op = (fo_type == FailoverType::ALLOW_TRX_LOSS) ? OperationType::FAILOVER :
            OperationType::FAILOVER_SAFE;
        MariaDBServer* promotion_candidate = select_promotion_target(
            demotion_target, op, log_mode, &gtid_domain_id, error_out);
        if (promotion_candidate)
        {
            promotion_target = promotion_candidate;
        }
        else
        {
            PRINT_ERROR_IF(log_mode, error_out, "Could not autoselect promotion target for failover.");
        }
    }

    bool gtid_ok = false;
    if (demotion_target)
    {
        gtid_ok = check_gtid_replication(log_mode, demotion_target, gtid_domain_id, error_out);
    }

    unique_ptr<FailoverParams> rval;
    if (promotion_target && demotion_target && gtid_ok)
    {
        const SlaveStatus* slave_conn = promotion_target->slave_connection_status(demotion_target);
        mxb_assert(slave_conn);
        uint64_t events = promotion_target->relay_log_events(*slave_conn);
        if (events > 0)
        {
            // The relay log of the promotion target is not yet clear. This is not really an error,
            // but should be communicated to the user in the case of manual failover. For automatic
            // failover, it's best to just try again during the next monitor iteration. The difference
            // to a typical prepare-fail is that the relay log status should be logged
            // repeatedly since it is likely to change continuously.
            if (start == OpStart::MANUAL || log_mode == Log::ON)
            {
                const char unproc_fmt[] = "The relay log of '%s' has %lu unprocessed events "
                                          "(Gtid_IO_Pos: %s, Gtid_Current_Pos: %s).";
                string unproc_events = string_printf(
                    unproc_fmt, promotion_target->name(), events, slave_conn->gtid_io_pos.to_string().c_str(),
                    promotion_target->m_gtid_current_pos.to_string().c_str());

                if (start == OpStart::MANUAL)
                {
                    /* Print a bit more helpful error for the user, goes to log too.
                     * This should be a very rare occurrence: either the dba managed to start failover
                     * really fast, or the relay log is massive. In the latter case it's ok
                     * that the monitor does not do the waiting since there  is no telling how long
                     * the wait will be. */
                    const char wait_relay_log[] = "%s To avoid data loss, failover should be postponed until "
                                                  "the log has been processed. Please try again later.";
                    string error_msg = string_printf(wait_relay_log, unproc_events.c_str());
                    PRINT_JSON_ERROR(error_out, "%s", error_msg.c_str());
                }
                else if (log_mode == Log::ON)
                {
                    // For automatic failover the message is more typical. TODO: Think if this message should
                    // be logged more often.
                    MXB_WARNING("%s To avoid data loss, failover is postponed until the log "
                                "has been processed.", unproc_events.c_str());
                }
            }
        }
        else
        {
            // The Duration ctor taking a double interprets the value as seconds.
            auto time_limit = std::chrono::seconds(m_settings.failover_timeout);
            auto target_type = (demotion_target == m_master) ? ServerOperation::TargetType::MASTER :
                ServerOperation::TargetType::RELAY;
            ServerOperation promotion(promotion_target, target_type,
                                      demotion_target->m_slave_status, demotion_target->m_enabled_events);
            GeneralOpData general(start, error_out, time_limit);
            rval = std::make_unique<FailoverParams>(promotion, demotion_target, general);
        }
    }
    return rval;
}

/**
 * Check if failover is required and perform it if so.
 */
void MariaDBMonitor::handle_auto_failover()
{
    if (!m_master || m_master->is_running())
    {
        // No need for failover. This also applies if master is in maintenance, because that is a user
        // problem.
        m_warn_master_down = true;
        m_warn_failover_precond = true;
        return;
    }

    const int failcount = m_settings.failcount;
    const int master_down_count = m_master->mon_err_count;

    if (m_warn_master_down)
    {
        if (failcount > 1 && master_down_count < failcount)
        {
            // Failover is not happening yet but likely soon will.
            int ticks_until = failcount - master_down_count;
            MXB_WARNING("Primary has failed. If primary does not return in %i monitor tick(s), failover "
                        "begins.", ticks_until);
        }
        m_warn_master_down = false;
    }

    if (master_down_count >= failcount)
    {
        // Master has been down long enough.
        bool slave_verify_ok = true;
        if (m_settings.verify_master_failure)
        {
            Duration event_age;
            Duration delay_time;
            auto connected_slave = slave_receiving_events(m_master, &event_age, &delay_time);
            if (connected_slave)
            {
                slave_verify_ok = false;
                MXB_NOTICE("Slave '%s' is still connected to '%s' and received a new gtid or heartbeat "
                           "event %.1f seconds ago. Delaying failover for at least %.1f seconds.",
                           connected_slave->name(), m_master->name(),
                           mxb::to_secs(event_age), mxb::to_secs(delay_time));
            }
        }

        if (slave_verify_ok)
        {
            // Failover is required, but first we should check if preconditions are met.
            Log log_mode = m_warn_failover_precond ? Log::ON : Log::OFF;
            mxb::Json dummy(mxb::Json::Type::UNDEFINED);
            auto fo_type = (m_settings.auto_failover == AutoFailover::SAFE) ? FailoverType::SAFE :
                FailoverType::ALLOW_TRX_LOSS;
            auto op = failover_prepare(fo_type, log_mode, OpStart::AUTO, dummy);
            if (op)
            {
                m_warn_failover_precond = true;
                MXB_NOTICE("Performing automatic failover to replace failed primary '%s'.",
                           m_master->name());
                if (failover_perform(*op))
                {
                    MXB_NOTICE(FAILOVER_OK, op->demotion_target->name(), op->promotion.target->name());
                }
                else
                {
                    MXB_ERROR(FAILOVER_FAIL, op->demotion_target->name(), op->promotion.target->name());
                    delay_auto_cluster_ops();
                }
            }
            else
            {
                // Failover was not attempted because of errors, however these errors are not permanent.
                // Servers were not modified, so it's ok to try this again.
                if (m_warn_failover_precond)
                {
                    MXB_WARNING("Not performing automatic failover. Will keep retrying with most "
                                "error messages suppressed.");
                    m_warn_failover_precond = false;
                }
            }
        }
    }
}

/**
 * Is the topology such that failover and switchover are supported, even if not required just yet?
 * Print errors and disable the settings if this is not the case.
 */
void MariaDBMonitor::check_cluster_operations_support()
{
    bool supported = true;
    DelimitedPrinter printer("\n");
    string all_reasons;
    // Currently, only simple topologies are supported. No Relay Masters or multiple slave connections.
    // Gtid-replication is required, and a server version which supports it.
    for (MariaDBServer* server : m_servers)
    {
        // Check capabilities of running servers.
        if (server->is_usable())
        {
            auto& info = server->server->info();
            auto type = info.type();
            if ((type != ServerType::MARIADB && type != ServerType::BLR) || !server->m_capabilities.gtid)
            {
                supported = false;
                auto reason = string_printf("The version of '%s' (%s) is not supported. Failover/switchover "
                                            "requires MariaDB Server 10.4 or later.",
                                            server->name(), info.version_string());
                printer.cat(all_reasons, reason);
            }

            for (const auto& slave_conn : server->m_slave_status)
            {
                if (slave_conn.slave_io_running == SlaveStatus::SLAVE_IO_YES
                    && slave_conn.slave_sql_running && slave_conn.gtid_io_pos.empty())
                {
                    supported = false;
                    auto reason = string_printf("%s is not using gtid-replication.",
                                                slave_conn.settings.to_string().c_str());
                    printer.cat(all_reasons, reason);
                }
            }
        }
    }

    if (!supported)
    {
        const char PROBLEMS[] =
            "The backend cluster does not support failover/switchover due to the following reason(s):\n"
            "%s\n";
        string msg = string_printf(PROBLEMS, all_reasons.c_str());
        MXB_ERROR("%s", msg.c_str());
        delay_auto_cluster_ops();
    }
}

/**
 * Check if a slave is receiving events from master. Returns the first slave that is both
 * connected (or not realized the disconnect yet) and has an event more recent than
 * master_failure_timeout. The age of the event is written in 'event_age_out'.
 *
 * @param demotion_target The server whose slaves should be checked
 * @param event_age_out Output for event age
 * @return The first connected slave or NULL if none found
 */
const MariaDBServer*
MariaDBMonitor::slave_receiving_events(const MariaDBServer* demotion_target, Duration* event_age_out,
                                       Duration* delay_out) const
{
    auto event_timeout(std::chrono::seconds(m_settings.master_failure_timeout));
    auto current_time = maxbase::Clock::now();
    maxbase::TimePoint recent_event_time = current_time - event_timeout;

    const MariaDBServer* connected_slave = nullptr;
    for (MariaDBServer* slave : demotion_target->m_node.children)
    {
        const SlaveStatus* slave_conn = nullptr;
        if (slave->is_running()
            && (slave_conn = slave->slave_connection_status(demotion_target)) != nullptr
            && slave_conn->slave_io_running == SlaveStatus::SLAVE_IO_YES
            && slave_conn->last_data_time >= recent_event_time)
        {
            // The slave is still connected to the correct master and has received events. This means that
            // while MaxScale can't connect to the master, it's probably still alive.
            connected_slave = slave;
            auto latest_event_age = current_time - slave_conn->last_data_time;
            *event_age_out = latest_event_age;
            *delay_out = event_timeout - latest_event_age;
            break;
        }
    }
    return connected_slave;
}

/**
 * Check cluster and parameters for suitability to switchover. Also writes found servers to output pointers.
 *
 * @param promotion_server The server which should be promoted. If null, monitor will autoselect.
 * @param demotion_server The server which should be demoted. Can be null for autoselect.
 * @param log_mode Logging mode
 * @param error_out Error output
 * @return Operation object if cluster is suitable and switchover may proceed, or NULL on error
 */
unique_ptr<MariaDBMonitor::SwitchoverParams>
MariaDBMonitor::switchover_prepare(SwitchoverType type, SERVER* promotion_server, SERVER* demotion_server,
                                   Log log_mode, OpStart start, mxb::Json& error_out)
{
    // Check that both servers are ok if specified, or autoselect them. Demotion target must be checked
    // first since the promotion target depends on it.
    MariaDBServer* demotion_target = nullptr;
    string demotion_msg;
    if (demotion_server)
    {
        // Manual select.
        MariaDBServer* demotion_candidate = get_server(demotion_server);
        if (demotion_candidate == nullptr)
        {
            PRINT_ERROR_IF(log_mode, error_out, NO_SERVER, demotion_server->name(), name());
        }
        else if (!demotion_candidate->can_be_demoted_switchover(type, &demotion_msg))
        {
            PRINT_ERROR_IF(log_mode, error_out,
                           "'%s' is not a valid demotion target for switchover: %s",
                           demotion_candidate->name(), demotion_msg.c_str());
        }
        else
        {
            demotion_target = demotion_candidate;
        }
    }
    else
    {
        // Autoselect current master as demotion target.
        mxb_assert(type != SwitchoverType::AUTO);
        if (m_master == nullptr || (type == SwitchoverType::NORMAL && !m_master->is_master()))
        {
            const char msg[] = "Can not autoselect a demotion target for switchover: cluster does "
                               "not have a primary.";
            PRINT_ERROR_IF(log_mode, error_out, msg);
        }
        else if (!m_master->can_be_demoted_switchover(type, &demotion_msg))
        {
            const char msg[] = "Can not autoselect '%s' as a demotion target for switchover because %s";
            PRINT_ERROR_IF(log_mode, error_out, msg, m_master->name(), demotion_msg.c_str());
        }
        else
        {
            demotion_target = m_master;
        }
    }

    const auto op_type = (type == SwitchoverType::NORMAL || type == SwitchoverType::AUTO) ?
        OperationType::SWITCHOVER : OperationType::SWITCHOVER_FORCE;
    MariaDBServer* promotion_target = nullptr;
    if (demotion_target)
    {
        string promotion_msg;
        if (promotion_server)
        {
            // Manual select.
            MariaDBServer* promotion_candidate = get_server(promotion_server);
            if (promotion_candidate == nullptr)
            {
                PRINT_ERROR_IF(log_mode, error_out, NO_SERVER, promotion_server->name(), name());
            }
            else if (!promotion_candidate->can_be_promoted(op_type, demotion_target, &promotion_msg))
            {
                const char msg[] = "'%s' is not a valid promotion target for switchover because %s";
                PRINT_ERROR_IF(log_mode, error_out, msg, promotion_candidate->name(), promotion_msg.c_str());
            }
            else
            {
                promotion_target = promotion_candidate;
            }
        }
        else
        {
            // Autoselect. More involved than the autoselecting the demotion target.
            MariaDBServer* promotion_candidate = select_promotion_target(demotion_target, op_type,
                                                                         log_mode, nullptr, error_out);
            if (promotion_candidate)
            {
                promotion_target = promotion_candidate;
            }
            else
            {
                PRINT_ERROR_IF(log_mode, error_out, "Could not autoselect promotion target for switchover.");
            }
        }
    }

    bool gtid_ok = false;
    if (demotion_target)
    {
        gtid_ok = check_gtid_replication(log_mode, demotion_target, m_master_gtid_domain, error_out);
    }

    unique_ptr<SwitchoverParams> rval;
    if (promotion_target && demotion_target && gtid_ok)
    {
        maxbase::Duration time_limit(std::chrono::seconds(m_settings.shared.switchover_timeout));
        auto target_type = (demotion_target == m_master) ? ServerOperation::TargetType::MASTER :
            ServerOperation::TargetType::RELAY;
        ServerOperation promotion(promotion_target, target_type,
                                  demotion_target->m_slave_status, demotion_target->m_enabled_events);
        ServerOperation demotion(demotion_target, target_type, promotion_target->m_slave_status,
                                 EventNameSet());
        GeneralOpData general(start, error_out, time_limit);
        rval = std::make_unique<SwitchoverParams>(promotion, demotion, general, type);
    }
    return rval;
}

void MariaDBMonitor::enforce_read_only()
{
    if (!m_master || (!m_settings.enforce_read_only_slaves && !m_settings.enforce_read_only_servers))
    {
        // If primary is not known, do nothing. Don't want to set read_only on a server that may be selected
        // primary next tick.
        return;
    }

    const char QUERY[] = "SET GLOBAL read_only=1;";
    bool error = false;
    for (MariaDBServer* server : m_servers)
    {
        if (server != m_master && !server->is_read_only() && (server->server_type() == ServerType::MARIADB))
        {
            bool is_slave = server->is_slave();
            if (is_slave || (m_settings.enforce_read_only_servers && server->is_usable()))
            {
                MYSQL* conn = server->con;
                if (mxs_mysql_query(conn, QUERY) == 0)
                {
                    const char* type = is_slave ? "replica" : "server";
                    MXB_NOTICE("read_only set to ON on %s %s.", type, server->name());
                }
                else
                {
                    MXB_ERROR("Setting read_only on server %s failed. Error %i: '%s'.",
                              server->name(), mysql_errno(conn), mysql_error(conn));
                    error = true;
                }
            }
        }
    }

    if (error)
    {
        delay_auto_cluster_ops();
    }
}


void MariaDBMonitor::enforce_writable_on_master()
{
    bool error = false;
    if (m_master && m_master->is_read_only() && !m_master->is_in_maintenance())
    {
        auto type = m_master->server_type();
        if (type == ServerType::MARIADB || type == ServerType::MYSQL)
        {
            const char QUERY[] = "SET GLOBAL read_only=0;";
            MYSQL* conn = m_master->con;
            if (mxs_mysql_query(conn, QUERY) == 0)
            {
                MXB_NOTICE("read_only set to OFF on '%s'.", m_master->name());
            }
            else
            {
                MXB_ERROR("Disabling read_only on '%s' failed: '%s'.", m_master->name(), mysql_error(conn));
                error = true;
            }
        }
    }

    if (error)
    {
        delay_auto_cluster_ops();
    }
}

void MariaDBMonitor::handle_low_disk_space_master()
{
    // If master is really out of disk space, it has lost [Master] (if using default settings).
    // This needs to be taken into account in the following checks.
    if (m_master && m_master->is_low_on_disk_space())
    {
        if (m_warn_switchover_precond)
        {
            MXB_WARNING("Primary server '%s' is low on disk space. Attempting to switch it with a slave.",
                        m_master->name());
        }

        // Looks like the master should be swapped out. Before trying it, check if there is even
        // a likely valid slave to swap to.
        Log log_mode = m_warn_switchover_precond ? Log::ON : Log::OFF;
        mxb::Json dummy(mxb::Json::Type::UNDEFINED);
        auto op = switchover_prepare(SwitchoverType::AUTO, nullptr, m_master->server, log_mode,
                                     OpStart::AUTO, dummy);
        if (op)
        {
            m_warn_switchover_precond = true;
            bool switched = switchover_perform(*op);
            if (switched)
            {
                MXB_NOTICE(SWITCHOVER_OK, op->demotion.target->name(), op->promotion.target->name());
            }
            else
            {
                MXB_ERROR(SWITCHOVER_FAIL, op->demotion.target->name(), op->promotion.target->name());
                delay_auto_cluster_ops();
            }
        }
        else
        {
            // Switchover was not attempted because of errors, however these errors are not permanent.
            // Servers were not modified, so it's ok to try this again.
            if (m_warn_switchover_precond)
            {
                MXB_WARNING("Not performing automatic switchover. Will keep retrying with this message "
                            "suppressed.");
                m_warn_switchover_precond = false;
            }
        }
    }
    else
    {
        m_warn_switchover_precond = true;
    }
}

void MariaDBMonitor::handle_auto_rejoin()
{
    mxb::Json dummy(mxb::Json::Type::UNDEFINED);
    // Rejoin doesn't have its own time limit setting. Use switchover time limit for now since
    // the first phase of standalone rejoin is similar to switchover.
    maxbase::Duration time_limit(m_settings.shared.switchover_timeout);
    GeneralOpData op(OpStart::AUTO, dummy, time_limit);

    ServerArray joinable_servers;
    if (get_joinable_servers(op, &joinable_servers))
    {
        uint32_t joins = do_rejoin(op, joinable_servers);
        if (joins > 0)
        {
            MXB_NOTICE("%d server(s) redirected or rejoined the cluster.", joins);
        }
    }
    // get_joinable_servers prints an error if master is unresponsive
}

void MariaDBMonitor::handle_master_write_test()
{
    using WriteTestTblStatus = MariaDBServer::WriteTestTblStatus;
    if (m_master && m_master->is_master())
    {
        const string& target_tbl = m_settings.master_write_test_table;
        if (m_write_test_tbl_status == WriteTestTblStatus::UNKNOWN)
        {
            m_write_test_tbl_status = m_master->check_write_test_table(target_tbl);
        }

        if (m_write_test_tbl_status == WriteTestTblStatus::CREATED)
        {
            auto now = m_worker->epoll_tick_now();
            auto no_change_dur = now - m_last_master_gtid_change;
            if (no_change_dur > m_settings.master_write_test_interval)
            {
                MXB_INFO("gtid_binlog_pos of primary %s has not changed in %.0f seconds. "
                         "Performing write test to table '%s'.",
                         m_master->name(), mxb::to_secs(no_change_dur), target_tbl.c_str());
                if (m_master->test_writability(target_tbl))
                {
                    m_write_test_fails = 0;
                    m_warn_write_test_fail = true;
                    m_last_master_gtid_change = now;
                }
                else
                {
                    m_write_test_fails++;
                    if (m_settings.write_test_fail_action == WriteTestFailAction::FAILOVER)
                    {
                        if (m_write_test_fails >= m_settings.failcount)
                        {
                            // TODO: perform failover. Must be customized, as normal failover does not start
                            // if master appears to be running.
                            MXB_WARNING("Add failover here!");
                            m_write_test_fails = 0;
                            m_warn_write_test_fail = false;
                        }
                        else if (m_warn_write_test_fail)
                        {
                            MXB_WARNING("%s failed write test. If situation persists for %li monitor "
                                        "intervals, failover begins.",
                                        m_master->name(), m_settings.failcount - m_write_test_fails);
                            m_warn_write_test_fail = false;
                        }
                    }
                    else
                    {
                        MXB_ERROR("Primary server %s failed write test. MariaDB Server storage engine may "
                                  "be locked or filesystem cannot be written to.", m_master->name());
                        m_last_master_gtid_change = now;    // Prevents printing the message every tick.
                    }
                }
            }
            else
            {
                m_write_test_fails = 0;
                m_warn_write_test_fail = true;
            }
        }
    }
}

/**
 * Check that the slaves to demotion target are using gtid replication and that the gtid domain of the
 * cluster is defined. Only the slave connections to the demotion target are checked.
 *
 * @param log_mode Logging mode
 * @param demotion_target The server whose slaves should be checked
 * @param cluster_gtid_domain Cluster gtid domain
 * @param error_out Error output
 * @return True if gtid is used
 */
bool MariaDBMonitor::check_gtid_replication(Log log_mode, const MariaDBServer* demotion_target,
                                            int64_t cluster_gtid_domain, mxb::Json& error_out)
{
    bool gtid_domain_ok = false;
    if (cluster_gtid_domain == GTID_DOMAIN_UNKNOWN)
    {
        PRINT_ERROR_IF(log_mode, error_out,
                       "Cluster gtid domain is unknown. This is usually caused by the cluster never "
                       "having a primary server while MaxScale was running.");
    }
    else
    {
        gtid_domain_ok = true;
    }

    // Check that all slaves are using gtid-replication.
    bool gtid_ok = true;
    for (MariaDBServer* server : demotion_target->m_node.children)
    {
        auto sstatus = server->slave_connection_status(demotion_target);
        if (sstatus && sstatus->gtid_io_pos.empty())
        {
            PRINT_ERROR_IF(log_mode, error_out,
                           "The slave connection '%s' -> '%s' is not using gtid replication.",
                           server->name(), demotion_target->name());
            gtid_ok = false;
        }
    }

    return gtid_domain_ok && gtid_ok;
}

bool MariaDBMonitor::lock_status_is_ok() const
{
    return !(server_locks_in_use() && !is_cluster_owner());
}

/**
 * List slaves which should be redirected to the new master.
 *
 * @param old_master The server whose slaves are listed
 * @param ignored_slave A slave which should not be listed even if otherwise valid
 * @return A list of slaves to redirect
 */
ServerArray MariaDBMonitor::get_redirectables(const MariaDBServer* old_master,
                                              const MariaDBServer* ignored_slave)
{
    ServerArray redirectable_slaves;
    for (MariaDBServer* slave : old_master->m_node.children)
    {
        if (slave->is_usable() && slave != ignored_slave)
        {
            auto sstatus = slave->slave_connection_status(old_master);
            if (sstatus && !sstatus->gtid_io_pos.empty())
            {
                redirectable_slaves.push_back(slave);
            }
        }
    }
    return redirectable_slaves;
}

void MariaDBMonitor::delay_auto_cluster_ops(Log log)
{
    if (log == Log::ON && cluster_ops_configured())
    {
        const char DISABLING_AUTO_OPS[] = "Disabling automatic cluster operations for %li monitor ticks.";
        MXB_NOTICE(DISABLING_AUTO_OPS, m_settings.failcount);
    }
    // + 1 because the start of next tick subtracts 1.
    cluster_operation_disable_timer = m_settings.failcount + 1;
}

bool MariaDBMonitor::can_perform_cluster_ops()
{
    return !mxs::Config::get().passive.get() && cluster_operation_disable_timer <= 0
           && !m_cluster_modified && lock_status_is_ok();
}

/**
 * Guess the best gtid id by looking at promotion candidates.
 *
 * @param demotion_target Server being demoted
 * @param candidates List of candidates
 * @param id_missing_out If the selected domain id is not on all slaves, the number of missing slaves is
 * written here.
 * @return The guessed id. -1 if no candidates.
 */
int64_t MariaDBMonitor::guess_gtid_domain(MariaDBServer* demotion_target, const ServerArray& candidates,
                                          int* id_missing_out) const
{
    // Because gtid:s can be complicated, this guess is not an exact science. In most cases, however, the
    // correct answer is obvious. As a general rule, select the domain id which is in most candidates.
    using IdToCount = std::map<int64_t, int>;
    IdToCount id_to_count;      // How many of each domain id was found.
    for (const auto& cand : candidates)
    {
        auto& gtid_io_pos = cand->slave_connection_status(demotion_target)->gtid_io_pos;    // Must exist.
        GtidList::DomainList domains = gtid_io_pos.domains();
        for (auto domain : domains)
        {
            if (id_to_count.count(domain) == 0)
            {
                id_to_count[domain] = 1;
            }
            else
            {
                id_to_count[domain]++;
            }
        }
    }

    int64_t best_domain = GTID_DOMAIN_UNKNOWN;
    int best_count = 0;
    for (auto elem : id_to_count)
    {
        // In a tie, prefer the smaller domain id.
        if (elem.second > best_count || (elem.second == best_count && elem.first < best_domain))
        {
            best_domain = elem.first;
            best_count = elem.second;
        }
    }

    if (best_domain != GTID_DOMAIN_UNKNOWN && best_count < (int)candidates.size())
    {
        *id_missing_out = candidates.size() - best_count;
    }
    return best_domain;
}

MariaDBMonitor::SwitchoverParams::SwitchoverParams(ServerOperation promotion, ServerOperation demotion,
                                                   const GeneralOpData& general, SwitchoverType type)
    : promotion(move(promotion))
    , demotion(move(demotion))
    , general(general)
    , type(type)
{
}

MariaDBMonitor::FailoverParams::FailoverParams(ServerOperation promotion,
                                               const MariaDBServer* demotion_target,
                                               const GeneralOpData& general)
    : promotion(move(promotion))
    , demotion_target(demotion_target)
    , general(general)
{
}
