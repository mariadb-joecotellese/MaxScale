/*
 * Copyright (c) 2019 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include <maxscale/ccdefs.hh>

#include <fcntl.h>
#include <maxbase/format.hh>
#include <maxscale/json_api.hh>
#include <maxscale/paths.hh>
#include <maxscale/protocol/mariadb/maxscale.hh>

#include "internal/config.hh"
#include "internal/monitormanager.hh"
#include "internal/modules.hh"

using maxscale::Monitor;
using Guard = std::lock_guard<std::mutex>;
using std::string;
using mxb::string_printf;

namespace
{

class ThisUnit
{
public:

    /**
     * Call a function on every monitor in the global monitor list.
     *
     * @param apply The function to apply. If the function returns false, iteration is discontinued.
     */
    void foreach_monitor(const std::function<bool(Monitor*)>& apply)
    {
        Guard guard(m_all_monitors_lock);
        for (Monitor* monitor : m_all_monitors)
        {
            if (!apply(monitor))
            {
                break;
            }
        }
    }

    /**
     * Clear the internal list and return previous contents.
     *
     * @return Contents before clearing
     */
    std::vector<Monitor*> clear()
    {
        Guard guard(m_all_monitors_lock);
        m_all_monitors.insert(m_all_monitors.end(), m_deact_monitors.begin(), m_deact_monitors.end());
        m_deact_monitors.clear();
        return std::move(m_all_monitors);
    }

    void insert_front(Monitor* monitor)
    {
        Guard guard(m_all_monitors_lock);
        m_all_monitors.insert(m_all_monitors.begin(), monitor);
    }

    void move_to_deactivated_list(Monitor* monitor)
    {
        Guard guard(m_all_monitors_lock);
        auto iter = std::find(m_all_monitors.begin(), m_all_monitors.end(), monitor);
        mxb_assert(iter != m_all_monitors.end());
        m_all_monitors.erase(iter);
        m_deact_monitors.push_back(monitor);
    }

    MonitorManager::ConnDetails get_connection_settings()
    {
        MonitorManager::ConnDetails servers;
        Guard guard(m_all_monitors_lock);

        for (auto* m : m_all_monitors)
        {
            for (auto* s : m->active_routing_servers())
            {
                servers.emplace_back(s, m->conn_settings());
            }
        }

        return servers;
    }

private:
    std::mutex            m_all_monitors_lock;  /**< Protects access to arrays */
    std::vector<Monitor*> m_all_monitors;       /**< Global list of monitors, in configuration file order */
    std::vector<Monitor*> m_deact_monitors;     /**< Deactivated monitors. TODO: delete monitors */
};

ThisUnit this_unit;

const char RECONFIG_FAILED[] = "Monitor reconfiguration failed when %s. Check log for more details.";
}

template<class Params, class Unknown>
Monitor* do_create_monitor(const string& name, const string& module_name, Params params, Unknown* unknown)
{
    mxb_assert(Monitor::is_main_worker());

    if (!Monitor::specification()->validate(params, unknown))
    {
        return nullptr;
    }

    Monitor* new_monitor = nullptr;
    const MXS_MODULE* module = get_module(module_name, mxs::ModuleType::MONITOR);
    if (module)
    {
        mxb_assert(module->specification);

        if (module->specification && !module->specification->validate(params))
        {
            return nullptr;
        }

        MXS_MONITOR_API* api = (MXS_MONITOR_API*)module->module_object;
        new_monitor = api->createInstance(name, module_name);
        if (new_monitor)
        {
            if (new_monitor->base_configuration().configure(params, unknown)
                && new_monitor->configuration().configure(params))
            {
                this_unit.insert_front(new_monitor);
            }
            else
            {
                // Deactivate the monitor first. This triggers the removal of the servers from services that
                // might use the monitor. This can't be done in the destructor as the monitor will be
                // partially deleted and is no longer valid at that point.
                new_monitor->deactivate();
                delete new_monitor;
                new_monitor = nullptr;
            }
        }
        else
        {
            MXB_ERROR("Unable to create monitor instance for '%s', using module '%s'.",
                      name.c_str(), module_name.c_str());
        }
    }
    else
    {
        MXB_ERROR("Unable to load library file for monitor '%s'.", name.c_str());
    }
    return new_monitor;
}


Monitor* MonitorManager::create_monitor(const string& name, const string& module_name,
                                        mxs::ConfigParameters* params)

{
    mxs::ConfigParameters unknown;
    return do_create_monitor(name, module_name, *params, &unknown);
}

Monitor* MonitorManager::create_monitor(const string& name, const string& module_name, json_t* params)
{
    std::set<std::string> unknown;
    return do_create_monitor(name, module_name, params, &unknown);
}

bool MonitorManager::wait_one_tick()
{
    mxb_assert(Monitor::is_main_worker());
    std::map<Monitor*, long> tick_counts;

    // Get tick values for all monitors and instruct monitors to skip normal waiting.
    this_unit.foreach_monitor(
        [&tick_counts](Monitor* mon) {
            if (mon->is_running())
            {
                tick_counts[mon] = mon->ticks_started();
                mon->request_immediate_tick();
            }
            return true;
        });

    bool wait_success = true;
    auto wait_start = maxbase::Clock::now();
    // Due to immediate tick, monitors should generally run within 100ms. Slow-running operations on
    // backends may cause delay.
    auto time_limit = mxb::from_secs(10);

    auto sleep_time = std::chrono::milliseconds(30);
    std::this_thread::sleep_for(sleep_time);

    // Wait for all running monitors to advance at least one tick.
    this_unit.foreach_monitor(
        [&](Monitor* mon) {
            if (mon->is_running())
            {
                // Monitors may (in theory) have been modified between the two 'foreach_monitor'-calls.
                // Check if entry exists.
                auto it = tick_counts.find(mon);
                if (it != tick_counts.end())
                {
                    auto ticks_started_count = it->second;
                    while (true)
                    {
                        if (mon->ticks_complete() > ticks_started_count)
                        {
                            break;
                        }
                        else if (maxbase::Clock::now() - wait_start > time_limit)
                        {
                            wait_success = false;
                            break;
                        }
                        else
                        {
                            // Not ideal to sleep while holding a mutex.
                            mon->request_immediate_tick();
                            std::this_thread::sleep_for(sleep_time);
                        }
                    }
                }
            }
            return true;
        });

    return wait_success;
}

void MonitorManager::destroy_all_monitors()
{
    mxb_assert(Monitor::is_main_worker());
    auto monitors = this_unit.clear();
    for (auto monitor : monitors)
    {
        mxb_assert(!monitor->is_running());
        monitor->deactivate();
        delete monitor;
    }
}

void MonitorManager::start_monitor(Monitor* monitor)
{
    mxb_assert(Monitor::is_main_worker());

    // Only start the monitor if it's stopped.
    if (!monitor->is_running())
    {
        if (!monitor->start())
        {
            MXB_ERROR("Failed to start monitor '%s'.", monitor->name());
        }
    }
}

void MonitorManager::populate_services()
{
    mxb_assert(Monitor::is_main_worker());
    this_unit.foreach_monitor(
        [](Monitor* pMonitor) -> bool {
        pMonitor->active_servers_updated();
        return true;
    });
}

/**
 * Start all monitors
 */
void MonitorManager::start_all_monitors()
{
    mxb_assert(Monitor::is_main_worker());
    this_unit.foreach_monitor(
        [](Monitor* monitor) {
            MonitorManager::start_monitor(monitor);
            return true;
        });
}

void MonitorManager::stop_monitor(Monitor* monitor)
{
    mxb_assert(Monitor::is_main_worker());

    /** Only stop the monitor if it is running */
    if (monitor->is_running())
    {
        monitor->stop();
    }
}

std::tuple<bool, std::string> MonitorManager::soft_stop_monitor(mxs::Monitor* monitor)
{
    mxb_assert(Monitor::is_main_worker());
    std::tuple<bool, std::string> rval = {true, ""};
    if (monitor->is_running())
    {
        rval = monitor->soft_stop();
    }
    return rval;
}

void MonitorManager::deactivate_monitor(Monitor* monitor)
{
    mxb_assert(Monitor::is_main_worker());
    // This cannot be done with configure(), since other, module-specific config settings may depend on the
    // "servers"-setting of the base monitor.
    monitor->deactivate();
    this_unit.move_to_deactivated_list(monitor);
}

/**
 * Shutdown all running monitors
 */
void MonitorManager::stop_all_monitors()
{
    mxb_assert(Monitor::is_main_worker());
    this_unit.foreach_monitor(
        [](Monitor* monitor) {
            MonitorManager::stop_monitor(monitor);
            return true;
        });
}

/**
 * Find a monitor by name
 *
 * @param       name    The name of the monitor
 * @return      Pointer to the monitor or NULL
 */
Monitor* MonitorManager::find_monitor(const char* name)
{
    Monitor* rval = nullptr;
    this_unit.foreach_monitor(
        [&rval, name](Monitor* ptr) {
            if (ptr->m_name == name)
            {
                rval = ptr;
            }
            return rval == nullptr;
        });
    return rval;
}

Monitor* MonitorManager::server_is_monitored(const SERVER* server)
{
    Monitor* rval = nullptr;
    auto mon_name = Monitor::get_server_monitor(server);
    if (!mon_name.empty())
    {
        rval = find_monitor(mon_name.c_str());
        mxb_assert(rval);
    }
    return rval;
}

std::ostream& MonitorManager::monitor_persist(const Monitor* monitor, std::ostream& os)
{
    auto mon = const_cast<Monitor*>(monitor);
    mon->configuration().persist(os);
    mon->base_configuration().persist_append(os, {CN_TYPE});
    return os;
}

bool MonitorManager::reconfigure_monitor(mxs::Monitor* monitor, json_t* parameters)
{
    mxb_assert(Monitor::is_main_worker());
    // Backup monitor parameters in case configure fails.
    auto orig = monitor->parameters();
    bool success = false;
    bool ok_to_configure = false;
    // Stop/start monitor if it's currently running. If monitor was stopped already, this is likely
    // managed by the caller.
    bool was_running = monitor->is_running();
    if (was_running)
    {
        auto [stopped, errmsg] = monitor->soft_stop();
        if (stopped)
        {
            ok_to_configure = true;
        }
        else
        {
            MXB_ERROR("Reconfiguration of monitor '%s' failed because monitor cannot be safely stopped. %s",
                      monitor->name(), errmsg.c_str());
        }
    }
    else
    {
        ok_to_configure = true;
    }

    if (ok_to_configure)
    {
        std::set<std::string> unknown;
        auto& base = monitor->base_configuration();
        auto& mod = monitor->configuration();

        if (base.validate(parameters, &unknown) && mod.validate(parameters))
        {
            if (base.configure(parameters, &unknown))
            {
                success = mod.configure(parameters);
            }
        }

        // TODO: If the reconfiguration fails, the old parameters are not restored. Previously the monitor was
        // reconfigured with the old parameters if the new one failed to be processed. Either make sure the
        // configuration succeeds or add a generic way to roll back a partial change.
        if (was_running && !monitor->start())
        {
            MXB_ERROR("Reconfiguration of monitor '%s' failed because monitor did not start.",
                      monitor->name());
        }
    }
    return success;
}

json_t* MonitorManager::monitor_to_json(const Monitor* monitor, const char* host)
{
    string self = MXS_JSON_API_MONITORS;
    self += monitor->m_name;
    return mxs_json_resource(host, self.c_str(), monitor->to_json(host));
}

json_t* MonitorManager::monitored_server_attributes_json(const SERVER* srv)
{
    mxb_assert(Monitor::is_main_worker());
    Monitor* mon = server_is_monitored(srv);
    if (mon)
    {
        return mon->monitored_server_json_attributes(srv);
    }
    return nullptr;
}

json_t* MonitorManager::monitor_list_to_json(const char* host)
{
    json_t* rval = json_array();
    this_unit.foreach_monitor(
        [rval, host](Monitor* mon) {
            json_t* json = mon->to_json(host);
            if (json)
            {
                json_array_append_new(rval, json);
            }
            return true;
        });

    return mxs_json_resource(host, MXS_JSON_API_MONITORS, rval);
}

json_t* MonitorManager::monitor_relations_to_server(const SERVER* server,
                                                    const std::string& host,
                                                    const std::string& self)
{
    mxb_assert(Monitor::is_main_worker());
    json_t* rel = nullptr;

    string mon_name = Monitor::get_server_monitor(server);
    if (!mon_name.empty())
    {
        rel = mxs_json_relationship(host, self, MXS_JSON_API_MONITORS);
        mxs_json_add_relation(rel, mon_name.c_str(), CN_MONITORS);
    }

    return rel;
}

bool MonitorManager::set_server_status(SERVER* srv, int bit, string* errmsg_out)
{
    return set_clear_server_status(srv, bit, Monitor::BitOp::SET, errmsg_out);
}

bool MonitorManager::clear_server_status(SERVER* srv, int bit, string* errmsg_out)
{
    return set_clear_server_status(srv, bit, Monitor::BitOp::CLEAR, errmsg_out);
}

bool MonitorManager::set_clear_server_status(SERVER* srv, int bit, mxs::Monitor::BitOp op,
                                             std::string* errmsg_out)
{
    mxb_assert(Monitor::is_main_worker());
    bool written;
    Monitor* mon = MonitorManager::server_is_monitored(srv);
    if (mon)
    {
        written = mon->set_clear_server_status(srv, bit, op, errmsg_out);
    }
    else
    {
        // Set/clear the bit directly
        if (op == Monitor::BitOp::SET)
        {
            srv->set_status(bit);
        }
        else
        {
            srv->clear_status(bit);
        }
        written = true;
    }
    return written;
}

bool MonitorManager::add_server_to_monitor(mxs::Monitor* mon, SERVER* server, std::string* error_out)
{
    mxb_assert(Monitor::is_main_worker());
    bool success = false;
    string server_monitor = Monitor::get_server_monitor(server);
    if (!server_monitor.empty())
    {
        // Error, server is already monitored.
        string error = string_printf("Server '%s' is already monitored by '%s', ",
                                     server->name(), server_monitor.c_str());
        error += (server_monitor == mon->name()) ? "cannot add again to the same monitor." :
            "cannot add to another monitor.";
        *error_out = error;
    }
    else
    {
        mxb::Json json(mon->parameters_to_json(), mxb::Json::RefType::STEAL);

        auto servers = mon->configured_servers();
        servers.push_back(server);
        json.set_string(CN_SERVERS, mxb::transform_join(servers, std::mem_fn(&SERVER::name), ","));
        json.remove_nulls();

        success = reconfigure_monitor(mon, json.get_json());
        if (!success)
        {
            *error_out = string_printf(RECONFIG_FAILED, "adding a server");
        }
    }
    return success;
}

bool MonitorManager::remove_server_from_monitor(mxs::Monitor* mon, SERVER* server, std::string* error_out)
{
    mxb_assert(Monitor::is_main_worker());
    bool success = false;
    string server_monitor = Monitor::get_server_monitor(server);
    if (server_monitor != mon->name())
    {
        // Error, server is not monitored by given monitor.
        string error;
        if (server_monitor.empty())
        {
            error = string_printf("Server '%s' is not monitored by any monitor, ", server->name());
        }
        else
        {
            error = string_printf("Server '%s' is monitored by '%s', ",
                                  server->name(), server_monitor.c_str());
        }
        error += string_printf("cannot remove it from '%s'.", mon->name());
        *error_out = error;
    }
    else
    {
        mxb::Json json(mon->parameters_to_json(), mxb::Json::RefType::STEAL);

        auto servers = mon->configured_servers();
        servers.erase(std::remove(servers.begin(), servers.end(), server), servers.end());
        json.set_string(CN_SERVERS, mxb::transform_join(servers, std::mem_fn(&SERVER::name), ","));
        json.remove_nulls();

        success = MonitorManager::reconfigure_monitor(mon, json.get_json());
        if (!success)
        {
            *error_out = string_printf(RECONFIG_FAILED, "removing a server");
        }
    }

    return success;
}

// static
MonitorManager::ConnDetails MonitorManager::get_connection_settings()
{
    return this_unit.get_connection_settings();
}

// static
json_t* MonitorManager::server_diagnostics(const MonitorManager::ConnDetails& servers, const char* host)
{
    json_t* attr = json_object();

    for (const auto& kv : servers)
    {
        MYSQL* conn = nullptr;
        std::string err;
        auto result = mxs::MariaServer::ping_or_connect_to_db(kv.second, *kv.first, &conn, &err);

        if (result == mxs::MariaServer::ConnectResult::NEWCONN_OK)
        {
            auto json_query = [&](auto sql, int name_idx, int val_idx){
                unsigned int errnum;

                if (auto r = mxs::execute_query(conn, sql, &err, &errnum))
                {
                    json_t* var = json_object();

                    while (r->next_row())
                    {
                        json_object_set_new(var, r->get_string(name_idx).c_str(),
                                            json_string(r->get_string(val_idx).c_str()));
                    }

                    return var;
                }
                else
                {
                    return json_pack("{s: s}", "error", err.c_str());
                }
            };

            json_t* obj = json_object();
            json_object_set_new(obj, "global_variables", json_query("SHOW GLOBAL VARIABLES", 0, 1));
            json_object_set_new(obj, "global_status", json_query("SHOW GLOBAL STATUS", 0, 1));
            json_object_set_new(obj, "engine_status", json_query("SHOW ENGINE INNODB STATUS", 0, 2));
            json_object_set_new(attr, kv.first->name(), obj);

            mysql_close(conn);
        }
        else
        {
            json_object_set_new(attr, kv.first->name(), json_pack("{s: s}", "error", err.c_str()));
        }
    }

    json_t* rval = json_object();
    json_object_set_new(rval, CN_ID, json_string("server_diagnostics"));
    json_object_set_new(rval, CN_TYPE, json_string("server_diagnostics"));
    json_object_set_new(rval, CN_ATTRIBUTES, attr);
    return mxs_json_resource(host, MXS_JSON_API_SERVER_DIAG, rval);
}
