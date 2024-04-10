/*
 * Copyright (c) 2019 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
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
#pragma once

#include <maxscale/monitor.hh>

/**
 * This class contains internal monitor management functions that should not be exposed in the public
 * monitor class. It's a friend of MXS_MONITOR.
 */
class MonitorManager
{
public:

    /**
     * Creates a new monitor. Loads the module, calls constructor and configure, and adds monitor to the
     * global list.
     *
     * @param name          The configuration name of the monitor
     * @param module_name   The module name to load
     * @return              The newly created monitor, or NULL on error
     */
    static mxs::Monitor* create_monitor(const std::string& name, const std::string& module_name,
                                        mxs::ConfigParameters* params);
    static mxs::Monitor* create_monitor(const std::string& name, const std::string& module_name,
                                        json_t* params);
    /**
     * Mark monitor as deactivated. A deactivated monitor appears not to exist, as if it had been
     * destroyed. Any servers the monitor had are removed. The monitor should not be serialized after
     * this function.
     *
     * @param monitor Monitor to deactivate
     */
    static void deactivate_monitor(mxs::Monitor* monitor);

    /**
     * @brief Destroys all monitors. At this point all monitors should
     *        have been stopped.
     *
     * @attn Must only be called in single-thread context at system shutdown.
     */
    static void destroy_all_monitors();

    static void start_monitor(mxs::Monitor* monitor);

    /**
     * Stop a given monitor
     *
     * @param monitor The monitor to stop
     */
    static void                          stop_monitor(mxs::Monitor* monitor);
    static std::tuple<bool, std::string> soft_stop_monitor(mxs::Monitor* monitor);

    static void stop_all_monitors();
    static void start_all_monitors();

    static mxs::Monitor* find_monitor(const char* name);

    /**
     * Populate services with the servers of the monitors. Should be called at the end of configuration file
     * processing to ensure that services are notified of the servers a monitor has. During runtime, the
     * normal add/remove server functions do the notifying. TODO: If a service is created at runtime, is
     * it properly populated?
     */
    static void populate_services();

    /**
     * Get links to monitors that relate to a server.
     *
     * @param server Server to inspect
     * @param host   Hostname of this server
     * @param self   Self link to the relationship
     *
     * @return Array of monitor links or NULL if no relations exist
     */
    static json_t* monitor_relations_to_server(const SERVER* server,
                                               const std::string& host,
                                               const std::string& self);

    /**
     * Convert all monitors to JSON.
     *
     * @param host    Hostname of this server
     * @return JSON array containing all monitors
     */
    static json_t* monitor_list_to_json(const char* host);

    /**
     * Check if a server is being monitored and return the monitor.
     * @param server Server that is queried
     * @return The monitor watching this server, or NULL if not monitored
     */
    static mxs::Monitor* server_is_monitored(const SERVER* server);

    /**
     * @brief Persist monitor configuration into a stream
     *
     * This converts the static configuration of the monitor into an INI format file.
     *
     * @param monitor  Monitor to persist
     * @param filename Stream where the configuration is written
     *
     * @return The output stream
     */
    static std::ostream& monitor_persist(const mxs::Monitor* monitor, std::ostream& os);

    /**
     * Attempt to reconfigure a monitor. Should be only called from the admin thread.
     *
     * @param monitor    Monitor to reconfigure
     * @param parameters New parameters to apply
     * @return True if reconfiguration was successful
     */
    static bool reconfigure_monitor(mxs::Monitor* monitor, json_t* parameters);

    /**
     * Add server to monitor during runtime. Should only be called from the admin thread.
     *
     * @param mon Target monitor
     * @param server Server to add
     * @param error_out Error output
     * @return True on success
     */
    static bool add_server_to_monitor(mxs::Monitor* mon, SERVER* server, std::string* error_out);

    /**
     * Remove a server from a monitor during runtime. Should only be called from the admin thread.
     *
     * @param mon Target monitor
     * @param server Server to remove
     * @param error_out Error output
     * @return True on success
     */
    static bool remove_server_from_monitor(mxs::Monitor* mon, SERVER* server, std::string* error_out);

    /**
     * @brief Convert monitor to JSON
     *
     * @param monitor Monitor to convert
     * @param host    Hostname of this server
     *
     * @return JSON representation of the monitor
     */
    static json_t* monitor_to_json(const mxs::Monitor* monitor, const char* host);

    /**
     * Set a status bit in the server. If the server is monitored, only some bits can be modified,
     * and the modification goes through the monitor.
     *
     * @param bit           The bit to set for the server
     * @param errmsg_out    Error output
     * @return              True on success
     */
    static bool set_server_status(SERVER* srv, int bit, std::string* errmsg_out = NULL);

    /**
     * Clear a status bit in the server. If the server is monitored, only some bits can be modified,
     * and the modification goes through the monitor.
     *
     * @param bit           The bit to clear for the server
     * @param errmsg_out    Error output
     * @return              True on success
     */
    static bool clear_server_status(SERVER* srv, int bit, std::string* errmsg_out = nullptr);

    /**
     * Clear server status bit without waiting for monitor tick.
     */
    static bool clear_server_status_fast(SERVER* srv, int bit);

    static bool set_clear_server_status(SERVER* srv, int bit, mxs::Monitor::BitOp op,
                                        mxs::Monitor::WaitTick wait, std::string* errmsg_out = nullptr);

    static json_t* monitored_server_attributes_json(const SERVER* srv);

    /**
     * Waits until all running monitors have advanced one tick.
     *
     * @param time_limit Maximum time to wait
     * @return True if time limit was not reached
     */
    static bool wait_one_tick(mxb::Duration time_limit);

    using ConnDetails = std::vector<std::pair<SERVER*, mxs::MonitorServer::ConnectionSettings>>;

    /**
     * Get connection settings for each server
     *
     * With the help of MonitorServer::ping_or_connect_to_db(), the settings can be used to execute queries
     * without blocking the monitors or the MainWorker.
     *
     * @return The connection settings for all monitored servers
     */
    static ConnDetails get_connection_settings();

    /**
     * Connect to the servers and get JSON diagnostics from them
     *
     * This function connects to the servers and converts the results of diagnostic queries
     * (e.g. SHOW GLOBAL VARIABLES) into JSON. Since this function can block for a long time, it should be
     * executed asynchronously by the REST-API.
     *
     * @param servers The connection details from get_connection_settings()
     * @param host    The hostname of this MaxScale instance
     *
     * @return The results as JSON
     */
    static json_t* server_diagnostics(const ConnDetails& servers, const char* host);
};
