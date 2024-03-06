/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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
#pragma once
#include "mariadbmon_common.hh"
#include <functional>
#include <string>
#include <memory>
#include <mutex>
#include <maxbase/stopwatch.hh>
#include <maxbase/queryresult.hh>
#include <maxscale/monitor.hh>
#include "server_utils.hh"

class MariaDBServer;
// Server pointer array
typedef std::vector<MariaDBServer*> ServerArray;
using ServerType = SERVER::VersionInfo::Type;

/**
 * Data required for checking replication topology cycles and other graph algorithms. This data is mostly
 * used by the monitor object, as the data only makes sense in relation to other nodes.
 */
struct NodeData
{
    // Default values for index parameters
    static const int INDEX_NOT_VISITED = 0;
    static const int INDEX_FIRST = 1;
    // Default values for the cycle
    static const int CYCLE_NONE = 0;
    static const int CYCLE_FIRST = 1;
    // Default value for reach
    static const int REACH_UNKNOWN = -1;

    // Bookkeeping for graph searches. May be overwritten by multiple algorithms.
    int  index;         /* Marks the order in which this node was visited. */
    int  lowest_index;  /* The lowest index node this node has in its subtree. */
    bool in_stack;      /* Is this node currently is the search stack. */

    // Results from algorithm runs. Should only be overwritten when server data has been queried.
    int         cycle;          /* Which cycle is this node part of, if any. */
    int         reach;          /* How many servers replicate from this server or its children. */
    ServerArray parents;        /* Which nodes is this node replicating from. External masters excluded. */
    ServerArray parents_failed; /* Broken replication sources */
    ServerArray children;       /* Which nodes are replicating from this node. */
    ServerArray children_failed;/* Nodes with broken replication links. */

    std::vector<int64_t> external_masters;      /* Server id:s of external masters. */

    NodeData();

    /**
     * Reset result data to default values. Should be ran when starting an iteration.
     */
    void reset_results();

    /**
     * Reset index data. Should be ran before an algorithm run.
     */
    void reset_indexes();
};

/**
 * Monitor specific information about a server. Eventually, this will be the primary data structure handled
 * by the monitor. These are initialized in @c init_server_info.
 */
class MariaDBServer : public mxs::MariaServer
{
public:
    class SharedSettings;

    MariaDBServer(SERVER* server, int config_index,
                  const MonitorServer::SharedSettings& base_settings,
                  const MariaDBServer::SharedSettings& settings);

    class EventInfo
    {
    public:
        std::string name;       /**< Event name in <database.name> form */
        std::string definer;    /**< Definer of the event */
        std::string status;     /**< Status of the event */
        std::string charset;    /**< character_set_client-field */
        std::string collation;  /**< collation_connection-field */
    };

    enum class BinlogMode
    {
        BINLOG_ON,
        BINLOG_OFF
    };

    // Class which encapsulates server capabilities depending on its version.
    class Capabilities
    {
    public:
        bool basic_support {false};         // Is the server version supported by the monitor at all?
        bool gtid {false};                  // Supports MariaDB gtid? Required for failover etc.
        bool slave_status_all {false};      // Supports "show all slaves status"?
        bool max_statement_time {false};    // Supports max_statement_time?
        bool events {false};                // Supports event handling?
        bool read_only_admin {false};       // Implements read-only admin priv?
        bool separate_ro_admin {false};     // Is read-only admin separate from super?
    };

    // This class groups some miscellaneous replication related settings together.
    class ReplicationSettings
    {
    public:
        bool gtid_strict_mode = false;  /* Enable additional checks for replication */
        bool log_bin = false;           /* Is binary logging enabled? */
        bool log_slave_updates = false; /* Does the slave write replicated events to binlog? */
    };

    // Settings shared between the MariaDB-Monitor and the MariaDB-Servers.
    // These are only written to when configuring the monitor.
    class SharedSettings
    {
    public:
        // Required by cluster operations
        std::string replication_user;           /**< Username for CHANGE MASTER TO-commands */
        std::string replication_password;       /**< Password for CHANGE MASTER TO-commands */
        bool        replication_ssl {false};    /**< Set MASTER_SSL = 1 in CHANGE MASTER TO-commands */
        std::string replication_custom_opts;    /**< Custom CHANGE MASTER TO options */

        std::string promotion_sql_file;     /**< File with sql commands which are ran to a server being
                                             *  promoted. */
        std::string demotion_sql_file;      /**< File with sql commands which are ran to a server being
                                             *  demoted. */

        /* Should failover/switchover enable/disable any scheduled events on the servers during
         * promotion/demotion? */
        bool handle_event_scheduler {true};

        /** Should the server regularly update locks status. True if either lock mode is on. */
        bool server_locks_enabled {true};

        std::chrono::seconds switchover_timeout {0};    /* Switchover time limit */

        uint32_t master_conds;  /**< Master conditions */
        uint32_t slave_conds;   /**< Slave conditions */
    };

    /* What position this server has in the monitor config? Used for tiebreaking between servers. */
    int m_config_index = 0;

    Capabilities m_capabilities;                    /* Server capabilities */

    int64_t m_server_id = Gtid::SERVER_ID_UNKNOWN;  /* Value of @@server_id. Valid values are
                                                     * 32bit unsigned. */
    int64_t m_gtid_domain_id = GTID_DOMAIN_UNKNOWN; /* The value of gtid_domain_id, the domain which is used
                                                     * for new non-replicated events. */

    bool             m_read_only = false;   /* Value of @@read_only */
    GtidList         m_gtid_current_pos;    /* Gtid of latest event. */
    GtidList         m_gtid_binlog_pos;     /* Gtid of latest event written to binlog. */
    SlaveStatusArray m_slave_status;        /* Data returned from SHOW (ALL) SLAVE(S) STATUS */
    SlaveStatusArray m_old_slave_status;    /* Data from the previous loop */
    NodeData         m_node;                /* Replication topology data */

    /* Replication lag of the server. Used during calculation so that the actual SERVER struct is
     * only written to once. */
    int64_t m_replication_lag = mxs::Target::RLAG_UNDEFINED;

    /* Has anything that could affect replication topology changed this iteration?
     * Causes: server id, slave connections, read-only. */
    bool m_topology_changed = true;

    // If true, warn when querying of events fails
    bool m_warn_event_handling = true;

    /* Miscellaneous replication related settings. These are not normally queried from the server, call
     * 'update_replication_settings' before use. */
    ReplicationSettings m_rpl_settings;

    EventNameSet m_enabled_events;              /* Enabled scheduled events */

    /**
     * Print server information to a json object.
     *
     * @return Json diagnostics object
     */
    json_t* to_json() const;

    void update_server(bool time_to_update_disk_space, bool first_tick, bool is_topology_master);

    std::string print_changed_slave_connections();

    /**
     * Query this server.
     */
    void monitor_server();

    /**
     * Update server version. This method should be called after (re)connecting to a
     * backend. Calling this every monitoring loop is overkill.
     */
    void update_server_version();

    /**
     * Calculate how many events are left in the relay log of the slave connection.
     *
     * @param slave_conn The slave connection to calculate for
     * @return Number of events in relay log. Always  0 or greater.
     */
    uint64_t relay_log_events(const SlaveStatus& slave_conn) const;

    /**
     * execute_cmd_ex with query retry ON.
     */
    bool execute_cmd(const std::string& cmd, std::string* errmsg_out = nullptr);

    /**
     * execute_cmd_ex with query retry OFF.
     */
    bool execute_cmd_no_retry(const std::string& cmd, const std::string& masked_cmd,
                              std::string* errmsg_out = nullptr, unsigned int* errno_out = nullptr);

    /**
     * Update server slave connection information.
     *
     * @param gtid_domain Which gtid domain should be parsed.
     * @param errmsg_out Where to store an error message if query fails. Can be null.
     * @return True on success
     */
    bool do_show_slave_status(std::string* errmsg_out = nullptr);

    /**
     * Query gtid_current_pos and gtid_binlog_pos and save the values to the server.
     *
     * @param errmsg_out Where to store an error message if query fails. Can be null.
     * @return True if query succeeded
     */
    bool update_gtids(std::string* errmsg_out = nullptr);

    /**
     * Query a few miscellaneous replication settings.
     *
     * @param error_out Query error output
     * @return True on success
     */
    bool update_replication_settings(std::string* error_out = nullptr);

    /**
     * Query and save server_id, read_only and (if 10.X) gtid_domain_id.
     *
     * @param errmsg_out Where to store an error message if query fails. Can be null.
     * @return True on success.
     */
    bool read_server_variables(std::string* errmsg_out = nullptr);

    /**
     * Print warnings if gtid_strict_mode or log_slave_updates is off. Does not query the server,
     * so 'update_replication_settings' should have been called recently to update the values.
     */
    void warn_replication_settings() const;

    /**
     * Wait until server catches up to demotion target. Only considers gtid domains common
     * to this server and the target. The gtid compared to on the demotion target is 'gtid_binlog_pos'.
     * It is not updated during this method.
     *
     * The gtid used for comparison on this server is 'gtid_binlog_pos' if this server has both 'log_bin'
     * and 'log_slave_updates' on, and 'gtid_current_pos' otherwise. This server is updated during the
     * method.
     *
     * @return True, if target server gtid was reached within allotted time
     */
    bool catchup_to_master(GeneralOpData& op, const GtidList& target);

    /**
     * Find slave connection to the target server. If the IO thread is trying to connect
     * ("Connecting"), the connection is only accepted if the 'Master_Server_Id' is known to be correct.
     * If the IO or the SQL thread is stopped, the connection is not returned.
     *
     * @param target Immediate master or relay server
     * @return The slave status info of the slave thread, or NULL if not found or not accepted
     */
    const SlaveStatus* slave_connection_status(const MariaDBServer* target) const;

    /**
     * Find slave connection to the target server. Only considers host and port when selecting
     * the connection. A matching connection is accepted even if its IO or SQL thread is stopped.
     *
     * @param target Immediate master or relay server
     * @return The slave status info of the slave thread, or NULL if not found
     */
    const SlaveStatus* slave_connection_status_host_port(const MariaDBServer* target) const;

    /**
     * Checks if this server can replicate from master. Only considers gtid:s and only detects obvious
     * errors. The non-detected errors will mostly be detected once the slave tries to start replicating.
     * Before calling this, update the gtid:s of the master so that the the gtid:s of the master are more
     * recent than those of this server.
     *
     * @param master_info Master server
     * @param reason_out Details the reason for a negative result
     * @return True if slave can replicate from master
     */
    bool can_replicate_from(MariaDBServer* master, std::string* reason_out) const;

    /**
     * Check if the server can be demoted by switchover.
     *
     * @param reason_out Output explaining why server cannot be demoted
     * @return True if server can be demoted
     */
    bool can_be_demoted_switchover(SwitchoverType type, std::string* reason_out);

    enum class FailoverType
    {
        SAFE,
        RISKY
    };
    /**
     * Check if the server can be demoted by failover.
     *
     * @param failover_mode Is failover with guessed gtid domain allowed
     * @param reason_out Output explaining why server cannot be demoted
     * @return True if server can be demoted
     */
    bool can_be_demoted_failover(FailoverType failover_mode, std::string* reason_out) const;

    /**
     * Check if the server can be promoted by switchover or failover.
     *
     * @param op Switchover or failover
     * @param demotion_target The server this should be promoted to
     * @param reason_out Output for the reason server cannot be promoted
     * @return True, if suggested new master is a viable promotion candidate
     */
    bool can_be_promoted(OperationType op, const MariaDBServer* demotion_target, std::string* reason_out);

    /**
     * Read the file contents and send them as sql queries to the server. Any data
     * returned by the queries is discarded.
     *
     * @param server Server to send queries to
     * @param path Text file path.
     * @param error_out Error output
     * @return True if file was read and all commands were completed successfully
     */
    bool run_sql_from_file(const std::string& path, mxb::Json& error_out);

    /**
     * Enable any "SLAVESIDE_DISABLED" or "DISABLED events. Event scheduler is not touched. Only events
     * with names matching an element in the event_names set are enabled.
     *
     * @param binlog_mode If OFF, binlog event creation is disabled for the session during method execution.
     * @param event_names Names of events that should be enabled
     * @param error_out Error output
     * @return True if all SLAVESIDE_DISABLED events were enabled
     */
    bool enable_events(BinlogMode binlog_mode, const EventNameSet& event_names, mxb::Json& error_out);

    /**
     * Disable any "ENABLED" events. Event scheduler is not touched.
     *
     * @param binlog_mode If OFF, binlog event creation is disabled for the session during method execution.
     * @param error_out Error output
     * @return True if all ENABLED events were disabled
     */
    bool disable_events(BinlogMode binlog_mode, mxb::Json& error_out);

    /**
     * Stop and delete all slave connections.
     *
     * @param error_out Error output
     * @return True if successful. If false, some connections may have been successfully deleted.
     */
    bool reset_all_slave_conns(mxb::Json& error_out);

    /**
     * Promote this server to take role of demotion target. Remove slave connections from this server.
     * If target is/was a master, set read-only to OFF. Copy slave connections from target.
     *
     * @param op Cluster operation descriptor
     * @return True if successful
     */
    bool promote(GeneralOpData& op, ServerOperation& promotion, OperationType type,
                 const MariaDBServer* demotion_target);

    /**
     * Demote this server. Removes all slave connections. If server was master, sets read_only.
     *
     * @param general General operation data
     * @param demotion Demotion-specific settings
     * @param type Which specific operation is this part of
     * @return True if successful
     */
    bool demote(GeneralOpData& general, ServerOperation& demotion, OperationType type);

    /**
     * Redirect the slave connection going to old master to replicate from new master.
     *
     * @param op Operation descriptor
     * @param conn_settings The connection which is redirected
     * @param new_master The new master for the redirected connection
     * @return True on success
     */
    bool redirect_existing_slave_conn(GeneralOpData& op, const SlaveStatus::Settings& conn_settings,
                                      const MariaDBServer* new_master);

    /**
     * Copy slave connections to this server. This is usually needed during switchover promotion and on
     * the demoted server. It is assumed that all slave connections of this server have
     * been stopped & removed so there will be no conflicts with existing connections.
     * A special rule is applied to a connection which points to this server itself: that connection
     * is directed towards the 'replacement'. This is required to properly handle connections between
     * the promotion and demotion targets.
     *
     * @param op Operation descriptor
     * @params conns_to_copy The connections to add to the server
     * @params replacement Which server should replace this server as replication target
     * @return True on success
     */
    bool copy_slave_conns(GeneralOpData& op, const SlaveStatusArray& conns_to_copy,
                          const MariaDBServer* replacement, SlaveStatus::Settings::GtidMode gtid_mode);

    /**
     * Remove slave connections from this server.
     *
     * @param op Operation descriptor
     * @params conns_to_copy The connections to remove from the server
     * @return True on success
     */
    bool remove_slave_conns(GeneralOpData& op, const SlaveStatusArray& conns_to_remove);

    /**
     * Create a new slave connection on the server and start it.
     *
     * @param op Operation descriptor
     * @param conn_settings Existing connection to emulate
     * @return True on success
     */
    bool create_start_slave(GeneralOpData& op, const SlaveStatus::Settings& conn_settings);

    /**
     * Kill the connections of any super-users except for the monitor itself.
     *
     * @param op Operation descriptor
     * @return True on success. If super-users cannot be queried because of insufficient privileges,
     * return true as it means the user does not want this feature.
     */
    bool kick_out_super_users(GeneralOpData& op);

    /**
     * Is binary log on? 'update_replication_settings' should be ran before this function to query the data.
     *
     * @return True if server has binary log enabled
     */
    bool binlog_on() const;

    /**
     * Check if server is a running master.
     *
     * @return True if server is a master
     */
    bool is_master() const;

    /**
     * Check if server is a running slave.
     *
     * @return True if server is a slave
     */
    bool is_slave() const;

    /**
     * Check if server is running and not in maintenance.
     *
     * @return True if server is usable
     */
    bool is_usable() const;

    /**
     * Check if server is running.
     *
     * @return True if server is running
     */
    bool is_running() const;

    /**
     * Check if server is down.
     *
     * @return True if server is down
     */
    bool is_down() const;

    /**
     * Check if server is in maintenance.
     */
    bool is_in_maintenance() const;

    /**
     * Is the server a relay master.
     */
    bool is_relay_master() const;

    /**
     * Is the server low on disk space?
     */
    bool is_low_on_disk_space() const;

    /**
     * Getter for m_read_only.
     *
     * @return True if server is in read_only mode
     */
    bool is_read_only() const;

    /**
     * Returns the server name.
     *
     * @return Server unique name
     */
    const char* name() const;

    /**
     * Clear server pending status flags.
     *
     * @param bits Which flags to clear
     */
    void clear_status(uint64_t bits);

    /**
     * Set server pending status flags.
     *
     * @param bits Which flags to set
     */
    void set_status(uint64_t bits);

    void update_locks_status();

    enum class LockType
    {
        SERVER,
        MASTER,
    };
    bool release_lock(LockType lock_type);
    int  release_all_locks();
    bool get_lock(LockType lock_type);
    bool lock_owned(LockType lock_type);

    ServerLock masterlock_status() const;
    ServerLock serverlock_status() const;
    ServerLock lock_status(LockType lock_type) const;

    bool marked_as_master(std::string* why_not = nullptr) const;

    SERVER::VersionInfo::Type server_type() const;

    /**
     * Update server replication lag state in relation to the limit. If state changes, writes an event
     * to the custom events list.
     *
     * @param limit Replication lag limit, in seconds. Assumed >= 0.
     */
    void update_rlag_state(int64_t limit);

    const EventList& new_custom_events() const override;

    bool relax_connector_timeouts(std::chrono::seconds op_timeout);
    void restore_connector_timeouts();

private:
    using EventManipulator = std::function<void (const EventInfo& event, mxb::Json& error_out)>;
    using EventStatusMapper = std::function<std::string (const EventInfo& event)>;

    enum class StopMode
    {
        STOP_ONLY,
        RESET,
        RESET_ALL
    };
    enum class QueryRetryMode
    {
        ENABLED,
        DISABLED
    };
    enum class ReadOnlySetting
    {
        ENABLE,
        DISABLE
    };

    /* Protects array-like fields from concurrent access. This is only required for fields which can be
     * read from another thread while the monitor is running. In practice, these are fields read during
     * diagnostics-methods. Reading inside monitor thread does not need to be mutexed, as outside threads
     * only read the values. */
    mutable std::mutex m_arraylock;

    const SharedSettings& m_settings;       /* Settings required for various operations */

    ServerLock m_serverlock;        /* Server lock status */
    ServerLock m_masterlock;        /* Master lock status */

    bool m_print_update_errormsg {true};    /* Should an update error be printed? */

    /* Replication lag state compared to the MariaDBMonitor-specific replication lag script event limit. */
    mxs::RLagState m_rlag_state {mxs::RLagState::NONE};

    EventList m_new_events;
    MYSQL*    m_old_conn {nullptr}; /**< Stored old connection for duration of switchover. */

    bool               update_slave_status(std::string* errmsg_out = nullptr);
    bool               sstatus_array_topology_equal(const SlaveStatusArray& new_slave_status) const;
    const SlaveStatus* sstatus_find_previous_row(const SlaveStatus& new_row, size_t guess);

    bool stop_slave_conn(const std::string& conn_name, StopMode mode, maxbase::Duration time_limit,
                         mxb::Json& error_out);

    bool execute_cmd_ex(const std::string& cmd, const std::string& masked_cmd, QueryRetryMode mode,
                        std::string* errmsg_out = nullptr, unsigned int* errno_out = nullptr);
    bool execute_cmd_time_limit(const std::string& cmd, const std::string& masked_cmd,
                                maxbase::Duration time_limit,
                                std::string* errmsg_out, unsigned int* errnum_out);
    bool execute_cmd_time_limit(const std::string& cmd, maxbase::Duration time_limit,
                                std::string* errmsg_out, unsigned int* errnum_out = nullptr);

    bool set_read_only(ReadOnlySetting value, maxbase::Duration time_limit, mxb::Json& error_out);

    bool merge_slave_conns(GeneralOpData& op, const SlaveStatusArray& conns_to_merge,
                           SlaveStatus::Settings::GtidMode gtid_mode);
    bool demote_master(GeneralOpData& general, OperationType type);
    bool check_gtid_stable(mxb::Json& error_out);

    struct ChangeMasterCmd
    {
        std::string real_cmd;   /**< Real command sent to server */
        std::string masked_cmd; /**< Version with masked credentials */
    };
    ChangeMasterCmd generate_change_master_cmd(const SlaveStatus::Settings& conn_settings);

    bool update_enabled_events();

    bool alter_events(BinlogMode binlog_mode, const EventStatusMapper& mapper, mxb::Json& error_out);
    void warn_event_scheduler();
    bool events_foreach(EventManipulator& func, mxb::Json& error_out);
    bool alter_event(const EventInfo& event, const std::string& target_status, mxb::Json& error_out);

    int64_t conn_id() const;
    void    clear_locks_info();

    struct ConnInfo
    {
        int64_t     conn_id {-1};
        std::string username;
    };
    std::tuple<bool, std::vector<ConnInfo>> get_super_user_conns(mxb::Json& error_out);

    const std::string& permission_test_query() const override;
};
