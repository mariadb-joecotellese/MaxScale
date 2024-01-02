/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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
#pragma once

#include <maxscale/ccdefs.hh>

#include <ctime>

#include <maxbase/jansson.hh>
#include <maxbase/statistics.hh>
#include <maxscale/config2.hh>
#include <maxscale/protocol.hh>
#include <maxscale/filter.hh>
#include <maxscale/target.hh>
#include <maxscale/workerlocal.hh>
#include <maxscale/router.hh>
#include <maxscale/monitor.hh>

class SERVER;
struct users;

#define MAX_SERVICE_USER_LEN     1024
#define MAX_SERVICE_PASSWORD_LEN 1024
#define MAX_SERVICE_VERSION_LEN  1024

/** Value of service timeout if timeout checks are disabled */
#define SERVICE_NO_SESSION_TIMEOUT 0

/**
 * Parameters that are automatically detected but can also be configured by the
 * user are initially set to this value.
 */
#define SERVICE_PARAM_UNINIT -1

/* Refresh rate limits for load users from database */
#define USERS_REFRESH_TIME_DEFAULT 30   /* Allowed time interval (in seconds) after last update*/

/** Default timeout values used by the connections which fetch user authentication data */
#define DEFAULT_AUTH_CONNECT_TIMEOUT 10
#define DEFAULT_AUTH_READ_TIMEOUT    10
#define DEFAULT_AUTH_WRITE_TIMEOUT   10

enum service_version_which_t
{
    SERVICE_VERSION_ANY,    /*< Any version of the servers of a service. */
    SERVICE_VERSION_MIN,    /*< The minimum version. */
    SERVICE_VERSION_MAX,    /*< The maximum version. */
};

/**
 * Defines a service within the gateway.
 *
 * A service is a combination of a set of backend servers, a routing mechanism
 * and a set of client side protocol/port pairs used to listen for new connections
 * to the service.
 */
class SERVICE : public mxs::Target
{
public:

    enum class State
    {
        ALLOC,      /**< The service has been allocated */
        STARTED,    /**< The service has been started */
        FAILED,     /**< The service failed to start */
        STOPPED,    /**< The service has been stopped */
    };

    struct Config : public mxs::config::Configuration
    {
        Config(SERVICE* service);

        struct Values
        {
            std::string       type;     // Always "service"
            const MXS_MODULE* router;   // The router module

            std::string user;                       /**< Username */
            std::string password;                   /**< Password */
            std::string version_string;             /**< Version string sent to clients */
            int64_t     max_connections;            /**< Maximum client connections */
            bool        enable_root;                /**< Allow root user  access */
            bool        users_from_all;             /**< Load users from all servers */
            bool        log_auth_warnings;          /**< Log authentication failures and warnings */
            bool        session_trace;

            std::chrono::seconds conn_idle_timeout;     /**< Session timeout in seconds */
            std::chrono::seconds net_write_timeout;     /**< Write timeout in seconds */

            int64_t retain_last_statements;     /**< How many statements to retain per session,
                                                 * -1 if not explicitly specified. */

            std::chrono::seconds connection_keepalive;      /**< How often to ping idle sessions */
            bool                 force_connection_keepalive;

            /**
             * Remove the '\' characters from database names when querying them from the server. This
             * is required when users make grants such as "grant select on `test\_1`.* to ..." to avoid
             * wildcard matching against _. A plain "grant select on `test_1`.* to ..." would normally
             * grant access to e.g. testA1. MaxScale does not support this type of wilcard matching for
             * the database, but it must still understand the escaped version of the grant. */
            bool strip_db_esc;

            bool localhost_match_wildcard_host;

            int64_t rank;   /*< The ranking of this service */

            bool    prune_sescmd_history;
            bool    disable_sescmd_history;
            int64_t max_sescmd_history;

            /**
             * Can backend connections be pooled while session is still running? This is the time a
             * session must be idle before backend connections can be pooled. */
            std::chrono::milliseconds idle_session_pool_time;

            /** How long an endpoint can wait for a connection to become available */
            std::chrono::seconds multiplex_timeout;

            /** User accounts file settings. Given to user account manager. **/
            std::string                             user_accounts_file_path;
            mxs::UserAccountManager::UsersFileUsage user_accounts_file_usage;
        };

        const mxs::WorkerGlobal<Values>& values() const
        {
            return m_values;
        }

        /**
         * Get enabled log levels
         *
         * The returned value has one bit for each LOG_... flag, with the bits left-shifted by the value of
         * the macro. For example, LOG_NOTICE has the value 5 which means the sixth bit is set:
         * `1 << LOG_NOTICE == 1 << 5`
         *
         * @return The enabled log levels
         */
        int log_levels() const;

    private:
        bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;

        Values                    m_v;
        mxs::WorkerGlobal<Values> m_values;
        SERVICE*                  m_service;

        // These aren't used directly so we don't need to expose them
        bool m_log_debug;
        bool m_log_info;
        bool m_log_notice;
        bool m_log_warning;
    };

    State  state {State::ALLOC};        /**< The service state */
    time_t started {0};                 /**< The time when the service was started */

    const char* name() const override
    {
        return m_name.c_str();
    }

    const char* router_name() const
    {
        return m_router_name.c_str();
    }

    /**
     * Get service configuration
     *
     * The returned configuration can only be accessed on a RoutingWorker thread.
     *
     * @return Reference to the WorkerGlobal configuration object
     */
    virtual const mxs::WorkerGlobal<Config::Values>& config() const = 0;

    /**
     * Get server version
     *
     * @param which Which value to retrieve, the minimum, maximum or any value
     *
     * @return The corresponding backend server version
     */
    virtual uint64_t get_version(service_version_which_t which) const = 0;

    /**
     * Get all servers that are reachable from this service
     *
     * @return All servers that can be reached via this service
     */
    virtual std::vector<SERVER*> reachable_servers() const = 0;

    /**
     * Get the cluster that the service uses
     *
     * @return The cluster that the service uses or nullptr if it doesn't use one
     */
    virtual mxs::Monitor* cluster() const = 0;

    /**
     * Get the user account cache for the current routing worker. Should be only called from a routing
     * worker.
     *
     * @return Thread-specific user account cache
     */
    virtual const mxs::UserAccountCache* user_account_cache() const = 0;

    /**
     * Notify the service that authentication failed. The service may forward the notification to its user
     * account manager which then updates its data.
     */
    virtual void request_user_account_update() = 0;

    /**
     *  The user account manager should call this function after it has read user data from a backend
     *  and updated its internal database. Calling this function causes the service to sync all
     *  thread-specific user data caches with the master data.
     *
     *  Even empty (no changes) and failed updates should be broadcasted as they may be of interest
     *  to any sessions waiting for user account data.
     */
    virtual void sync_user_account_caches() = 0;

    /**
     * Add a client connection to the list of clients to wakeup on userdata load.
     *
     * @param client Client connection to add
     */
    virtual void mark_for_wakeup(mxs::ClientConnection* client) = 0;

    /**
     * Remove a client connection from the wakeup list. Typically only needed when a sleeping connection
     * is closed.
     *
     * @param client Client connection to remove
     */
    virtual void unmark_for_wakeup(mxs::ClientConnection* client) = 0;

    /**
     * Whether to log a message at a specific level for this service.
     *
     * @param level The log level to inspect
     *
     * @return True if the mesage should be logged
     */
    virtual bool log_is_enabled(int level) const = 0;

    /**
     * Has a connection limit been reached?
     */
    bool has_too_many_connections() const
    {
        auto limit = config()->max_connections;
        return limit && stats().n_client_conns() > limit;
    }

    /**
     * Get the version string of the service. If a version string is configured, returns that. Otherwise
     * returns the version string of the server with the smallest version number.
     *
     * @return Version string
     */
    std::string version_string() const;

    /**
     * Get custom version suffix. Used by client protocol when generating server handshake.
     *
     * @return Version suffix
     */
    const std::string& custom_version_suffix();

    /**
     * Set custom version suffix. This is meant to be used by a router which wants to add custom text to
     * any version string sent to clients. Should be only called during service/router creation,
     * as there is no concurrency protection.
     *
     * @param custom_version_suffix The version suffix to set
     */
    void set_custom_version_suffix(const std::string& custom_version_suffix);

    uint8_t charset() const;

    mxs::Router* router() const;

    /**
     * The service should track these variables.
     *
     * @param variables  The variables to be tracked.
     */
    void track_variables(const std::set<std::string>& variables);

    /**
     * The service should track this variables.
     *
     * @param variable  The variable to be tracked.
     */
    void track_variable(const std::string& variable)
    {
        std::set<std::string> variables { variable };
        track_variables(variables);
    }

    bool is_suspended() const
    {
        return m_suspended.load(std::memory_order_relaxed);
    }

    // Tracks the maximum length of the session command history. This should be called at the end of the
    // session if the session had session command history enabled.
    void track_history_length(size_t len)
    {
        m_history_len.track(len);
    }

    // Tracks session lifetime
    void track_session_duration(std::chrono::steady_clock::duration dur)
    {
        m_session_lifetime.track(mxb::to_secs(dur));
    }

    json_t* stats_to_json() const;

protected:
    friend class Configuration;

    std::set<std::string> m_tracked_variables;

    SERVICE(const std::string& name,
            const std::string& router_name)
        : started(time(nullptr))
        , m_name(name)
        , m_router_name(router_name)
    {
    }

    uint64_t m_capabilities {0};    /**< The capabilities of the service, @see enum routing_capability */

    std::atomic<bool> m_suspended { false };

    std::unique_ptr<mxs::Router> m_router;

    virtual bool post_configure() = 0;

private:
    const std::string m_name;
    const std::string m_router_name;
    std::string       m_custom_version_suffix;

    mxb::stats::Value<double> m_history_len;        // Sescmd history length
    mxb::stats::Value<double> m_session_lifetime;
};

typedef enum count_spec_t
{
    COUNT_NONE = 0,
    COUNT_ATLEAST,
    COUNT_EXACT,
    COUNT_ATMOST
} count_spec_t;

/**
 * Return the version of the service. The returned version can be
 *
 * - the version of any (in practice the first) server associated
 *   with the service,
 * - the smallest version of any of the servers associated with
 *   the service, or
 * - the largest version of any of the servers associated with
 *   the service.
 *
 * @param service  The service.
 * @param which    Which version.
 *
 * @return The version of the service.
 */
uint64_t service_get_version(const SERVICE* service, service_version_which_t which);
