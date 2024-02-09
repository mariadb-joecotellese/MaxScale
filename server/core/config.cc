/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * @file config.c Configuration file processing
 */

#include <maxscale/config.hh>

#include <ctype.h>
#include <ftw.h>
#include <fcntl.h>
#include <glob.h>
#include <netdb.h>
#include <net/if.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <fstream>
#include <functional>
#include <map>
#include <numeric>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <unordered_set>

#include <maxbase/format.hh>
#include <maxbase/ini.hh>
#include <maxbase/pretty_print.hh>
#include <maxscale/cachingparser.hh>
#include <maxbase/system.hh>
#include <maxscale/clock.hh>
#include <maxscale/http.hh>
#include <maxscale/json_api.hh>
#include <maxscale/key_manager.hh>
#include <maxscale/listener.hh>
#include <maxscale/log.hh>
#include <maxscale/maxscale.hh>
#include <maxscale/paths.hh>
#include <maxscale/pcre2.hh>
#include <maxscale/router.hh>
#include <maxscale/secrets.hh>
#include <maxscale/utils.hh>
#include <maxscale/version.hh>

#include "internal/adminusers.hh"
#include "internal/config.hh"
#include "internal/configmanager.hh"
#include "internal/defaults.hh"
#include "internal/event.hh"
#include "internal/filter.hh"
#include "internal/modules.hh"
#include "internal/monitormanager.hh"
#include "internal/servermanager.hh"
#include "internal/service.hh"
#include "internal/defaults.hh"

#ifdef MXS_WITH_ASAN
#include <sanitizer/lsan_interface.h>
#endif

using std::move;
using std::set;
using std::string;
using maxscale::Monitor;
using std::chrono::milliseconds;
using std::chrono::seconds;

namespace
{
constexpr char CN_ADMIN_AUTH[] = "admin_auth";
constexpr char CN_ADMIN_ENABLED[] = "admin_enabled";
constexpr char CN_ADMIN_GUI[] = "admin_gui";
constexpr char CN_ADMIN_SECURE_GUI[] = "admin_secure_gui";
constexpr char CN_ADMIN_HOST[] = "admin_host";
constexpr char CN_ADMIN_PAM_READONLY_SERVICE[] = "admin_pam_readonly_service";
constexpr char CN_ADMIN_PAM_READWRITE_SERVICE[] = "admin_pam_readwrite_service";
constexpr char CN_ADMIN_READWRITE_HOSTS[] = "admin_readwrite_hosts";
constexpr char CN_ADMIN_READONLY_HOSTS[] = "admin_readonly_hosts";
constexpr char CN_ADMIN_PORT[] = "admin_port";
constexpr char CN_ADMIN_SSL_CA[] = "admin_ssl_ca";
constexpr char CN_ADMIN_SSL_CA_CERT[] = "admin_ssl_ca_cert";
constexpr char CN_ADMIN_SSL_CERT[] = "admin_ssl_cert";
constexpr char CN_ADMIN_SSL_KEY[] = "admin_ssl_key";
constexpr char CN_ADMIN_SSL_VERSION[] = "admin_ssl_version";
constexpr char CN_AUTO[] = "auto";
constexpr char CN_AUTO_TUNE[] = "auto_tune";
constexpr char CN_DEBUG[] = "debug";
constexpr char CN_DUMP_LAST_STATEMENTS[] = "dump_last_statements";
constexpr char CN_LOCAL_ADDRESS[] = "local_address";
constexpr char CN_LOG_DEBUG[] = "log_debug";
constexpr char CN_LOG_INFO[] = "log_info";
constexpr char CN_LOG_NOTICE[] = "log_notice";
constexpr char CN_LOG_THROTTLING[] = "log_throttling";
constexpr char CN_LOG_WARNING[] = "log_warning";
constexpr char CN_LOG_WARN_SUPER_USER[] = "log_warn_super_user";
constexpr char CN_MAX_AUTH_ERRORS_UNTIL_BLOCK[] = "max_auth_errors_until_block";
constexpr char CN_MAX_READ_AMOUNT[] = "max_read_amount";
constexpr char CN_MS_TIMESTAMP[] = "ms_timestamp";
constexpr char CN_PASSIVE[] = "passive";
constexpr char CN_PERSIST_RUNTIME_CHANGES[] = "persist_runtime_changes";
constexpr char CN_QUERY_CLASSIFIER_ARGS[] = "query_classifier_args";
constexpr char CN_QUERY_RETRIES[] = "query_retries";
constexpr char CN_QUERY_RETRY_TIMEOUT[] = "query_retry_timeout";
constexpr char CN_REBALANCE_PERIOD[] = "rebalance_period";
constexpr char CN_REBALANCE_WINDOW[] = "rebalance_window";
constexpr char CN_SKIP_NAME_RESOLVE[] = "skip_name_resolve";
constexpr char CN_SKIP_PERMISSION_CHECKS[] = "skip_permission_checks";
constexpr char CN_USERS_REFRESH_INTERVAL[] = "users_refresh_interval";
constexpr char CN_USERS_REFRESH_TIME[] = "users_refresh_time";
constexpr char CN_SERVER[] = "server";

static int64_t DEFAULT_QC_CACHE_SIZE = get_total_memory() * 0.15;
static int64_t DEFAULT_MAX_READ_AMOUNT = 0;

template<class StoredType>
struct TrackedValue
{
    template<class InitType>
    TrackedValue(InitType initial_value)
        : value(initial_value)
    {
    }

    template<class SetType>
    bool set(SetType value, mxs::config::Origin origin)
    {
        mxb_assert(origin != mxs::config::Origin::DEFAULT);

        bool rv = false;

        if (static_cast<int>(origin) >= static_cast<int>(this->origin))
        {
            this->value = value;
            this->origin = origin;
            rv = true;
        }

        return rv;
    }

    StoredType          value;
    mxs::config::Origin origin { mxs::config::Origin::DEFAULT };
};

const bool DEFAULT_MAXLOG = true;
const bool DEFAULT_SYSLOG = false;

struct ThisUnit
{
    bool mask_passwords = true;

    // The set of objects that were read from the configuration files
    std::set<std::string> static_objects {"maxscale"};

    // The objects that were created at runtime or read from persisted configuration files
    std::set<std::string> dynamic_objects;

    // The names of all objects mapped to the source file they were read from.
    std::map<std::string, std::string> source_files;

    TrackedValue<std::string> configdir           { cmake_defaults::DEFAULT_CONFIGDIR };
    TrackedValue<std::string> config_persistdir   { cmake_defaults::DEFAULT_CONFIG_PERSISTDIR };
    TrackedValue<std::string> module_configdir    { cmake_defaults::DEFAULT_MODULE_CONFIGDIR };
    TrackedValue<std::string> logdir              { cmake_defaults::DEFAULT_LOGDIR };
    TrackedValue<std::string> libdir              { cmake_defaults::DEFAULT_LIBDIR };
    TrackedValue<std::string> sharedir            { cmake_defaults::DEFAULT_SHAREDIR };
    TrackedValue<std::string> cachedir            { cmake_defaults::DEFAULT_CACHEDIR };
    TrackedValue<std::string> datadir             { cmake_defaults::DEFAULT_DATADIR };
    std::string               processdatadir      { cmake_defaults::DEFAULT_DATADIR };
    TrackedValue<std::string> langdir             { cmake_defaults::DEFAULT_LANGDIR };
    TrackedValue<std::string> piddir              { cmake_defaults::DEFAULT_PIDDIR };
    TrackedValue<std::string> execdir             { cmake_defaults::DEFAULT_EXECDIR };
    TrackedValue<std::string> connector_plugindir { cmake_defaults::DEFAULT_CONNECTOR_PLUGINDIR };
    TrackedValue<uint32_t>    log_augmentation    { 0 };
    TrackedValue<bool>        maxlog              { DEFAULT_MAXLOG };
    TrackedValue<bool>        syslog              { DEFAULT_SYSLOG };
} this_unit;

class DuplicateChecker
{
public:
    void check(const std::string& type, const std::string& who,
               const std::string& param, const std::string& value)
    {
        if (!m_values.insert(value).second)
        {
            MXB_WARNING("The %s '%s' has a duplicate value in '%s': %s",
                        type.c_str(), who.c_str(), param.c_str(), value.c_str());
        }
    }

private:
    std::set<std::string> m_values;
};
}

namespace maxscale
{

void set_configdir(std::string_view path, config::Origin origin)
{
    this_unit.configdir.set(clean_up_pathname(std::string{path}), origin);
}

void set_module_configdir(std::string_view path, config::Origin origin)
{
    this_unit.module_configdir.set(clean_up_pathname(std::string{path}), origin);
}

void set_config_persistdir(std::string_view path, config::Origin origin)
{
    this_unit.config_persistdir.set(clean_up_pathname(std::string{path}), origin);
}

void set_logdir(std::string_view path, config::Origin origin)
{
    this_unit.logdir.set(clean_up_pathname(std::string{path}), origin);
}

void set_langdir(std::string_view path, config::Origin origin)
{
    this_unit.langdir.set(clean_up_pathname(std::string{path}), origin);
}

void set_piddir(std::string_view path, config::Origin origin)
{
    this_unit.piddir.set(clean_up_pathname(std::string{path}), origin);
}

void set_cachedir(std::string_view path, config::Origin origin)
{
    this_unit.cachedir.set(clean_up_pathname(std::string{path}), origin);
}

void set_datadir(std::string_view path, config::Origin origin)
{
    this_unit.datadir.set(clean_up_pathname(std::string{path}), origin);
}

void set_process_datadir(std::string_view path)
{
    this_unit.processdatadir = clean_up_pathname(std::string{path});
}

void set_libdir(std::string_view path, config::Origin origin)
{
    this_unit.libdir.set(clean_up_pathname(std::string{path}), origin);
}

void set_sharedir(std::string_view path, config::Origin origin)
{
    this_unit.sharedir.set(clean_up_pathname(std::string{path}), origin);
}

void set_execdir(std::string_view path, config::Origin origin)
{
    this_unit.execdir.set(clean_up_pathname(std::string{path}), origin);
}

void set_connector_plugindir(std::string_view path, config::Origin origin)
{
    this_unit.connector_plugindir.set(clean_up_pathname(std::string{path}), origin);
}

void set_log_augmentation(uint32_t bits, config::Origin origin)
{
    if (this_unit.log_augmentation.set(bits, origin))
    {
        mxb_log_set_augmentation(bits);
    }
}

// TODO: The maxlog setting is now kept in two places; in this_unit.maxlog above
// TODO: and in Configuration::maxlog. The configuration system should be extended
// TODO: so that change origin tracking is performed by it.
void set_maxlog(bool on, config::Origin origin)
{
    if (this_unit.maxlog.set(on, origin))
    {
        Config::get().maxlog.set(on);
    }
}

// TODO: See maxlog above.
void set_syslog(bool on, config::Origin origin)
{
    if (this_unit.syslog.set(on, origin))
    {
        Config::get().syslog.set(on);
    }
}

const char* libdir()
{
    return this_unit.libdir.value.c_str();
}

const char* sharedir()
{
    return this_unit.sharedir.value.c_str();
}

const char* cachedir()
{
    return this_unit.cachedir.value.c_str();
}

const char* datadir()
{
    return this_unit.datadir.value.c_str();
}

const char* process_datadir()
{
    return this_unit.processdatadir.c_str();
}

const char* configdir()
{
    return this_unit.configdir.value.c_str();
}

const char* module_configdir()
{
    return this_unit.module_configdir.value.c_str();
}

const char* config_persistdir()
{
    return this_unit.config_persistdir.value.c_str();
}

const char* piddir()
{
    return this_unit.piddir.value.c_str();
}

const char* logdir()
{
    return this_unit.logdir.value.c_str();
}

const char* langdir()
{
    return this_unit.langdir.value.c_str();
}

const char* execdir()
{
    return this_unit.execdir.value.c_str();
}

const char* connector_plugindir()
{
    return this_unit.connector_plugindir.value.c_str();
}

bool Config::Specification::validate(const Configuration* pConfig,
                                     const ConfigParameters& params,
                                     ConfigParameters* pUnrecognized) const
{
    ConfigParameters unrecognized;

    bool validated = config::Specification::validate(pConfig, params, &unrecognized);

    if (validated)
    {
        for (const auto& kv : unrecognized)
        {
            bool found = false;

            const auto& name = kv.first;
            const auto& value = kv.second;

            if (maxscale::event::validate(name, value) == maxscale::event::ACCEPTED)
            {
                found = true;
            }

            if (!found)
            {
                for (int i = 0; !found && config_pre_parse_global_params[i]; ++i)
                {
                    found = (name == config_pre_parse_global_params[i]);
                }
            }

            if (!found)
            {
                if (pUnrecognized)
                {
                    pUnrecognized->set(name, value);
                }
                else
                {
                    MXB_ERROR("Unknown global parameter '%s'.", name.c_str());
                    validated = false;
                }
            }
        }
    }

    // The validity of config_sync_cluster is checked after the monitors have been allocated
    if (!s_config_sync_cluster.get(params).empty())
    {
        if (s_config_sync_user.get(params).empty())
        {
            MXB_ERROR("Parameter '%s' must be defined when '%s' is used.",
                      s_config_sync_user.name().c_str(), s_config_sync_cluster.name().c_str());
            validated = false;
        }
        else if (s_config_sync_password.get(params).empty())
        {
            MXB_ERROR("Parameter '%s' must be defined when '%s' is used.",
                      s_config_sync_password.name().c_str(), s_config_sync_cluster.name().c_str());
            validated = false;
        }
    }

    auto algo = s_admin_jwt_algorithm.get(params);

    switch (algo)
    {
    case mxs::JwtAlgo::HS256:
    case mxs::JwtAlgo::HS384:
    case mxs::JwtAlgo::HS512:
    case mxs::JwtAlgo::AUTO:
        // No need for private keys
        break;

    default:
        if (s_admin_ssl_key.get(params).empty() || s_admin_ssl_cert.get(params).empty())
        {
            MXB_ERROR("Both '%s' and '%s' must be defined when '%s=%s' is used.",
                      s_admin_ssl_key.name().c_str(), s_admin_ssl_cert.name().c_str(),
                      s_admin_jwt_algorithm.name().c_str(), s_admin_jwt_algorithm.to_string(algo).c_str());
            validated = false;
        }
        break;
    }

    if (s_config_sync_db.get(params).empty())
    {
        MXB_ERROR("'%s'cannot be empty.", s_config_sync_db.name().c_str());
        validated = false;
    }

    return validated;
}

bool Config::Specification::validate(const Configuration* pConfig,
                                     json_t* pJson,
                                     std::set<std::string>* pUnrecognized) const
{
    bool ok = false;
    auto cluster = s_config_sync_cluster.get(pJson);

    if (cluster.empty() || MonitorManager::find_monitor(cluster.c_str()))
    {
        // TODO: Build length limits into ParamString
        if (cluster.length() > mxs::ConfigManager::CLUSTER_MAX_LEN)
        {
            MXB_ERROR("The cluster name for '%s' must be less than %d characters long.",
                      CN_CONFIG_SYNC_CLUSTER, mxs::ConfigManager::CLUSTER_MAX_LEN);
        }
        else
        {
            ok = mxs::config::Specification::validate(pConfig, pJson, pUnrecognized);
        }
    }
    else
    {
        MXB_ERROR("The value of '%s' is not the name of a monitor: %s.",
                  CN_CONFIG_SYNC_CLUSTER, cluster.c_str());
    }

    if (!cluster.empty())
    {
        if (s_config_sync_user.get(pJson).empty())
        {
            MXB_ERROR("Parameter '%s' must be defined when '%s' is used.",
                      s_config_sync_user.name().c_str(), s_config_sync_cluster.name().c_str());
            ok = false;
        }
        if (s_config_sync_password.get(pJson).empty())
        {
            MXB_ERROR("Parameter '%s' must be defined when '%s' is used.",
                      s_config_sync_password.name().c_str(), s_config_sync_cluster.name().c_str());
            ok = false;
        }
    }

    if (s_config_sync_db.get(pJson).empty())
    {
        MXB_ERROR("'%s'cannot be empty.", s_config_sync_db.name().c_str());
        ok = false;
    }

    return ok;
}

template<class Params, class NestedParams>
bool Config::Specification::do_post_validate(Params& params, const NestedParams& nested_params) const
{
    bool rv = true;
    auto it = nested_params.find("event");

    if (it != nested_params.end())
    {
        rv = validate_events(it->second);
    }

    auto whw = s_writeq_high_water.get(params);
    auto wlw = s_writeq_low_water.get(params);

    if (whw != 0 || wlw != 0)
    {
        if (whw <= wlw)
        {
            MXB_ERROR("Invalid configuration. %s should be greater than %s or both should be zero.",
                      CN_WRITEQ_HIGH_WATER, CN_WRITEQ_LOW_WATER);
            rv = false;
        }
    }

    if (s_skip_name_resolve.get(params))
    {
        // Skip name resolve is on, this will cause host pattern entries to stop working. Do not allow
        // such a configuration, since if admin_readwrite_hosts only has host patterns, runtime config
        // modifications become impossible.
        const char fmt[] = "'%s' cannot be enabled if '%s' includes hostname patterns. "
                           "Use only numeric addresses in '%s'";
        auto rw_hosts = s_admin_rw_hosts.get(params);
        if (!rw_hosts.host_patterns.empty())
        {
            MXB_ERROR(fmt, CN_SKIP_NAME_RESOLVE, CN_ADMIN_READWRITE_HOSTS, CN_ADMIN_READWRITE_HOSTS);
            rv = false;
        }
        auto ro_hosts = s_admin_ro_hosts.get(params);
        if (!ro_hosts.host_patterns.empty())
        {
            MXB_ERROR(fmt, CN_SKIP_NAME_RESOLVE, CN_ADMIN_READONLY_HOSTS, CN_ADMIN_READONLY_HOSTS);
            rv = false;
        }
    }

    int nRunning = RoutingWorker::nRunning();
    int nRequested = s_n_threads.get(params);
    int nThreads_max = s_n_threads_max.get(params);

    if (nRequested != nRunning)
    {
        if (nRunning != 0) // Will be 0 at startup.
        {
            std::vector<Service*> services = Service::get_all();

            for (auto* service : services)
            {
                if (rcap_type_required(service->capabilities(), RCAP_TYPE_NO_THREAD_CHANGE))
                {
                    MXB_ERROR("The service '%s' cannot handle a change in the number of threads. "
                              "The configuration must manually be updated and MaxScale restarted.",
                              service->name());
                    rv = false;
                }
            }

            if (rv && (nRequested > nThreads_max))
            {
                MXB_ERROR("MaxScale can have at most %d routing threads; a request for %d cannot be honored. "
                          "The maximum can be increased with `threads_max`.", nThreads_max, nRequested);
                rv = false;
            }
        }

        if (rv && RoutingWorker::termination_in_process())
        {
            MXB_ERROR("A thread is being terminated, a change in the number of threads "
                      "cannot currently be made.");
            rv = false;

        }
    }

    return rv;
}

bool Config::Specification::validate_events(const mxs::ConfigParameters& event_params) const
{
    bool rv = true;

    for (const auto& kv : event_params)
    {
        string name = "event." + kv.first;
        string value = kv.second;

        if (!validate_event(name, value))
        {
            rv = false;
        }
    }

    return rv;
}

bool Config::Specification::validate_events(json_t* pEvent_params) const
{
    bool rv = true;

    const char* zKey;
    json_t* pValue;
    json_object_foreach(pEvent_params, zKey, pValue)
    {
        string name = string("event.") + zKey;
        string value = mxb::json_to_string(pValue);

        if (!validate_event(name, value))
        {
            rv = false;
        }
    }

    return rv;
}

bool Config::Specification::validate_event(const std::string& name, const std::string& value) const
{
    bool rv = true;

    if (maxscale::event::validate(name, value) == maxscale::event::INVALID)
    {
        MXB_ERROR("'%s' is not a valid value for the event '%s'.", value.c_str(), name.c_str());
        rv = false;
    }

    return rv;
}

Config::Specification Config::s_specification("maxscale", config::Specification::GLOBAL);

Config::ParamAutoTune Config::s_auto_tune(
    &Config::s_specification,
    CN_AUTO_TUNE,
    "Specifies whether a MaxScale parameter whose value depends on a specific global server "
    "variable, should automatically be updated to match the variable's current value.",
    ",",    // Delimiter
    {},
    config::Param::Modifiable::AT_STARTUP);

config::ParamBool Config::s_log_debug(
    &Config::s_specification,
    CN_LOG_DEBUG,
    "Specifies whether debug messages should be logged (meaningful only with debug builds).",
    false,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_log_info(
    &Config::s_specification,
    CN_LOG_INFO,
    "Specifies whether info messages should be logged.",
    false,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_log_notice(
    &Config::s_specification,
    CN_LOG_NOTICE,
    "Specifies whether notice messages should be logged.",
    true,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_log_warning(
    &Config::s_specification,
    CN_LOG_WARNING,
    "Specifies whether warning messages should be logged.",
    true,
    config::Param::Modifiable::AT_RUNTIME);

Config::ParamLogThrottling Config::s_log_throttling(
    &Config::s_specification,
    CN_LOG_THROTTLING,
    "Limit the amount of identical log messages than can be logged during a certain time period."
    );

config::ParamEnum<session_dump_statements_t> Config::s_dump_statements(
    &Config::s_specification,
    CN_DUMP_LAST_STATEMENTS,
    "In what circumstances should the last statements that a client sent be dumped.",
    {
        {SESSION_DUMP_STATEMENTS_ON_CLOSE, "on_close"},
        {SESSION_DUMP_STATEMENTS_ON_ERROR, "on_error"},
        {SESSION_DUMP_STATEMENTS_NEVER, "never"}
    },
    SESSION_DUMP_STATEMENTS_NEVER,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamCount Config::s_session_trace(
    &Config::s_specification,
    CN_SESSION_TRACE,
    "How many log entries are stored in the session specific trace log.",
    0,                                                          // default
    0,                                                          // min
    std::numeric_limits<config::ParamCount::value_type>::max(), // max
    config::Param::Modifiable::AT_RUNTIME);

config::ParamRegex Config::s_session_trace_match(
    &Config::s_specification, "session_trace_match",
    "Regular expression that is matched against the contents of the session trace log and "
    "if it matches the contents are logged when the session stops.",
    "", config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_ms_timestamp(
    &Config::s_specification,
    CN_MS_TIMESTAMP,
    "Enable or disable high precision timestamps.",
    false,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamCount Config::s_retain_last_statements(
    &Config::s_specification,
    CN_RETAIN_LAST_STATEMENTS,
    "How many statements should be retained for each session for debugging purposes.",
    0,                                                              // default
    0,                                                              // min
    std::numeric_limits<config::ParamInteger::value_type>::max(),   // max
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_syslog(
    &Config::s_specification,
    CN_SYSLOG,
    "Log to syslog.",
    DEFAULT_SYSLOG,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_maxlog(
    &Config::s_specification,
    CN_MAXLOG,
    "Log to MaxScale's own log.",
    DEFAULT_MAXLOG,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamSeconds Config::s_auth_conn_timeout(
    &Config::s_specification,
    CN_AUTH_CONNECT_TIMEOUT,
    "Connection timeout for fetching user accounts.",
    std::chrono::seconds(DEFAULT_AUTH_CONNECT_TIMEOUT),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamSeconds Config::s_auth_read_timeout(
    &Config::s_specification,
    CN_AUTH_READ_TIMEOUT,
    "Read timeout for fetching user accounts (deprecated).",
    std::chrono::seconds(DEFAULT_AUTH_READ_TIMEOUT),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamSeconds Config::s_auth_write_timeout(
    &Config::s_specification,
    CN_AUTH_WRITE_TIMEOUT,
    "Write timeout for for fetching user accounts (deprecated).",
    std::chrono::seconds(DEFAULT_AUTH_WRITE_TIMEOUT),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamDeprecated<config::ParamBool> Config::s_skip_permission_checks(
    &Config::s_specification,
    CN_SKIP_PERMISSION_CHECKS,
    "Skip service and monitor permission checks.",
    false,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_passive(
    &Config::s_specification,
    CN_PASSIVE,
    "True if MaxScale is in passive mode.",
    false,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamSize Config::s_qc_cache_max_size(
    &Config::s_specification,
    CN_QUERY_CLASSIFIER_CACHE_SIZE,
    "Maximum amount of memory used by query classifier cache.",
    DEFAULT_QC_CACHE_SIZE,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_admin_log_auth_failures(
    &Config::s_specification,
    CN_ADMIN_LOG_AUTH_FAILURES,
    "Log admin interface authentication failures.",
    true,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamInteger Config::s_query_retries(
    &Config::s_specification,
    CN_QUERY_RETRIES,
    "Number of times an interrupted query is retried.",
    DEFAULT_QUERY_RETRIES,
    0,
    std::numeric_limits<config::ParamInteger::value_type>::max());

config::ParamSeconds Config::s_query_retry_timeout(
    &Config::s_specification,
    CN_QUERY_RETRY_TIMEOUT,
    "The total timeout in seconds for any retried queries.",
    std::chrono::seconds(DEFAULT_QUERY_RETRY_TIMEOUT),
    config::Param::Modifiable::AT_RUNTIME);

Config::ParamUsersRefreshTime Config::s_users_refresh_time(
    &Config::s_specification,
    CN_USERS_REFRESH_TIME,
    "How often the users can be refreshed.",
    std::chrono::seconds(USERS_REFRESH_TIME_DEFAULT),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamSeconds Config::s_users_refresh_interval(
    &Config::s_specification,
    CN_USERS_REFRESH_INTERVAL,
    "How often the users will be refreshed.",
    std::chrono::seconds(0),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamSize Config::s_writeq_high_water(
    &Config::s_specification,
    CN_WRITEQ_HIGH_WATER,
    "High water mark of dcb write queue.",
    64 * 1024,
    0, std::numeric_limits<config::ParamInteger::value_type>::max(),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamSize Config::s_writeq_low_water(
    &Config::s_specification,
    CN_WRITEQ_LOW_WATER,
    "Low water mark of dcb write queue.",
    1024,
    0, std::numeric_limits<config::ParamInteger::value_type>::max(),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamInteger Config::s_max_auth_errors_until_block(
    &Config::s_specification,
    CN_MAX_AUTH_ERRORS_UNTIL_BLOCK,
    "The maximum number of authentication failures that are tolerated "
    "before a host is temporarily blocked.",
    DEFAULT_MAX_AUTH_ERRORS_UNTIL_BLOCK,
    0, std::numeric_limits<config::ParamInteger::value_type>::max(),    // min, max
    config::Param::Modifiable::AT_RUNTIME);

config::ParamInteger Config::s_rebalance_threshold(
    &Config::s_specification,
    CN_REBALANCE_THRESHOLD,
    "If the difference in load between the thread with the maximum load and the thread "
    "with the minimum load is larger than the value of this parameter, then work will "
    "be moved from the former to the latter.",
    20,     // default
    5, 100, // min, max
    config::Param::Modifiable::AT_RUNTIME);

config::ParamDuration<std::chrono::milliseconds> Config::s_rebalance_period(
    &Config::s_specification,
    CN_REBALANCE_PERIOD,
    "How often should the load of the worker threads be checked and rebalancing be made.",
    std::chrono::milliseconds(0),
    config::Param::Modifiable::AT_RUNTIME);

config::ParamCount Config::s_rebalance_window(
    &Config::s_specification,
    CN_REBALANCE_WINDOW,
    "The load of how many seconds should be taken into account when rebalancing.",
    10,     // default
    1, 60,  // min, max
    config::Param::Modifiable::AT_RUNTIME);

config::ParamBool Config::s_skip_name_resolve(
    &Config::s_specification,
    CN_SKIP_NAME_RESOLVE,
    "Do not resolve client IP addresses to hostnames during authentication",
    false,
    config::Param::Modifiable::AT_RUNTIME);

Config::ParamThreadsCount Config::s_n_threads(
    &Config::s_specification,
    CN_THREADS,
    "This parameter specifies how many threads will be used for handling the routing.",
    get_processor_count(), // default
    1, std::numeric_limits<Config::ParamThreadsCount::value_type>::max(), // min, max
    config::Param::Modifiable::AT_RUNTIME);

config::ParamCount Config::s_n_threads_max(
    &Config::s_specification,
    CN_THREADS_MAX,
    "This parameter specifies a hard maximum for the number of routing threads.",
    Config::DEFAULT_THREADS_MAX,
    1, std::numeric_limits<Config::ParamThreadsCount::value_type>::max()); // min, max

config::ParamDeprecated<config::ParamString> Config::s_qc_name(
    &Config::s_specification,
    CN_QUERY_CLASSIFIER,
    "The name of the query classifier to load.",
    "qc_sqlite");

config::ParamDeprecated<config::ParamString> Config::s_qc_args(
    &Config::s_specification,
    CN_QUERY_CLASSIFIER_ARGS,
    "Arguments for the query classifier.",
    "");

config::ParamEnum<mxs::Parser::SqlMode> Config::s_qc_sql_mode(
    &Config::s_specification,
    CN_SQL_MODE,
    "The query classifier sql mode.",
    {
        {mxs::Parser::SqlMode::DEFAULT, "default"},
        {mxs::Parser::SqlMode::ORACLE, "oracle"}
    },
    mxs::Parser::SqlMode::DEFAULT);

config::ParamString Config::s_admin_host(
    &Config::s_specification,
    CN_ADMIN_HOST,
    "Admin interface host.",
    DEFAULT_ADMIN_HOST);

config::ParamInteger Config::s_admin_port(
    &Config::s_specification,
    CN_ADMIN_PORT,
    "Admin interface port.",
    DEFAULT_ADMIN_HTTP_PORT);

config::ParamBool Config::s_admin_auth(
    &Config::s_specification,
    CN_ADMIN_AUTH,
    "Admin interface authentication.",
    true);

config::ParamBool Config::s_admin_enabled(
    &Config::s_specification,
    CN_ADMIN_ENABLED,
    "Admin interface is enabled.",
    true);

config::ParamString Config::s_admin_pam_rw_service(
    &Config::s_specification,
    CN_ADMIN_PAM_READWRITE_SERVICE,
    "PAM service for read-write users.",
    "");

config::ParamString Config::s_admin_pam_ro_service(
    &Config::s_specification,
    CN_ADMIN_PAM_READONLY_SERVICE,
    "PAM service for read-only users.",
    "");

config::ParamHostsPatternList Config::s_admin_rw_hosts(
    &Config::s_specification, CN_ADMIN_READWRITE_HOSTS,
    "Allowed hosts for read-only rest-api users.", config::HostPatterns::default_value());

config::ParamHostsPatternList Config::s_admin_ro_hosts(
    &Config::s_specification, CN_ADMIN_READONLY_HOSTS,
    "Allowed hosts for read-only rest-api users.", config::HostPatterns::default_value());

config::ParamPath Config::s_admin_ssl_key(
    &Config::s_specification,
    CN_ADMIN_SSL_KEY,
    "Admin SSL key",
    config::ParamPath::R,
    "");

config::ParamEnum<mxb::ssl_version::Version> Config::s_admin_ssl_version(
    &Config::s_specification,
    CN_ADMIN_SSL_VERSION,
    "Minimum required TLS protocol version for the REST API",
    {
        {mxb::ssl_version::SSL_TLS_MAX, "MAX"},
        {mxb::ssl_version::TLS10, "TLSv10"},
        {mxb::ssl_version::TLS11, "TLSv11"},
        {mxb::ssl_version::TLS12, "TLSv12"},
        {mxb::ssl_version::TLS13, "TLSv13"}
    }, mxb::ssl_version::SSL_TLS_MAX);

config::ParamPath Config::s_admin_ssl_cert(
    &Config::s_specification,
    CN_ADMIN_SSL_CERT,
    "Admin SSL cert",
    config::ParamPath::R,
    "");

config::ParamPath Config::s_admin_ssl_ca(
    &Config::s_specification,
    CN_ADMIN_SSL_CA,
    "Admin SSL CA cert",
    config::ParamPath::R,
    "");

// Alias admin_ssl_ca_cert -> admin_ssl_ca.
config::ParamDeprecated<config::ParamAlias> Config::s_admin_ssl_ca_cert(
    &Config::s_specification,
    CN_ADMIN_SSL_CA_CERT,
    &Config::s_admin_ssl_ca);

config::ParamEnum<mxs::JwtAlgo> Config::s_admin_jwt_algorithm(
    &Config::s_specification,
    "admin_jwt_algorithm",
    "JWT signature algorithm",
    {
        {mxs::JwtAlgo::AUTO, "auto"},
        {mxs::JwtAlgo::HS256, "HS256"},
        {mxs::JwtAlgo::HS384, "HS384"},
        {mxs::JwtAlgo::HS512, "HS512"},
        {mxs::JwtAlgo::RS256, "RS256"},
        {mxs::JwtAlgo::RS384, "RS384"},
        {mxs::JwtAlgo::RS512, "RS512"},
        {mxs::JwtAlgo::ES256, "ES256"},
        {mxs::JwtAlgo::ES384, "ES384"},
        {mxs::JwtAlgo::ES512, "ES512"},
        {mxs::JwtAlgo::PS256, "PS256"},
        {mxs::JwtAlgo::PS384, "PS384"},
        {mxs::JwtAlgo::PS512, "PS512"},
        {mxs::JwtAlgo::ED25519, "ED25519"},
        {mxs::JwtAlgo::ED448, "ED448"},
    },
    mxs::JwtAlgo::AUTO);

config::ParamString Config::s_admin_jwt_key(
    &Config::s_specification,
    "admin_jwt_key",
    "Encryption key ID for symmetric signature algorithms. If left empty, MaxScale will "
    "generate a random key that is used to sign the JWT.",
    "");

config::ParamSeconds Config::s_admin_jwt_max_age(
    &Config::s_specification,
    "admin_jwt_max_age",
    "Maximum age of the JWTs generated by MaxScale",
    24h,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamString Config::s_admin_jwt_issuer(
    &Config::s_specification,
    "admin_jwt_issuer",
    "The issuer claim for all JWTs generated by MaxScale.",
    "maxscale");

config::ParamString Config::s_admin_oidc_url(
    &Config::s_specification,
    "admin_oidc_url",
    "Extra public certificates used to validate externally signed JWTs",
    "",
    config::Param::Modifiable::AT_RUNTIME);

config::ParamString Config::s_admin_verify_url(
    &Config::s_specification,
    "admin_verify_url",
    "URL for third-party verification of client tokens",
    "");

config::ParamBool Config::s_admin_audit_enabled(
    &Config::s_specification,
    "admin_audit",
    "Enable REST audit logging",
    false,
    config::Param::Modifiable::AT_RUNTIME);

config::ParamString Config::s_admin_audit_file(
    &Config::s_specification,
    "admin_audit_file",
    "Full path to admin audit file",
    std::string(cmake_defaults::DEFAULT_LOGDIR) + "/admin_audit.csv"s,
    mxs::config::Param::AT_RUNTIME);

config::ParamEnumList<maxbase::http::Method> Config::s_admin_audit_exclude_methods(
    &Config::s_specification,
    "admin_audit_exclude_methods",
    "List of HTTP methods to exclude from audit logging, e.g. \"GET\"",
    {
        {maxbase::http::Method::GET, "GET"},
        {maxbase::http::Method::PUT, "PUT"},
        {maxbase::http::Method::POST, "POST"},
        {maxbase::http::Method::PATCH, "PATCH"},
        {maxbase::http::Method::DELETE, "DELETE"},
        {maxbase::http::Method::HEAD, "HEAD"},
        {maxbase::http::Method::CONNECT, "CONNECT"},
        {maxbase::http::Method::OPTIONS, "OPTIONS"},
        {maxbase::http::Method::TRACE, "TRACE"},
    },
    {},
    mxs::config::Param::AT_RUNTIME);

config::ParamString Config::s_local_address(
    &Config::s_specification,
    CN_LOCAL_ADDRESS,
    "Local address to use when connecting.",
    "");

config::ParamBool Config::s_load_persisted_configs(
    &Config::s_specification,
    CN_LOAD_PERSISTED_CONFIGS,
    "Specifies whether persisted configuration files should be loaded on startup.",
    true);

config::ParamBool Config::s_persist_runtime_changes(
    &Config::s_specification,
    CN_PERSIST_RUNTIME_CHANGES,
    "Persist configurations changes done at runtime.",
    true);

config::ParamString Config::s_config_sync_cluster(
    &Config::s_specification,
    CN_CONFIG_SYNC_CLUSTER,
    "Cluster used for configuration synchronization."
    " If left empty (i.e. value is \"\"), synchronization is not done.",
    "", mxs::config::Param::AT_RUNTIME);

config::ParamString Config::s_config_sync_user(
    &Config::s_specification,
    CN_CONFIG_SYNC_USER,
    "User account used for configuration synchronization.",
    "", mxs::config::Param::AT_RUNTIME);

config::ParamPassword Config::s_config_sync_password(
    &Config::s_specification,
    CN_CONFIG_SYNC_PASSWORD,
    "Password for the user used for configuration synchronization.",
    "", mxs::config::Param::AT_RUNTIME);

config::ParamString Config::s_config_sync_db(
    &Config::s_specification,
    CN_CONFIG_SYNC_DB,
    "Database where the 'maxscale_config' table is created.",
    "mysql", mxs::config::Param::AT_STARTUP);

config::ParamSeconds Config::s_config_sync_timeout(
    &Config::s_specification,
    CN_CONFIG_SYNC_TIMEOUT,
    "Timeout for the configuration synchronization operations.",
    std::chrono::seconds(10), mxs::config::Param::AT_RUNTIME);

config::ParamMilliseconds Config::s_config_sync_interval(
    &Config::s_specification,
    CN_CONFIG_SYNC_INTERVAL,
    "How often to synchronize the configuration.",
    std::chrono::seconds(5), mxs::config::Param::AT_RUNTIME);

config::ParamBool Config::s_log_warn_super_user(
    &Config::s_specification,
    CN_LOG_WARN_SUPER_USER,
    "Log a warning when a user with super privilege logs in.",
    false);

config::ParamBool Config::s_gui(
    &Config::s_specification,
    CN_ADMIN_GUI,
    "Enable admin GUI.",
    true);

config::ParamBool Config::s_secure_gui(
    &Config::s_specification,
    CN_ADMIN_SECURE_GUI,
    "Only serve GUI over HTTPS.",
    true);

config::ParamString Config::s_debug(
    &Config::s_specification,
    CN_DEBUG,
    "Debug options",
    "");

config::ParamSize Config::s_max_read_amount(
    &Config::s_specification,
    CN_MAX_READ_AMOUNT,
    "Maximum amount of data read before return to epoll_wait.",
    DEFAULT_MAX_READ_AMOUNT);

Config::ParamKeyManager Config::s_key_manager(
    &Config::s_specification, "key_manager", "Key manager type",
    {
        {mxs::KeyManager::Type::NONE, "none"},
        {mxs::KeyManager::Type::FILE, "file"},
        {mxs::KeyManager::Type::KMIP, "kmip"},
        {mxs::KeyManager::Type::VAULT, "vault"},
    },
    mxs::KeyManager::Type::NONE,
    config::Param::AT_RUNTIME
    );

std::tuple<bool, mxs::ConfigParameters> parse_auth_options(std::string_view opts)
{
    bool success = true;
    mxs::ConfigParameters params;

    auto opt_list = mxb::strtok(opts, ",");
    for (const auto& opt : opt_list)
    {
        auto equals_pos = opt.find('=');
        if (equals_pos != string::npos && equals_pos > 0 && opt.length() > equals_pos + 1)
        {
            string opt_name = opt.substr(0, equals_pos);
            mxb::trim(opt_name);
            string opt_value = opt.substr(equals_pos + 1);
            mxb::trim(opt_value);
            params.set(opt_name, opt_value);
        }
        else
        {
            MXB_ERROR("Invalid authenticator option setting: %s", opt.c_str());
            success = false;
            params.clear();
            break;
        }
    }
    return {success, std::move(params)};
}
}

namespace
{

void reconnect_config_manager(const std::string& ignored)
{
    if (auto manager = mxs::ConfigManager::get())
    {
        manager->reconnect();
    }
}
}

static bool get_milliseconds(const char* zName,
                             const char* zValue,
                             const char* zDisplay_value,
                             time_t* pMilliseconds);

static int get_ifaddr(unsigned char* output);

namespace maxscale
{

class Config::ThreadsCount : public config::Native<ParamThreadsCount, Config>
{
    using Base = config::Native<ParamThreadsCount, Config>;
public:
    using config::Native<ParamThreadsCount, Config>::Native;

    bool set_from_string(const std::string& value_as_string,
                         std::string* pMessage = nullptr) override final
    {
        bool rv = Base::set_from_string(value_as_string, pMessage);

        if (rv)
        {
            m_value_as_string = value_as_string;
        }

        return rv;
    }

    std::string to_string() const override
    {
        std::string rv;

        if (m_value_as_string == CN_AUTO)
        {
            rv = m_value_as_string;
        }
        else
        {
            rv = Base::to_string();
        }

        return rv;
    }

private:
    std::string m_value_as_string;
};


Config::Config()
    : config::Configuration(CN_MAXSCALE, &s_specification)
    , log_debug(this, &s_log_debug, [](bool enable) {
#ifndef SS_DEBUG
    MXB_WARNING("The 'log_debug' option has no effect in release mode.");
#endif
    mxb_log_set_priority_enabled(LOG_DEBUG, enable);
}),
    log_info(this, &s_log_info, [](bool enable) {
    mxb_log_set_priority_enabled(LOG_INFO, enable);
}),
    log_notice(this, &s_log_notice, [](bool enable) {
    mxb_log_set_priority_enabled(LOG_NOTICE, enable);
}),
    log_warning(this, &s_log_warning, [](bool enable) {
    mxb_log_set_priority_enabled(LOG_WARNING, enable);
}),
    log_throttling(this, &s_log_throttling, [](MXB_LOG_THROTTLING throttling) {
    mxb_log_set_throttling(&throttling);
    mxb_log_reset_suppression();
}),
    dump_statements(this, &s_dump_statements, [](session_dump_statements_t when) {
    session_set_dump_statements(when);
}),
    session_trace(this, &s_session_trace, [](int32_t count) {
    session_set_session_trace(count);
    mxb_log_set_session_trace(count > 0 ? true : false);
}),
    session_trace_match(this, &s_session_trace_match),
    ms_timestamp(this, &s_ms_timestamp, [](bool enable) {
    mxb_log_set_highprecision_enabled(enable);
}),
    retain_last_statements(this, &s_retain_last_statements, [](int32_t count) {
    session_set_retain_last_statements(count);
}),
    syslog(this, &s_syslog, [](bool enable) {
    mxb_log_set_syslog_enabled(enable);
}),
    maxlog(this, &s_maxlog, [](bool enable) {
    mxb_log_set_maxlog_enabled(enable);
}),
    auth_conn_timeout(this, &s_auth_conn_timeout),
    auth_read_timeout(this, &s_auth_read_timeout),
    auth_write_timeout(this, &s_auth_write_timeout),
    passive(this, &s_passive, [](bool value) {
    if (Config::get().passive.get() && !value)
    {
        // If we were passive, but no longer are, we register the time.
        Config::get().promoted_at = mxs_clock();
    }
}),
    qc_cache_max_size(this, &s_qc_cache_max_size, [](int64_t size) {
    Config::get().qc_cache_properties.max_size = size;
    mxs::CachingParser::set_properties(Config::get().qc_cache_properties);
}),
    admin_log_auth_failures(this, &s_admin_log_auth_failures),
    query_retries(this, &s_query_retries),
    query_retry_timeout(this, &s_query_retry_timeout),
    users_refresh_time(this, &s_users_refresh_time),
    users_refresh_interval(this, &s_users_refresh_interval),
    writeq_high_water(this, &s_writeq_high_water),
    writeq_low_water(this, &s_writeq_low_water),
    max_auth_errors_until_block(this, &s_max_auth_errors_until_block),
    rebalance_threshold(this, &s_rebalance_threshold),
    rebalance_period(this, &s_rebalance_period, [](const std::chrono::milliseconds&) {
    mxb_assert(MainWorker::get());
    MainWorker::get()->update_rebalancing();
}),
    rebalance_window(this, &s_rebalance_window),
    skip_name_resolve(this, &s_skip_name_resolve),
    admin_audit_enabled(this, &s_admin_audit_enabled),
    admin_audit_file(this, &s_admin_audit_file),
    admin_audit_exclude_methods(this, &s_admin_audit_exclude_methods),
    config_check(false),
    log_target(MXB_LOG_TARGET_DEFAULT),
    substitute_variables(false),
    promoted_at(0)
{
    add_native(&Config::auto_tune, &s_auto_tune);
    add_native<ParamThreadsCount, Config, ThreadsCount>(&Config::n_threads, &s_n_threads);
    add_native(&Config::n_threads_max, &s_n_threads_max);
    add_native(&Config::qc_sql_mode, &s_qc_sql_mode);
    add_native(&Config::admin_host, &s_admin_host);
    add_native(&Config::admin_port, &s_admin_port);
    add_native(&Config::admin_auth, &s_admin_auth);
    add_native(&Config::admin_enabled, &s_admin_enabled);
    add_native(&Config::admin_pam_rw_service, &s_admin_pam_rw_service);
    add_native(&Config::admin_pam_ro_service, &s_admin_pam_ro_service);
    add_native(&Config::admin_rw_hosts, &s_admin_rw_hosts);
    add_native(&Config::admin_ro_hosts, &s_admin_ro_hosts);
    add_native(&Config::admin_ssl_key, &s_admin_ssl_key);
    add_native(&Config::admin_ssl_cert, &s_admin_ssl_cert);
    add_native(&Config::admin_ssl_ca, &s_admin_ssl_ca);
    add_native(&Config::admin_ssl_version, &s_admin_ssl_version);
    add_native(&Config::admin_jwt_algorithm, &s_admin_jwt_algorithm);
    add_native(&Config::admin_jwt_key, &s_admin_jwt_key);
    add_native(&Config::admin_jwt_max_age, &s_admin_jwt_max_age);
    add_native(&Config::admin_jwt_issuer, &s_admin_jwt_issuer);
    add_native(&Config::admin_verify_url, &s_admin_verify_url);
    add_native(&Config::admin_oidc_url, &s_admin_oidc_url);
    add_native(&Config::local_address, &s_local_address);
    add_native(&Config::load_persisted_configs, &s_load_persisted_configs);
    add_native(&Config::persist_runtime_changes, &s_persist_runtime_changes);
    add_native(&Config::config_sync_cluster, &s_config_sync_cluster);
    add_native(&Config::config_sync_user, &s_config_sync_user, reconnect_config_manager);
    add_native(&Config::config_sync_password, &s_config_sync_password, reconnect_config_manager);
    add_native(&Config::config_sync_db, &s_config_sync_db);
    add_native(&Config::config_sync_timeout, &s_config_sync_timeout);
    add_native(&Config::config_sync_interval, &s_config_sync_interval);
    add_native(&Config::log_warn_super_user, &s_log_warn_super_user);
    add_native(&Config::gui, &s_gui);
    add_native(&Config::secure_gui, &s_secure_gui);
    add_native(&Config::debug, &s_debug);
    add_native(&Config::max_read_amount, &s_max_read_amount);
    add_native(&Config::key_manager, &s_key_manager);

    /* get release string */
    this->release_string = mxb::get_release_string();
    if (this->release_string.empty())
    {
        this->release_string = "undefined";
    }

    /* get uname info */
    struct utsname uname_data;
    if (uname(&uname_data) == 0)
    {
        this->sysname = uname_data.sysname;
        this->nodename = uname_data.nodename;
        this->release = uname_data.release;
        this->version = uname_data.version;
        this->machine = uname_data.machine;
    }
}

// static
Config Config::s_config;

// static
std::vector<std::string> Config::s_argv;

// static
void Config::init(int argc, char** argv)
{
    mxb_assert(s_argv.empty());

    s_argv.insert(s_argv.begin(), argv, argv + argc);
}

// static
const char* Config::get_object_type(const std::string& name)
{
    if (ServerManager::find_by_unique_name(name))
    {
        return "server";
    }
    else if (Service::find(name))
    {
        return "service";
    }
    else if (MonitorManager::find_monitor(name.c_str()))
    {
        return "monitor";
    }
    else if (filter_find(name))
    {
        return "filter";
    }
    else if (mxs::Listener::find(name))
    {
        return "listener";
    }

    return nullptr;
}

// static
bool Config::is_static_object(const std::string& name)
{
    return this_unit.static_objects.find(name) != this_unit.static_objects.end();
}

// static
bool Config::is_dynamic_object(const std::string& name)
{
    return this_unit.dynamic_objects.find(name) != this_unit.dynamic_objects.end()
           || !is_static_object(name);
}

// static
void Config::set_object_source_file(const std::string& name, const std::string& file)
{
    this_unit.source_files[name] = file;
}

// static
json_t* Config::object_source_to_json(const std::string& name)
{
    json_t* obj = json_object();
    json_t* source_file = nullptr;
    json_t* source_type = nullptr;

    if (name.substr(0, 2) == "@@")
    {
        source_file = json_null();
        source_type = json_string("volatile");
    }
    else if (!mxs::Config::get().config_sync_cluster.empty())
    {
        source_file = json_string(mxs::ConfigManager::get()->dynamic_config_filename().c_str());
        source_type = json_string("cluster");
    }
    else if (auto it = this_unit.source_files.find(name); it != this_unit.source_files.end())
    {
        source_file = json_string(it->second.c_str());
        source_type = json_string(is_dynamic_object(name) ? "runtime" : "static");
    }
    else
    {
        // load_persisted_configs or persist_runtime_changes has been disabled which means we don't know if
        // the object was modified, only if it originated from a config file or not. This branch should only
        // be reached with objects that were created at runtime.
        mxb_assert(!is_static_object(name)
                   && (!mxs::Config::get().load_persisted_configs
                       || !mxs::Config::get().persist_runtime_changes));
        source_file = json_null();
        source_type = json_string("runtime");
    }

    mxb_assert(source_file && source_type);
    json_object_set_new(obj, "file", source_file);
    json_object_set_new(obj, "type", source_type);

    return obj;
}

bool Config::configure(const mxs::ConfigParameters& params, mxs::ConfigParameters* pUnrecognized)
{
    mxs::ConfigParameters unrecognized;
    bool configured = config::Configuration::configure(params, &unrecognized);

    if (configured)
    {
        check_cpu_situation();

        if (this->qc_cache_properties.max_size == 0)
        {
            MXB_NOTICE("Query classifier cache is disabled");
        }
        else
        {
            MXB_NOTICE("Using up to %s of memory for query classifier cache",
                       mxb::pretty_size(this->qc_cache_properties.max_size).c_str());

            check_memory_situation();
        }
    }

    return configured;
}

void Config::check_cpu_situation() const
{
    // We can hardly have a fewer number of threads than 1, and we have warned already
    // if the specified number of threads is larger than the number of hardware cores.
    if (this->n_threads > 1 && this->n_threads <= get_processor_count())
    {
        double vcpu = get_vcpu_count();

        if (this->n_threads > ceil(vcpu) + 1) // One more than available is still ok.
        {
            MXB_WARNING("Number of threads set to %d, which is significantly more than "
                        "the %.2f virtual cores available to MaxScale. This may lead "
                        "to worse performance and MaxScale using more resources than what "
                        "is available.",
                        (int)this->n_threads, vcpu);
        }
    }
}

void Config::check_memory_situation() const
{
    int64_t total_memory = get_total_memory();
    int64_t available_memory = get_available_memory();

    if (total_memory != available_memory)
    {
        // If the query classifier cache size has not been explicitly specified
        // and the default (calculated based upon total size) is used, or if the
        // size is clearly wrong.

        if (this->qc_cache_properties.max_size == DEFAULT_QC_CACHE_SIZE
            || this->qc_cache_properties.max_size > available_memory)
        {
            MXB_WARNING("It seems MaxScale is running in a constrained environment with "
                        "less memory (%s) available in it than what is installed on the "
                        "machine (%s). In this context, the query classifier cache size "
                        "should be specified explicitly in the configuration file with "
                        "'query_classifier_cache_size' set to 15%% of the available memory. "
                        "Otherwise MaxScale may use more resources than what is available, "
                        "which may cause it to crash.",
                        mxb::pretty_size(available_memory).c_str(),
                        mxb::pretty_size(total_memory).c_str());
        }
    }
}

std::ostream& Config::persist_maxscale(std::ostream& os) const
{
    mxs::config::Configuration::persist(os);
    auto prefix = s_key_manager.to_string(key_manager);

    for (const auto& [k, v] : key_manager_options)
    {
        os << prefix << '.' << k << '=' << v << '\n';
    }

    return os;
}

bool Config::post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params)
{
    bool rv = true;
    auto it = nested_params.find("event");

    if (it != nested_params.end())
    {
        for (const auto& kv : it->second)
        {
            const auto& name = "event." + kv.first;
            const auto& value = kv.second;

            MXB_AT_DEBUG(auto result =)maxscale::event::configure(name, value);
            mxb_assert(result != maxscale::event::INVALID);
        }
    }

    it = nested_params.find(s_key_manager.to_string(this->key_manager));

    if (it != nested_params.end())
    {
        this->key_manager_options = it->second;
    }

    if (!mxs::KeyManager::configure())
    {
        rv = false;
    }

    // Assign local address only on startup.
    if (!RoutingWorker::is_running() && !local_address.empty())
    {
        auto [ai, errmsg] = getaddrinfo(local_address.c_str());
        if (ai)
        {
            if (!mxb::Host::is_valid_ipv4(local_address) && !mxb::Host::is_valid_ipv6(local_address))
            {
                // Warn if local address is a hostname.
                auto addr_str = mxb::ntop(ai->ai_addr);
                MXB_WARNING("Config setting '%s' is a hostname and resolved to address %s. The name "
                            "lookup will not be repeated so hostname mapping changes will only "
                            "take effect on MaxScale restart.", CN_LOCAL_ADDRESS, addr_str.c_str());
            }
            local_address_bin = std::move(ai);
        }
        else
        {
            MXB_ERROR("Could not get address information for local address %s: %s "
                      "Backend connections will use default local address.",
                      local_address.c_str(), errmsg.c_str());
        }
    }

    // TODO: this needs to be fixed at a higher level. For a
    // config value with a default and an on_set() function,
    // the on_set() function should be called at config time
    // else any side effect that the function has (like copying
    // the value somewhere) will not happen. The problem is not
    // trivial as config values are mostly initialized in a constructor,
    // leading to problems related to initialization order
    // in the constructor, across translation units and threads.
    this->qc_cache_properties.max_size = this->qc_cache_max_size.get();

    if (this->n_threads > this->n_threads_max)
    {
        // We should get this far only at startup.
        mxb_assert(!RoutingWorker::is_running());

        MXB_WARNING("MaxScale can have at most %d routing threads; the request for %d "
                    "will be reduced to that. The maximum can be increased with `threads_max`.",
                    (int)this->n_threads_max, (int)this->n_threads);

        this->n_threads = this->n_threads_max;
    }

    if (this->n_threads != RoutingWorker::nRunning())
    {
        if (RoutingWorker::is_running()) // false at startup
        {
            rv = RoutingWorker::adjust_threads(this->n_threads);
        }
    }

    return rv;
}

bool Config::ParamAutoTune::from_string(const std::string& value_as_string,
                                        value_type* pValue,
                                        std::string* pMessage) const
{
    value_type value;
    bool rv = ParamStringList::from_string(value_as_string, &value, pMessage);

    if (rv)
    {
        string message;
        std::vector<std::string> unknowns;
        auto dependencies = Service::specification()->server_dependencies();

        bool all_specified = false;
        bool some_specified = false;

        for (const auto& parameter : *pValue)
        {
            if (parameter == CN_ALL)
            {
                all_specified = true;
            }
            else
            {
                bool found = false;
                auto it = std::find_if(dependencies.begin(),
                                       dependencies.end(),
                                       [parameter](const auto* pDependency) {
                    return pDependency->parameter().name() == parameter;
                });

                if (it != dependencies.end())
                {
                    some_specified = true;
                }
                else
                {
                    unknowns.push_back(parameter);
                }
            }
        }

        if (all_specified && some_specified)
        {
            message = "If 'all' is specified for 'auto_tune', then no specific parameters can be specified.";
        }
        else if (!unknowns.empty())
        {
            message = "Unknown auto tunable parameter(s): " + mxb::join(unknowns, ",", "'");
        }

        if (message.empty())
        {
            *pValue = std::move(value);
        }
        else
        {
            if (pMessage)
            {
                *pMessage = std::move(message);
            }
            rv = false;
        }
    }

    return rv;
}

bool Config::ParamUsersRefreshTime::from_string(const std::string& value_as_string,
                                                value_type* pValue,
                                                std::string* pMessage) const
{
    bool rv = true;

    char* endptr;
    long value = strtol(value_as_string.c_str(), &endptr, 0);

    if (*endptr == '\0' && value < 0)
    {
        MXB_NOTICE("The value of '%s' is less than 0, users will be updated "
                   "as fast as the user account manager can.",
                   CN_USERS_REFRESH_TIME);
        // Strictly speaking they will be refreshed once every 68 years,
        // but I just don't beleave the uptime will be that long.
        *pValue = value_type(INT32_MAX);
    }
    else
    {
        rv = config::ParamSeconds::from_string(value_as_string, pValue, pMessage);
    }

    return rv;
}

bool Config::ParamKeyManager::takes_parameters() const
{
    return true;
}

bool Config::ParamKeyManager::validate_parameters(const std::string& value,
                                                  const mxs::ConfigParameters& params,
                                                  mxs::ConfigParameters* pUnrecognized) const
{
    return do_validate_parameters(value, params, pUnrecognized);
}

bool Config::ParamKeyManager::validate_parameters(const std::string& value, json_t* pParams,
                                                  std::set<std::string>* pUnrecognized) const
{
    return do_validate_parameters(value, pParams, pUnrecognized);
}

template<class Params, class Unknown>
bool Config::ParamKeyManager::do_validate_parameters(const std::string& value,
                                                     Params params, Unknown* pUnrecognized) const
{
    bool ok = false;
    value_type val;

    if (from_string(value, &val, nullptr))
    {
        if (val == KeyManager::Type::NONE)
        {
            if (mxs::key_manager())
            {
                MXB_ERROR("The key manager cannot be disabled at runtime once enabled.");
            }
            else
            {
                ok = true;
            }
        }
        else if (auto* spec = mxs::KeyManager::specification(val))
        {
            ok = spec->validate(params, pUnrecognized);
        }
    }

    return ok;
}

std::string Config::ParamLogThrottling::type() const
{
    return "throttling";
}

std::string Config::ParamLogThrottling::to_string(const value_type& value) const
{
    std::stringstream ss;
    ss << value.count << "," << value.window_ms << "ms," << value.suppress_ms << "ms";
    return ss.str();
}

bool Config::ParamLogThrottling::from_string(const std::string& value_as_string,
                                             value_type* pValue,
                                             std::string* pMessage) const
{
    bool rv = false;

    if (value_as_string.empty())
    {
        *pValue = MXB_LOG_THROTTLING {0, 0, 0};
        rv = true;
    }
    else
    {
        char v[value_as_string.size() + 1];
        strcpy(v, value_as_string.c_str());

        char* count = v;
        char* window_ms = NULL;
        char* suppress_ms = NULL;

        window_ms = strchr(count, ',');
        if (window_ms)
        {
            *window_ms = 0;
            ++window_ms;

            suppress_ms = strchr(window_ms, ',');
            if (suppress_ms)
            {
                *suppress_ms = 0;
                ++suppress_ms;
            }
        }

        if (!count || !window_ms || !suppress_ms)
        {
            MXB_ERROR("Invalid value for the `log_throttling` configuration entry: '%s'. "
                      "The format of the value for `log_throttling` is 'X, Y, Z', where "
                      "X is the maximum number of times a particular error can be logged "
                      "in the time window of Y milliseconds, before the logging is suppressed "
                      "for Z milliseconds.", value_as_string.c_str());
        }
        else
        {
            int c = atoi(count);
            time_t w;
            time_t s;

            if (c >= 0
                && get_milliseconds(name().c_str(), window_ms, value_as_string.c_str(), &w)
                && get_milliseconds(name().c_str(), suppress_ms, value_as_string.c_str(), &s))
            {
                MXB_LOG_THROTTLING throttling;
                throttling.count = c;
                throttling.window_ms = w;
                throttling.suppress_ms = s;

                *pValue = throttling;
                rv = true;
            }
            else
            {
                MXB_ERROR("Invalid value for the `log_throttling` configuration entry: '%s'. "
                          "The configuration entry `log_throttling` requires as value one zero or "
                          "positive integer and two durations.", value_as_string.c_str());
            }
        }
    }

    return rv;
}

json_t* Config::ParamLogThrottling::to_json(const value_type& value) const
{
    json_t* pJson = json_object();
    json_object_set_new(pJson, "count", json_integer(value.count));
    json_object_set_new(pJson, "window", json_integer(value.window_ms));
    json_object_set_new(pJson, "suppress", json_integer(value.suppress_ms));
    return pJson;
}

bool Config::ParamLogThrottling::from_json(const json_t* pJson,
                                           value_type* pValue,
                                           std::string* pMessage) const
{
    bool rv = false;

    if (json_is_object(pJson))
    {
        json_t* pCount = json_object_get(pJson, "count");
        json_t* pWindow = json_object_get(pJson, "window");
        json_t* pSuppress = json_object_get(pJson, "suppress");

        if (pCount && json_is_integer(pCount)
            && pWindow && (json_is_integer(pWindow) || json_is_string(pWindow))
            && pSuppress && (json_is_integer(pSuppress) || json_is_string(pSuppress)))
        {
            time_t w;
            time_t s;

            rv = true;
            pValue->count = json_integer_value(pCount);

            if (json_is_integer(pWindow))
            {
                pValue->window_ms = json_integer_value(pWindow);
            }
            else if (get_milliseconds(name().c_str(), json_string_value(pWindow), json_string_value(pWindow),
                                      &w))
            {
                pValue->window_ms = w;
            }
            else
            {
                rv = false;
            }

            if (json_is_integer(pSuppress))
            {
                pValue->suppress_ms = json_integer_value(pSuppress);
            }
            else if (get_milliseconds(name().c_str(), json_string_value(pSuppress),
                                      json_string_value(pSuppress), &s))
            {
                pValue->suppress_ms = s;
            }
            else
            {
                rv = false;
            }
        }
        else if (pMessage)
        {
            *pMessage =
                "Expected an object like '{ count = <integer>, window = <integer>, "
                "suppress = <integer> }' but one or more of the keys were missing and/or "
                "one or more of the values were not an integer.";
        }
    }
    else if (json_is_string(pJson))
    {
        rv = from_string(json_string_value(pJson), pValue, pMessage);
    }
    else
    {
        *pMessage = "Expected a json object, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

bool Config::ParamThreadsCount::from_string(const std::string& value_as_string,
                                            value_type* pValue,
                                            std::string* pMessage) const
{
    bool rv = true;

    int processor_count = get_processor_count();

    if (value_as_string == CN_AUTO)
    {
        *pValue = processor_count;
    }
    else
    {
        value_type value;
        rv = ParamCount::from_string(value_as_string, &value, pMessage);

        if (rv)
        {
            if (value > processor_count)
            {
                MXB_WARNING("Number of threads set to %d, which is greater than "
                            "the number of processors available: %d",
                            (int)value,
                            processor_count);
            }

            *pValue = value;
        }
    }

    return rv;
}
}

static bool process_config_context(ConfigSectionMap& context);
static bool check_config_objects(ConfigSectionMap& context);

static bool check_first_last_char(const char* string, char expected);
static void remove_first_last_char(char* value);
static bool test_regex_string_validity(const char* regex_string, const char* key);
static bool get_milliseconds(const char* zName,
                             const char* zValue,
                             const char* zDisplay_value,
                             std::chrono::milliseconds* pMilliseconds);

int create_new_service(ConfigSection* obj);
int create_new_server(ConfigSection* obj);
int create_new_monitor(ConfigSection* obj);
int create_new_listener(ConfigSection* obj);
int create_new_filter(ConfigSection* obj);


/*
 * This is currently only used in config_load_global() to verify that
 * all global configuration item names are valid.
 */
const char* config_pre_parse_global_params[] =
{
    CN_LOGDIR,
    CN_LIBDIR,
    CN_SHAREDIR,
    CN_PIDDIR,
    CN_DATADIR,
    CN_CACHEDIR,
    CN_LANGUAGE,
    CN_EXECDIR,
    CN_CONNECTOR_PLUGINDIR,
    CN_PERSISTDIR,
    CN_MODULE_CONFIGDIR,
    CN_SYSLOG,
    CN_MAXLOG,
    CN_LOG_AUGMENTATION,
    CN_SUBSTITUTE_VARIABLES,
    NULL
};

ConfigSection::ConfigSection(string header, SourceType source_type)
    : m_name(std::move(header))
    , source_type(source_type)
{
}

ConfigSection::ConfigSection(std::string header, SourceType source_type, std::string source_file, int lineno)
    : m_name(move(header))
    , source_type(source_type)
    , source_file(move(source_file))
    , source_lineno(lineno)
{
}

void fix_object_name(char* name)
{
    mxb::trim(name);
}

void fix_object_name(std::string& name)
{
    char buf[name.size() + 1];
    strcpy(buf, name.c_str());
    fix_object_name(buf);
    name.assign(buf);
}

namespace
{

bool is_empty_string(const string& str)
{
    return std::all_of(str.begin(), str.end(), isspace);
}

std::string get_section_type(const maxbase::ini::map_result::ConfigSection& section)
{
    auto it = section.key_values.find(CN_TYPE);

    return it != section.key_values.end() ? it->second.value : std::string {};
}

std::pair<int, mxb::ini::map_result::Configuration>
process_includes(const mxb::ini::map_result::Configuration& input)
{
    int errors = 0;
    mxb::ini::map_result::Configuration processed_input;

    std::set<std::string> include_sections;

    for (const auto& section : input)
    {
        const auto& header = section.first;
        const auto& config = section.second;
        auto type = get_section_type(config);

        if (type == CN_INCLUDE)
        {
            include_sections.insert(header);
        }

        auto it = config.key_values.find(CN_AT_INCLUDE);

        if (it != config.key_values.end())
        {
            if (type.empty())
            {
                MXB_ERROR("Section [%s] has no type.", header.c_str());
                ++errors;
            }
            else if (type == CN_INCLUDE)
            {
                // Could be allowed, but would require cycle detection, so postponed until
                // there is a clear need for it.
                MXB_ERROR("Section [%s] is of type 'include' and can thus not include other sections.",
                          header.c_str());
                ++errors;
            }
            else
            {
                auto includes = mxb::strtok(it->second.value, ",");

                maxbase::ini::map_result::ConfigSection merged_config;
                merged_config.lineno = config.lineno;

                for (auto include : includes)
                {
                    mxb::trim(include);

                    auto jt = input.find(include);

                    if (jt != input.end())
                    {
                        const maxbase::ini::map_result::ConfigSection& included_config = jt->second;

                        type = get_section_type(included_config);

                        if (type == CN_INCLUDE)
                        {
                            include_sections.erase(include);

                            for (const auto& kv : included_config.key_values)
                            {
                                merged_config.key_values[kv.first] = kv.second;
                            }
                        }
                        else
                        {
                            MXB_ERROR("Section [%s] includes section [%s] whose type is not 'include', "
                                      "but '%s'.",
                                      header.c_str(), include.c_str(), type.c_str());
                            ++errors;
                        }
                    }
                    else
                    {
                        MXB_ERROR("Section [%s] includes section [%s], which does not exist.",
                                  header.c_str(), include.c_str());
                        ++errors;
                    }
                }

                for (const auto& kv : config.key_values)
                {
                    if (kv.first != CN_AT_INCLUDE)
                    {
                        merged_config.key_values[kv.first] = kv.second;
                    }
                }

                processed_input.emplace(header, merged_config);
            }
        }
        else
        {
            processed_input.emplace(section);
        }
    }

    if (!include_sections.empty())
    {
        MXB_WARNING("The following 'include' sections were not used: %s",
                    mxb::join(include_sections, ", ").c_str());
    }

    return std::make_pair(errors, processed_input);
}

}

bool config_add_to_context(const std::string& source_file, ConfigSection::SourceType source_type,
                           const mxb::ini::map_result::Configuration& raw_input, ConfigSectionMap& output)
{
    using Type = ConfigSection::SourceType;
    auto type_to_str = [](Type type) {
        switch (type)
        {
        case Type::MAIN:
            return "main";

        case Type::ADDITIONAL:
            return "additional";

        case Type::RUNTIME:
            return "runtime";
        }
        return "";
    };

    auto [errors, input] = process_includes(raw_input);

    for (const auto& section : input)
    {
        const auto& header = section.first;
        string reason;
        if (!config_is_valid_name(header, &reason))
        {
            MXB_ERROR("%s", reason.c_str());
            errors++;
        }
        else
        {
            // Search for a matching header in the config.
            auto it = output.find(header);

            bool header_ok = false;
            if (it != output.end())
            {
                // If the previous entry is from a static file (main or additional) and the new entry is
                // from a runtime file, then overwrite. Otherwise, we have an error.
                auto& prev_entry = it->second;
                auto prev_type = prev_entry.source_type;
                if ((prev_type == Type::MAIN || prev_type == Type::ADDITIONAL)
                    && source_type == Type::RUNTIME)
                {
                    const char msg[] = "Overwriting configuration section '%s' from %s file '%s' "
                                       "with contents from runtime file '%s'. To prevent this warning "
                                       "message, manually move the runtime changes to the %s file.";
                    auto* prev_type_str = type_to_str(prev_type);
                    MXB_WARNING(msg, header.c_str(), prev_type_str, prev_entry.source_file.c_str(),
                                source_file.c_str(), prev_type_str);

                    output.erase(it);
                    ConfigSection replacement(header, source_type, source_file, section.second.lineno);
                    auto res = output.emplace(header, move(replacement));
                    it = res.first;
                    header_ok = true;
                }
                else
                {
                    MXB_ERROR("Configuration section '%s' in %s file '%s' is a duplicate. "
                              "Previous definition in %s file '%s'.",
                              header.c_str(), type_to_str(source_type), source_file.c_str(),
                              type_to_str(prev_type), prev_entry.source_file.c_str());
                }
            }
            else
            {
                // Add new entry.
                ConfigSection new_ctxt(header, source_type, source_file, section.second.lineno);

                auto is_url_char = [](char c) {
                    return isalnum(c) || c == '_' || c == '.' || c == '~' || c == '-';
                };
                if (!std::all_of(new_ctxt.m_name.begin(), new_ctxt.m_name.end(), is_url_char))
                {
                    MXB_WARNING("Configuration section name '%s' in %s file '%s' contains URL-unsafe "
                                "characters. It cannot be safely used with the REST API or MaxCtrl.",
                                new_ctxt.name(), type_to_str(source_type), source_file.c_str());
                }

                auto ret = output.emplace(header, move(new_ctxt));
                it = ret.first;
                header_ok = true;
            }

            if (header_ok && header == CN_MAXSCALE && source_type == Type::ADDITIONAL)
            {
                MXB_ERROR("Additional configuration file '%s' contains a [maxscale] section. Only the main "
                          "configuration file or a runtime file may contain this section.",
                          source_file.c_str());
                header_ok = false;
            }

            if (header_ok)
            {
                const auto& kvs = section.second.key_values;
                auto& params_out = it->second.m_parameters;

                for (const auto& kv : kvs)
                {
                    const string& name = kv.first;
                    const string& value = kv.second.value;

                    params_out.set(name, value);
                }
            }
            else
            {
                errors++;
            }
        }
    }
    return errors == 0;
}

namespace
{
struct ConfFilePath
{
    string total_path;
    string filename;
};

// Global variables used by config files search. Required to work around nftw limitations.
std::vector<ConfFilePath> config_files_list;
std::unordered_set<std::string> hidden_dirs;

int config_files_search_cb(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf)
{
    if (typeflag == FTW_SL)     // A symbolic link; let's see what it points to.
    {
        // Another stat-call is required as the sb given by nftw contains info about the link itself, not
        // the linked file.
        struct stat sb2 {};
        if (stat(fpath, &sb2) == 0)
        {
            auto file_type = (sb2.st_mode & S_IFMT);
            switch (file_type)
            {
            case S_IFREG:
                // Points to a file; we'll handle that regardless of where the file resides.
                typeflag = FTW_F;
                break;

            case S_IFDIR:
                // Points to a directory; we'll ignore that.
                MXB_WARNING("Symbolic link %s in configuration directory points to a "
                            "directory; it will be ignored.", fpath);
                break;

            default:
                ;   // Points to something else; we'll silently ignore.
            }
        }
        else
        {
            MXB_WARNING("Could not get information about the symbolic link %s; "
                        "it will be ignored.", fpath);
        }
    }

    string path_to(fpath, fpath + ftwbuf->base - 1);
    if (typeflag == FTW_D)
    {
        // Hidden directory or a directory inside a hidden directory.
        if (fpath[ftwbuf->base] == '.' || hidden_dirs.count(path_to))
        {
            hidden_dirs.emplace(fpath);
        }
    }
    else if (typeflag == FTW_F)
    {
        // We are only interested in files...
        const char* filename = fpath + ftwbuf->base;
        const char* dot = strrchr(filename, '.');

        if (hidden_dirs.count(path_to))
        {
            MXB_INFO("Ignoring file inside hidden directory: %s", fpath);
        }
        else if (dot && *filename != '.')   // that have a suffix .cnf and are not hidden.
        {
            const char* suffix = dot + 1;
            if (strcmp(suffix, "cnf") == 0)
            {
                ConfFilePath file_info;
                file_info.total_path = fpath;
                file_info.filename = filename;
                config_files_list.push_back(std::move(file_info));
            }
        }
    }
    return 0;
}

/**
 * Loads all configuration files in a directory hierarchy.
 *
 * Only files with the suffix ".cnf" are considered to be configuration files.
 *
 * @param dir      The directory.
 * @param source_type Configuration load type.
 * @param output Output object.
 *
 * @return True, if all configuration files in the directory hierarchy could be loaded,
 *         otherwise false.
 */
bool config_load_dir(const string& dir, ConfigSection::SourceType source_type, ConfigSectionMap& output)
{
    const int nopenfd = 5;      // Maximum concurrently opened directory descriptors
    int rc = nftw(dir.c_str(), config_files_search_cb, nopenfd, FTW_PHYS);
    hidden_dirs.clear();
    const auto file_list = move(config_files_list);

    bool success = false;
    if (rc == 0)
    {
        success = true;
        // Substitution does not apply to runtime files as it's not supposed to work with REST-API.
        bool substitute_vars = mxs::Config::get().substitute_variables
            && source_type != ConfigSection::SourceType::RUNTIME;

        for (auto it = file_list.begin(); it != file_list.end() && success; it++)
        {
            auto& file = *it;
            // Load config file.
            auto [load_res, warning] = parse_mxs_config_file_to_map(file.total_path);
            if (load_res.errors.empty())
            {
                if (!warning.empty())
                {
                    // Having a [maxscale]-section in an additional file is always an error. Printing the
                    // warning may still be useful.
                    MXB_WARNING("In file '%s': %s", file.total_path.c_str(), warning.c_str());
                }

                if (substitute_vars)
                {
                    auto errors = maxbase::ini::substitute_env_vars(load_res.config);
                    if (!errors.empty())
                    {
                        string errmsg = mxb::string_printf("Variable substitution to file '%s' failed. ",
                                                           file.total_path.c_str());
                        errmsg += mxb::create_list_string(errors, " ");
                        MXB_ERROR("%s", errmsg.c_str());
                        success = false;
                    }
                }

                if (success && !config_add_to_context(file.total_path, source_type, load_res.config, output))
                {
                    success = false;
                }
            }
            else
            {
                success = false;
                string all_errors = mxb::create_list_string(load_res.errors, " ");
                MXB_ERROR("Failed to read configuration file '%s': %s",
                          file.total_path.c_str(), all_errors.c_str());
            }
        }
    }
    else
    {
        int eno = errno;
        MXB_ERROR("File tree walk (nftw) failed for '%s'. Error %i: %s",
                  dir.c_str(), eno, mxb_strerror(eno));
    }
    return success;
}

/**
 * Take into use global ([maxscale]-section) configuration.
 *
 * @param global_params Text-form parameters
 * @return True on success
 */
bool apply_global_config(const mxs::ConfigParameters& global_params)
{
    mxs::Config& global_config = mxs::Config::get();
    bool rval = true;

    if (!global_config.specification().validate(global_params))
    {
        rval = false;
    }
    else
    {
        rval = global_config.configure(global_params);
    }
    return rval;
}
}

/**
 * Check if a directory exists
 *
 * This function also logs warnings if the directory cannot be accessed or if
 * the file is not a directory.
 * @param dir Directory to check
 * @return True if the file is an existing directory
 */
static bool is_directory(const char* dir)
{
    bool rval = false;
    struct stat st {};
    if (stat(dir, &st) == -1)
    {
        if (errno == ENOENT)
        {
            MXB_NOTICE("%s does not exist, not reading.", dir);
        }
        else
        {
            MXB_WARNING("Could not access %s, not reading: %s",
                        dir,
                        mxb_strerror(errno));
        }
    }
    else
    {
        if (S_ISDIR(st.st_mode))
        {
            rval = true;
        }
        else
        {
            MXB_WARNING("%s exists, but it is not a directory. Ignoring.", dir);
        }
    }

    return rval;
}

bool export_config_file(const char* filename, ConfigSectionMap& config)
{
    bool rval = true;
    std::vector<ConfigSection*> contexts;

    // The config objects are stored in reverse order so first convert it back
    // to the correct order. TODO: preserve order somehow
    for (auto& elem : config)
    {
        contexts.push_back(&elem.second);
    }

    std::ostringstream ss;
    ss << "# Generated by MaxScale " << MAXSCALE_VERSION << '\n';
    ss << "# Documentation: https://mariadb.com/kb/en/mariadb-enterprise/maxscale/ \n\n";

    for (ConfigSection* ctx : contexts)
    {
        ss << '[' << ctx->m_name << "]\n";
        for (const auto& elem : ctx->m_parameters)
        {
            ss << elem.first << '=' << elem.second << '\n';
        }
        ss << '\n';
    }

    int fd = open(filename, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    if (fd != -1)
    {
        std::string payload = ss.str();

        if (write(fd, payload.c_str(), payload.size()) == -1)
        {
            MXB_ERROR("Failed to write to file '%s': %d, %s",
                      filename, errno, mxb_strerror(errno));
            rval = false;
        }

        close(fd);
    }
    else
    {
        MXB_ERROR("Failed to open configuration export file '%s': %d, %s",
                  filename, errno, mxb_strerror(errno));
        rval = false;
    }

    return rval;
}

bool config_load(const string& main_cfg_file,
                 const mxb::ini::map_result::Configuration& main_cfg_in,
                 ConfigSectionMap& output)
{
    using Type = ConfigSection::SourceType;
    bool success = false;

    if (config_add_to_context(main_cfg_file, Type::MAIN, main_cfg_in, output))
    {
        success = true;

        // Search for more config files in a directory named <main_cfg_filename>.d.
        string config_dir = main_cfg_file + ".d";
        if (is_directory(config_dir.c_str()))
        {
            success = config_load_dir(config_dir, Type::ADDITIONAL, output);
        }

        // If loading the additional config files failed, do not load runtime files.
        const char* persist_cnf = mxs::config_persistdir();
        if (success && mxs::Config::get().load_persisted_configs && is_directory(persist_cnf))
        {
            success = config_load_dir(persist_cnf, Type::RUNTIME, output);
        }

        if (success)
        {
            for (const auto& [k, v] : output)
            {
                mxs::Config::set_object_source_file(k, v.source_file);

                if (v.source_type == ConfigSection::SourceType::RUNTIME)
                {
                    this_unit.dynamic_objects.insert(k);
                }
                else
                {
                    this_unit.static_objects.insert(k);
                }
            }

            if (!check_config_objects(output))
            {
                success = false;
            }
        }
    }
    return success;
}

bool config_process(ConfigSectionMap& output)
{
    return process_config_context(output);
}

bool config_load_and_process(const string& main_cfg_file,
                             const mxb::ini::map_result::Configuration& main_cfg_in,
                             ConfigSectionMap& output)
{
    bool rv = config_load(main_cfg_file, main_cfg_in, output);

    if (rv)
    {
        rv = config_process(output);
    }

    return rv;
}

bool apply_main_config(const ConfigSectionMap& config)
{
    bool rv = false;
    auto it = config.find(CN_MAXSCALE);

    if (it != config.end())
    {
        const ConfigSection& maxscale_section = it->second;

        rv = apply_global_config(maxscale_section.m_parameters);
    }
    else
    {
        rv = apply_global_config(mxs::ConfigParameters {});
    }

    return rv;
}

bool valid_object_type(const std::string& type)
{
    return type == CN_SERVICE || type == CN_LISTENER || type == CN_SERVER
           || type == CN_MONITOR || type == CN_FILTER;
}

const char* get_missing_module_parameter_name(const ConfigSection* obj)
{
    std::string type = obj->m_parameters.get_string(CN_TYPE);

    if (type == CN_SERVICE && !obj->m_parameters.contains(CN_ROUTER))
    {
        return CN_ROUTER;
    }
    else if ((type == CN_MONITOR || type == CN_FILTER) && !obj->m_parameters.contains(CN_MODULE))
    {
        return CN_MODULE;
    }
    return nullptr;
}

bool is_valid_module(const ConfigSection* obj)
{
    using mxs::ModuleType;
    ModuleType expected = ModuleType::UNKNOWN;
    string type_str = obj->m_parameters.get_string(CN_TYPE);
    string param_name;

    if (type_str == CN_SERVICE)
    {
        param_name = CN_ROUTER;
        expected = ModuleType::ROUTER;
    }
    else if (type_str == CN_MONITOR)
    {
        param_name = CN_MODULE;
        expected = ModuleType::MONITOR;
    }
    else if (type_str == CN_FILTER)
    {
        param_name = CN_MODULE;
        expected = ModuleType::FILTER;
    }

    bool rval = true;

    if (!param_name.empty())
    {
        string param_value = obj->m_parameters.get_string(param_name);
        if (!get_module(param_value, expected))
        {
            // An error is already printed by get_module, but we can print additional info.
            MXB_ERROR("'%s' is not a valid %s for %s '%s'",
                      param_value.c_str(), param_name.c_str(), type_str.c_str(), obj->m_name.c_str());
            rval = false;
        }
    }
    return rval;
}

const MXS_MODULE* get_module_details(const ConfigSection* obj)
{
    std::string type = obj->m_parameters.get_string(CN_TYPE);

    if (type == CN_SERVICE)
    {
        auto name = obj->m_parameters.get_string(CN_ROUTER);
        return get_module(name, mxs::ModuleType::ROUTER);
    }
    else if (type == CN_MONITOR)
    {
        auto name = obj->m_parameters.get_string(CN_MODULE);
        return get_module(name, mxs::ModuleType::MONITOR);
    }
    else if (type == CN_FILTER)
    {
        auto name = obj->m_parameters.get_string(CN_MODULE);
        return get_module(name, mxs::ModuleType::FILTER);
    }

    mxb_assert(!true);
    return nullptr;
}

ConfigSection* name_to_object(const std::vector<ConfigSection*>& objects,
                              const ConfigSection* obj,
                              std::string name)
{
    ConfigSection* rval = nullptr;

    fix_object_name(name);

    auto equal_name = [&](ConfigSection* c) {
        std::string s = c->m_name;
        fix_object_name(s);
        return s == name;
    };

    auto it = std::find_if(objects.begin(), objects.end(), equal_name);

    if (it == objects.end())
    {
        MXB_ERROR("Could not find object '%s' that '%s' depends on. "
                  "Check that the configuration object exists.",
                  name.c_str(),
                  obj->name());
    }
    else
    {
        rval = *it;
    }

    return rval;
}

std::unordered_set<ConfigSection*> get_spec_dependencies(const std::vector<ConfigSection*>& objects,
                                                         const ConfigSection* obj,
                                                         const mxs::config::Specification* spec)
{
    std::unordered_set<ConfigSection*> rval;

    for (const auto& p : *spec)
    {
        if (obj->m_parameters.contains(p.second->name()))
        {
            std::string val = obj->m_parameters.get_string(p.second->name());

            for (const auto& dep : p.second->get_dependencies(val))
            {
                rval.insert(name_to_object(objects, obj, dep));
            }
        }
    }

    return rval;
}

std::unordered_set<ConfigSection*> get_dependencies(const std::vector<ConfigSection*>& objects,
                                                    const ConfigSection* obj)
{
    std::unordered_set<ConfigSection*> rval;
    std::string type = obj->m_parameters.get_string(CN_TYPE);

    if (type == CN_INCLUDE)
    {
        // Includes do not have dependencies by themselves.
        return rval;
    }
    else if (type == CN_SERVER)
    {
        // Servers are leaf objects in the dependency tree, they never have dependencies
        return rval;
    }
    else if (type == CN_LISTENER)
    {
        return get_spec_dependencies(objects, obj, mxs::Listener::specification());
    }

    const MXS_MODULE* module = get_module_details(obj);
    mxb_assert(module);
    mxb_assert(module->specification);

    auto deps = get_spec_dependencies(objects, obj, module->specification);
    rval.insert(deps.begin(), deps.end());

    if (type == CN_SERVICE && obj->m_parameters.contains(CN_FILTERS))
    {
        for (std::string name : mxs::strtok(obj->m_parameters.get_string(CN_FILTERS), "|"))
        {
            rval.insert(name_to_object(objects, obj, name));
        }
    }

    if (type == CN_SERVICE && obj->m_parameters.contains(CN_TARGETS))
    {
        DuplicateChecker checker;

        for (auto name : mxs::strtok(obj->m_parameters.get_string(CN_TARGETS), ","))
        {
            checker.check(type, obj->m_name, CN_TARGETS, name);
            rval.insert(name_to_object(objects, obj, name));
        }
    }

    if (type == CN_SERVICE && obj->m_parameters.contains(CN_CLUSTER))
    {
        rval.insert(name_to_object(objects, obj, obj->m_parameters.get_string(CN_CLUSTER)));
    }

    if ((type == CN_MONITOR || type == CN_SERVICE) && obj->m_parameters.contains(CN_SERVERS))
    {
        DuplicateChecker checker;

        for (std::string name : mxs::strtok(obj->m_parameters.get_string(CN_SERVERS), ","))
        {
            checker.check(type, obj->m_name, CN_SERVERS, name);
            rval.insert(name_to_object(objects, obj, name));
        }
    }

    return rval;
}

namespace
{

// Represents a node in a graph
template<class T>
struct Node
{
    static const int NOT_VISITED = 0;

    T    value;
    int  index;
    int  lowlink;
    bool on_stack;

    Node(T value)
        : value(value)
        , index(NOT_VISITED)
        , lowlink(NOT_VISITED)
        , on_stack(false)
    {
    }
};

template<class T>
using Container = std::unordered_map<T, std::unordered_set<T>>;
template<class T>
using Groups = std::vector<std::vector<T>>;
template<class T>
using Graph = std::unordered_multimap<Node<T>*, Node<T>*>;

/**
 * Calculate strongly connected components (i.e. cycles) of a graph
 *
 * @see https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
 *
 * @param graph An std::unordered_multimap where the keys represent nodes and
 *              the set of values for that key as the edges from that node
 *
 * @return A list of groups where each group is an ordered list of values
 */
template<class T>
Groups<T> get_graph_cycles(Container<T> graph)
{
    using namespace std::placeholders;

    std::vector<Node<T>> nodes;

    auto find_node = [&](T target, const Node<T>& n) {
        return n.value == target;
    };

    // Iterate over all values and place unique values in the vector.
    for (auto&& a : graph)
    {
        nodes.emplace_back(a.first);
    }

    Graph<T> node_graph;

    for (auto&& a : graph)
    {
        auto first = std::find_if(nodes.begin(), nodes.end(), std::bind(find_node, a.first, _1));

        for (auto&& b : a.second)
        {
            auto second = std::find_if(nodes.begin(), nodes.end(), std::bind(find_node, b, _1));
            node_graph.emplace(&(*first), &(*second));
        }
    }

    std::vector<Node<T>*> stack;
    Groups<T> groups;

    std::function<void(Node<T>*)> visit_node = [&](Node<T>* n) {
        static int s_index = 1;
        n->index = s_index++;
        n->lowlink = n->index;
        stack.push_back(n);
        n->on_stack = true;
        auto range = node_graph.equal_range(n);

        for (auto it = range.first; it != range.second; it++)
        {
            Node<T>* s = it->second;

            if (s->index == Node<T>::NOT_VISITED)
            {
                visit_node(s);
                n->lowlink = std::min(n->lowlink, s->lowlink);
            }
            else if (n == s)
            {
                // This isn't strictly according to the algorithm but this is a convenient spot where we
                // can easily spot cycles of size one. Adding an extra group with the two nodes in it
                // causes it to be reported correctly.
                groups.push_back({n->value, s->value});
            }
            else if (s->on_stack)
            {
                n->lowlink = std::min(n->lowlink, s->index);
            }
        }

        if (n->index == n->lowlink)
        {
            // Start a new group
            groups.emplace_back();

            Node<T>* c;

            do
            {
                c = stack.back();
                stack.pop_back();
                c->on_stack = false;
                groups.back().push_back(c->value);
            }
            while (c != n);
        }
    };

    for (auto n = nodes.begin(); n != nodes.end(); n++)
    {
        if (n->index == Node<T>::NOT_VISITED)
        {
            visit_node((Node<T>*) & (*n));
        }
    }

    return groups;
}
}

/**
 * Resolve dependencies in the configuration and validate them
 *
 * @param objects List of objects, sorted so that dependencies are constructed first
 *
 * @return True if the configuration has bad dependencies
 */
bool resolve_dependencies(std::vector<ConfigSection*>& objects)
{
    int errors = 0;
    std::unordered_map<ConfigSection*, std::unordered_set<ConfigSection*>> g;

    for (const auto& obj : objects)
    {
        auto deps = get_dependencies(objects, obj);

        if (deps.count(nullptr))
        {
            // a missing reference, reported in get_dependencies
            errors++;
        }
        else
        {
            g.insert(std::make_pair(obj, deps));
        }
    }

    if (errors == 0)
    {
        std::vector<ConfigSection*> result;

        for (const auto& group : get_graph_cycles<ConfigSection*>(g))
        {
            if (group.size() > 1)
            {
                auto join = [](std::string total, ConfigSection* c) {
                    return total + " -> " + c->m_name;
                };

                std::string first = group[0]->m_name;
                std::string str_group = std::accumulate(std::next(group.begin()), group.end(), first, join);
                str_group += " -> " + first;
                MXB_ERROR("A circular dependency chain was found in the configuration: %s",
                          str_group.c_str());
                errors++;
            }
            else
            {
                mxb_assert(!group.empty());
                /** Due to the algorithm that was used, the strongly connected
                 * components are always identified before the nodes that depend
                 * on them. This means that the result is sorted at the same
                 * time the circular dependencies are resolved. */
                result.push_back(group[0]);
            }
        }

        // The end result should contain the same set of nodes we started with
        mxb_assert(std::set<ConfigSection*>(result.begin(), result.end())
                   == std::set<ConfigSection*>(objects.begin(), objects.end())
                   || errors > 0);

        objects = std::move(result);
    }

    return errors > 0;
}

/**
 * @brief Process a configuration context and turn it into the set of objects
 *
 * @param context The parsed configuration context
 * @return False on fatal error, true on success
 */
static bool process_config_context(ConfigSectionMap& context)
{
    std::vector<ConfigSection*> objects;
    int error_count = 0;

    // Ignore the 'maxscale' section when resolving dependencies.
    for (auto& elem : context)
    {
        if (elem.first != CN_MAXSCALE)
        {
            objects.push_back(&elem.second);
        }
    }

    // At this point, sort the objects in the array such that the order resembles the original definition
    // order, at least to some extent. TODO: Think more of how the ordering should work with runtime
    // modified and created objects.
    auto compare = [](const ConfigSection* lhs, const ConfigSection* rhs) {
        bool rval = false;
        // 1. Objects in main config file go first, then dir files, then runtime files.
        using Type = ConfigSection::SourceType;
        auto type_lhs = lhs->source_type;
        auto type_rhs = rhs->source_type;
        if (type_lhs != type_rhs)
        {
            if (type_lhs == Type::MAIN || (type_lhs == Type::ADDITIONAL && type_rhs == Type::RUNTIME))
            {
                rval = true;
            }
        }
        else
        {
            // 2. Same file type. Order by file name.
            int comp_res = lhs->source_file.compare(rhs->source_file);
            if (comp_res != 0)
            {
                if (comp_res < 0)
                {
                    rval = true;
                }
            }
            else
            {
                // 3. Same file. Order by line number.
                if (lhs->source_lineno < rhs->source_lineno)
                {
                    rval = true;
                }
            }
        }
        return rval;
    };
    std::sort(objects.begin(), objects.end(), compare);

    /**
     * Build the servers first to keep them in configuration file order. As
     * servers can't have references, this is safe to do as the first step.
     */
    for (ConfigSection* obj : objects)
    {
        std::string type = obj->m_parameters.get_string(CN_TYPE);
        mxb_assert(!type.empty());

        if (type == CN_SERVER)
        {
            error_count += create_new_server(obj);
        }
    }

    // Resolve any remaining dependencies between the objects
    if (resolve_dependencies(objects) || error_count)
    {
        return false;
    }

    /**
     * Process the data and create the services defined in the data.
     */
    for (ConfigSection* obj : objects)
    {
        std::string type = obj->m_parameters.get_string(CN_TYPE);
        mxb_assert(!type.empty());

        if (type == CN_SERVICE)
        {
            error_count += create_new_service(obj);
        }
        else if (type == CN_FILTER)
        {
            error_count += create_new_filter(obj);
        }
        else if (type == CN_LISTENER)
        {
            error_count += create_new_listener(obj);
        }
        else if (type == CN_MONITOR)
        {
            error_count += create_new_monitor(obj);
        }

        if (error_count)
        {
            /**
             * We need to stop creating objects after the first error since
             * any objects that depend on the object that failed would fail in
             * a very confusing manner.
             */
            break;
        }
    }

    if (error_count == 0)
    {
        MonitorManager::populate_services();
    }
    else
    {
        MXB_ERROR("%d errors were encountered while processing configuration.", error_count);
    }

    return error_count == 0;
}

bool mxs::ConfigParameters::get_bool(const std::string& key) const
{
    string param_value = get_string(key);
    return param_value.empty() ? false : config_truth_value(param_value.c_str());
}

bool mxs::ConfigParameters::contains(const string& key) const
{
    return m_contents.find(key) != m_contents.end();
}

// static
mxs::ConfigParameters mxs::ConfigParameters::from_json(json_t* json)
{
    mxs::ConfigParameters rval;
    const char* key;
    json_t* value;

    json_object_foreach(json, key, value)
    {
        if (!json_is_null(value) && !json_is_array(value) && !json_is_object(value))
        {
            auto strval = mxb::json_to_string(value);

            if (!strval.empty())
            {
                rval.set(key, strval);
            }
            else
            {
                mxb_assert_message(json_is_string(value), "Only strings can be empty (%s)", key);
            }
        }
    }

    return rval;
}

string mxs::ConfigParameters::get_string(const std::string& key) const
{
    string rval;
    auto iter = m_contents.find(key);
    if (iter != m_contents.end())
    {
        rval = iter->second;
    }
    return rval;
}

void mxs::ConfigParameters::set(const std::string& key, const std::string& value)
{
    m_contents[key] = value;
}

void mxs::ConfigParameters::remove(const string& key)
{
    m_contents.erase(key);
}

void mxs::ConfigParameters::clear()
{
    m_contents.clear();
}

bool mxs::ConfigParameters::empty() const
{
    return m_contents.empty();
}

mxs::ConfigParameters::ContainerType::const_iterator mxs::ConfigParameters::begin() const
{
    return m_contents.begin();
}

mxs::ConfigParameters::ContainerType::const_iterator mxs::ConfigParameters::end() const
{
    return m_contents.end();
}

/**
 * Return the number of configured threads
 *
 * @return The number of threads configured in the config file
 */
int config_threadcount()
{
    return mxs::Config::get().n_threads;
}

uint32_t config_writeq_high_water()
{
    return mxs::Config::get().writeq_high_water.get();
}

uint32_t config_writeq_low_water()
{
    return mxs::Config::get().writeq_low_water.get();
}

/**
 * @brief Check that the configuration objects have valid parameters
 *
 * @param context Configuration context
 * @return True if the configuration is OK, false if errors were detected
 */
static bool check_config_objects(ConfigSectionMap& context)
{
    bool rval = true;

    for (auto& elem : context)
    {
        auto& obj = elem.second;
        if (obj.m_name == CN_MAXSCALE)
        {
            continue;
        }

        std::string type = obj.m_parameters.get_string(CN_TYPE);
        const char* filec = obj.source_file.c_str();

        if (type == CN_INCLUDE)
        {
            // An include, no processing at this point.
        }
        else if (!valid_object_type(type))
        {
            MXB_ERROR("Invalid module type '%s' for object '%s' in file '%s'.",
                      type.c_str(), obj.name(), filec);
            rval = false;
        }
        else if (const char* missing_module_def = get_missing_module_parameter_name(&obj))
        {
            MXB_ERROR("'%s' in file '%s' is missing a required parameter '%s'.",
                      obj.name(), filec, missing_module_def);
            rval = false;
        }
        else if (!is_valid_module(&obj))
        {
            rval = false;
        }
    }

    return rval;
}

int config_truth_value(const char* str)
{
    if (strcasecmp(str, "true") == 0 || strcasecmp(str, "on") == 0
        || strcasecmp(str, "yes") == 0 || strcasecmp(str, "1") == 0)
    {
        return 1;
    }
    if (strcasecmp(str, "false") == 0 || strcasecmp(str, "off") == 0
        || strcasecmp(str, "no") == 0 || strcasecmp(str, "0") == 0)
    {
        return 0;
    }

    return -1;
}

/**
 * Get the MAC address of first network interface
 *
 * and fill the provided allocated buffer with SHA1 encoding
 * @param output        Allocated 6 bytes buffer
 * @return 1 on success, 0 on failure
 *
 */
static int get_ifaddr(unsigned char* output)
{
    struct ifreq ifr;
    struct ifconf ifc;
    char buf[1024];
    struct ifreq* it;
    struct ifreq* end;
    int success = 0;

    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sock == -1)
    {
        return 0;
    }

    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = buf;
    if (ioctl(sock, SIOCGIFCONF, &ifc) == -1)
    {
        close(sock);
        return 0;
    }

    it = ifc.ifc_req;
    end = it + (ifc.ifc_len / sizeof(struct ifreq));

    for (; it != end; ++it)
    {
        strcpy(ifr.ifr_name, it->ifr_name);

        if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0)
        {
            if (!(ifr.ifr_flags & IFF_LOOPBACK))
            {
                /* don't count loopback */
                if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0)
                {
                    success = 1;
                    break;
                }
            }
        }
        else
        {
            close(sock);
            return 0;
        }
    }

    if (success)
    {
        memcpy(output, ifr.ifr_hwaddr.sa_data, 6);
    }
    close(sock);

    return success;
}

/**
 * Create a new router for a service
 * @param obj Service configuration context
 * @return True if configuration was successful, false if an error occurred.
 */
int create_new_service(ConfigSection* obj)
{
    auto router = obj->m_parameters.get_string(CN_ROUTER);
    int error_count = 0;

    if (!Service::create(obj->name(), obj->m_parameters))
    {
        MXB_ERROR("Service '%s' creation failed.", obj->name());
        error_count++;
    }

    return error_count;
}

/**
 * Create a new server
 * @param obj Server configuration context
 * @return Number of errors
 */
int create_new_server(ConfigSection* obj)
{
    bool error = false;

    if (!ServerManager::create_server(obj->name(), obj->m_parameters))
    {
        MXB_ERROR("Failed to create a new server.");
        error = true;
    }

    return error;
}

/**
 * Create a new monitor
 *
 * @param obj Monitor configuration context
 * @return 0 on success
 */
int create_new_monitor(ConfigSection* obj)
{
    auto module = obj->m_parameters.get_string(CN_MODULE);
    mxb_assert(!module.empty());
    int rval = 1;
    Monitor* monitor = MonitorManager::create_monitor(obj->name(), module, &obj->m_parameters);
    if (monitor)
    {
        rval = 0;
    }
    else
    {
        MXB_ERROR("Failed to create monitor '%s'.", obj->name());
    }

    return rval;
}

/**
 * Create a new listener for a service
 *
 * @param obj Listener configuration context
 *
 * @return Number of errors
 */
int create_new_listener(ConfigSection* obj)
{
    return mxs::Listener::create(obj->name(), obj->m_parameters) ? 0 : 1;
}

/**
 * Create a new filter
 * @param obj Filter configuration context
 * @return Number of errors
 */
int create_new_filter(ConfigSection* obj)
{
    int error_count = 0;
    auto module_str = obj->m_parameters.get_string(CN_MODULE);
    mxb_assert(!module_str.empty());
    const char* module = module_str.c_str();

    if (const MXS_MODULE* mod = get_module(module_str, mxs::ModuleType::FILTER))
    {
        if (mod->specification && !mod->specification->validate(obj->m_parameters))
        {
            return 1;
        }

        if (!filter_alloc(obj->name(), obj->m_parameters))
        {
            MXB_ERROR("Failed to create filter '%s'.", obj->name());
            error_count++;
        }
    }
    else
    {
        MXB_ERROR("Failed to load filter module '%s'", module);
        error_count++;
    }

    return error_count;
}

std::vector<string> config_break_list_string(const string& list_string)
{
    string copy = list_string;
    /* Parse the elements from the list. They are separated by ',' and are trimmed of whitespace. */
    std::vector<string> tokenized = mxs::strtok(copy, ",");
    for (auto& elem : tokenized)
    {
        fix_object_name(elem);
    }
    return tokenized;
}

json_t* mxs::Config::params_to_json() const
{
    json_t* param = to_json();

    json_object_set_new(param, CN_CACHEDIR, json_string(mxs::cachedir()));
    json_object_set_new(param, CN_CONNECTOR_PLUGINDIR, json_string(mxs::connector_plugindir()));
    json_object_set_new(param, CN_DATADIR, json_string(mxs::datadir()));
    json_object_set_new(param, CN_EXECDIR, json_string(mxs::execdir()));
    json_object_set_new(param, CN_LANGUAGE, json_string(mxs::langdir()));
    json_object_set_new(param, CN_LIBDIR, json_string(mxs::libdir()));
    json_object_set_new(param, CN_LOGDIR, json_string(mxs::logdir()));
    json_object_set_new(param, CN_MODULE_CONFIGDIR, json_string(mxs::module_configdir()));
    json_object_set_new(param, CN_PERSISTDIR, json_string(mxs::config_persistdir()));
    json_object_set_new(param, CN_PIDDIR, json_string(mxs::piddir()));

    if (key_manager != mxs::KeyManager::Type::NONE)
    {
        auto prefix = s_key_manager.to_string(key_manager);
        json_t* opts = json_object();

        for (const auto& [k, v] : key_manager_options)
        {
            json_object_set_new(opts, k.c_str(), json_string(v.c_str()));
        }

        json_object_set_new(param, prefix.c_str(), opts);
    }

    return param;
}

json_t* mxs::Config::maxscale_to_json(const char* host) const
{
    json_t* param = params_to_json();

    json_t* attr = json_object();
    time_t started = maxscale_started();
    time_t activated = started + MXS_CLOCK_TO_SEC(promoted_at);
    json_object_set_new(attr, CN_PARAMETERS, param);
    json_object_set_new(attr, "version", json_string(MAXSCALE_VERSION));
    json_object_set_new(attr, "commit", json_string(maxscale_commit()));
    json_object_set_new(attr, "started_at", json_string(http_to_date(started).c_str()));
    json_object_set_new(attr, "activated_at", json_string(http_to_date(activated).c_str()));
    json_object_set_new(attr, "uptime", json_integer(maxscale_uptime()));
    json_object_set_new(attr, "process_datadir", json_string(mxs::process_datadir()));

    auto manager = mxs::ConfigManager::get()->to_json();
    json_object_set_new(attr, "config_sync", json_incref(manager.get_json()));

    json_object_set_new(attr, "system", system_to_json());

    json_t* obj = json_object();
    json_object_set_new(obj, CN_ATTRIBUTES, attr);
    json_object_set_new(obj, CN_ID, json_string(CN_MAXSCALE));
    json_object_set_new(obj, CN_TYPE, json_string(CN_MAXSCALE));

    return mxs_json_resource(host, MXS_JSON_API_MAXSCALE, obj);
}

json_t* mxs::Config::system_to_json() const
{
    // system.machine
    json_t* machine = json_object();

    json_object_set_new(machine, "cores_physical", json_integer(get_processor_count()));
    json_object_set_new(machine, "cores_available", json_integer(get_cpu_count()));
    json_object_set_new(machine, "cores_virtual", json_real(get_vcpu_count()));
    json_object_set_new(machine, "memory_physical", json_integer(get_total_memory()));
    json_object_set_new(machine, "memory_available", json_integer(get_available_memory()));

    // system.os
    json_t* os = json_object();

    const mxs::Config& c = mxs::Config::get();
    json_object_set_new(os, "sysname", json_string(c.sysname.c_str()));
    json_object_set_new(os, "nodename", json_string(c.nodename.c_str()));
    json_object_set_new(os, "release", json_string(c.release.c_str()));
    json_object_set_new(os, "version", json_string(c.version.c_str()));
    json_object_set_new(os, "machine", json_string(c.machine.c_str()));

    // system.maxscale
    json_t* maxscale = json_object();

    json_object_set_new(maxscale, "threads", json_integer(config_threadcount()));
    json_object_set_new(maxscale, "query_classifier_cache_size",
                        json_integer(this->qc_cache_properties.max_size));

    // system
    json_t* system = json_object();
    json_object_set_new(system, "machine", machine);
    json_object_set_new(system, "os", os);
    json_object_set_new(system, "maxscale", maxscale);

    return system;
}

/**
 * Test if first and last char in the string are as expected.
 *
 * @param string Input string
 * @param expected Required character
 * @return True, if string has at least two chars and both first and last char
 * equal @c expected
 */
static bool check_first_last_char(const char* string, char expected)
{
    bool valid = false;
    {
        size_t len = strlen(string);
        if ((len >= 2) && (string[0] == expected) && (string[len - 1] == expected))
        {
            valid = true;
        }
    }
    return valid;
}

/**
 * Chop a char off from both ends of the string.
 *
 * @param value Input string
 */
static void remove_first_last_char(char* value)
{
    size_t len = strlen(value);
    value[len - 1] = '\0';
    memmove(value, value + 1, len - 1);
}

pcre2_code* compile_regex_string(const char* regex_string,
                                 bool jit_enabled,
                                 uint32_t options,
                                 uint32_t* output_ovector_size)
{
    bool success = true;
    int errorcode = -1;
    PCRE2_SIZE error_offset = -1;
    uint32_t capcount = 0;
    pcre2_code* machine =
        pcre2_compile((PCRE2_SPTR) regex_string,
                      PCRE2_ZERO_TERMINATED,
                      options,
                      &errorcode,
                      &error_offset,
                      NULL);
    if (machine)
    {
        if (jit_enabled)
        {
            // Try to compile even further for faster matching
            if (pcre2_jit_compile(machine, PCRE2_JIT_COMPLETE) < 0)
            {
                MXB_WARNING("PCRE2 JIT compilation of pattern '%s' failed, "
                            "falling back to normal compilation.",
                            regex_string);
            }
        }
        /* Check what is the required match_data size for this pattern. */
        int ret_info = pcre2_pattern_info(machine, PCRE2_INFO_CAPTURECOUNT, &capcount);
        if (ret_info != 0)
        {
            MXS_PCRE2_PRINT_ERROR(ret_info);
            success = false;
        }
    }
    else
    {
        MXB_ERROR("Invalid PCRE2 regular expression '%s' (position '%zu').",
                  regex_string,
                  error_offset);
        MXS_PCRE2_PRINT_ERROR(errorcode);
        success = false;
    }

    if (!success)
    {
        pcre2_code_free(machine);
        machine = NULL;
    }
    else if (output_ovector_size)
    {
        *output_ovector_size = capcount + 1;
    }
    return machine;
}

/**
 * Test if the given string is a valid MaxScale regular expression and can be
 * compiled to a regex machine using PCRE2.
 *
 * @param regex_string The input string
 * @return True if compilation succeeded, false if string is invalid or cannot
 * be compiled.
 */
static bool test_regex_string_validity(const char* regex_string, const char* key)
{
    if (*regex_string == '\0')
    {
        return false;
    }
    char regex_copy[strlen(regex_string) + 1];
    strcpy(regex_copy, regex_string);
    if (!check_first_last_char(regex_string, '/'))
    {
        // return false; // Uncomment this line once '/ .. /' is no longer optional
        MXB_WARNING("Missing slashes (/) around a regular expression is deprecated: '%s=%s'.",
                    key,
                    regex_string);
    }
    else
    {
        remove_first_last_char(regex_copy);
    }

    pcre2_code* code = compile_regex_string(regex_copy, false, 0, NULL);
    bool rval = (code != NULL);
    pcre2_code_free(code);
    return rval;
}

bool get_suffixed_size(const char* value, uint64_t* dest)
{
    if (!isdigit(*value))
    {
        // This will also catch negative values
        return false;
    }

    bool rval = false;
    char* end;
    uint64_t size = strtoll(value, &end, 10);

    switch (*end)
    {
    case 'T':
    case 't':
        if ((*(end + 1) == 'i') || (*(end + 1) == 'I'))
        {
            size *= 1024ULL * 1024ULL * 1024ULL * 1024ULL;
        }
        else
        {
            size *= 1000ULL * 1000ULL * 1000ULL * 1000ULL;
        }
        break;

    case 'G':
    case 'g':
        if ((*(end + 1) == 'i') || (*(end + 1) == 'I'))
        {
            size *= 1024ULL * 1024ULL * 1024ULL;
        }
        else
        {
            size *= 1000ULL * 1000ULL * 1000ULL;
        }
        break;

    case 'M':
    case 'm':
        if ((*(end + 1) == 'i') || (*(end + 1) == 'I'))
        {
            size *= 1024ULL * 1024ULL;
        }
        else
        {
            size *= 1000ULL * 1000ULL;
        }
        break;

    case 'K':
    case 'k':
        if ((*(end + 1) == 'i') || (*(end + 1) == 'I'))
        {
            size *= 1024ULL;
        }
        else
        {
            size *= 1000ULL;
        }
        break;

    default:
        break;
    }

    const std::set<char> first {'T', 't', 'G', 'g', 'M', 'm', 'K', 'k'};
    const std::set<char> second {'I', 'i'};

    if (end[0] == '\0')
    {
        rval = true;
    }
    else if (end[1] == '\0')
    {
        // First character must be valid
        rval = first.count(end[0]);
    }
    else if (end[2] == '\0')
    {
        // Both characters have to be valid
        rval = first.count(end[0]) && second.count(end[1]);
    }

    if (dest)
    {
        *dest = size;
    }

    return rval;
}

bool get_suffixed_duration(const char* zValue,
                           milliseconds* pDuration,
                           mxs::config::DurationUnit* pUnit)
{
    if (!isdigit(*zValue))
    {
        // This will also catch negative values
        return false;
    }

    bool rval = true;
    char* zEnd;
    uint64_t value = strtoll(zValue, &zEnd, 10);

    milliseconds duration;
    mxs::config::DurationUnit unit;

    switch (*zEnd)
    {
    case 'H':
    case 'h':
        unit = mxs::config::DURATION_IN_HOURS;
        duration = std::chrono::duration_cast<milliseconds>(std::chrono::hours(value));
        ++zEnd;
        break;

    case 'M':
    case 'm':
        if (*(zEnd + 1) == 's' || *(zEnd + 1) == 'S')
        {
            unit = mxs::config::DURATION_IN_MILLISECONDS;
            duration = milliseconds(value);
            ++zEnd;
        }
        else
        {
            unit = mxs::config::DURATION_IN_MINUTES;
            duration = std::chrono::duration_cast<milliseconds>(std::chrono::minutes(value));

            if ((*(zEnd + 1) == 'i' || *(zEnd + 1) == 'I') && (*(zEnd + 2) == 'n' || *(zEnd + 2) == 'N'))
            {
                zEnd += 2;
            }
        }
        ++zEnd;
        break;

    case 'S':
    case 's':
        unit = mxs::config::DURATION_IN_SECONDS;
        duration = std::chrono::duration_cast<milliseconds>(seconds(value));
        ++zEnd;
        break;

    default:
        rval = false;
    }

    if (rval)
    {
        if (*zEnd == 0)
        {
            if (pDuration)
            {
                *pDuration = duration;
            }

            if (pUnit)
            {
                *pUnit = unit;
            }
        }
        else
        {
            rval = false;
        }
    }

    return rval;
}

static bool get_milliseconds(const char* zName,
                             const char* zValue,
                             const char* zDisplay_value,
                             milliseconds* pMilliseconds)
{
    bool valid = false;

    if (!zDisplay_value)
    {
        zDisplay_value = zValue;
    }

    mxs::config::DurationUnit unit;
    milliseconds milliseconds;
    if (get_suffixed_duration(zValue, &milliseconds, &unit))
    {
        *pMilliseconds = milliseconds;
        valid = true;
    }
    else
    {
        MXB_ERROR("Invalid duration %s: %s=%s.", zName, zValue, zDisplay_value);
    }

    return valid;
}

static bool get_milliseconds(const char* zName,
                             const char* zValue,
                             const char* zDisplay_value,
                             time_t* pMilliseconds)
{
    milliseconds milliseconds;

    bool valid = get_milliseconds(zName, zValue, zDisplay_value, &milliseconds);

    if (valid)
    {
        *pMilliseconds = milliseconds.count();
    }

    return valid;
}

bool config_parse_disk_space_threshold(DiskSpaceLimits* pDisk_space_threshold,
                                       const char* zDisk_space_threshold)
{
    mxb_assert(pDisk_space_threshold);
    mxb_assert(zDisk_space_threshold);

    bool success = true;

    using namespace std;

    DiskSpaceLimits disk_space_threshold;
    string s(zDisk_space_threshold);

    // Somewhat simplified, this is what we expect: [^:]+:[:digit:]+(,[^:]+:[:digit:]+)*
    // So, e.g. the following are fine "/data:20", "/data1:50,/data2:60", "*:80".

    while (success && !s.empty())
    {
        size_t i = s.find_first_of(',');
        string entry = s.substr(0, i);

        s.erase(0, i != string::npos ? i + 1 : i);

        size_t j = entry.find_first_of(':');

        if (j != string::npos)
        {
            string path = entry.substr(0, j);
            string tail = entry.substr(j + 1);

            mxb::trim(path);
            mxb::trim(tail);

            if (!path.empty() && !tail.empty())
            {
                char* end;
                int32_t percentage = strtol(tail.c_str(), &end, 0);

                if ((*end == 0) && (percentage >= 0) && (percentage <= 100))
                {
                    disk_space_threshold[path] = percentage;
                }
                else
                {
                    MXB_ERROR("The value following the ':' must be a percentage: %s",
                              entry.c_str());
                    success = false;
                }
            }
            else
            {
                MXB_ERROR("The %s parameter '%s' contains an invalid entry: '%s'",
                          CN_DISK_SPACE_THRESHOLD,
                          zDisk_space_threshold,
                          entry.c_str());
                success = false;
            }
        }
        else
        {
            MXB_ERROR("The %s parameter '%s' contains an invalid entry: '%s'",
                      CN_DISK_SPACE_THRESHOLD,
                      zDisk_space_threshold,
                      entry.c_str());
            success = false;
        }
    }

    if (success)
    {
        pDisk_space_threshold->swap(disk_space_threshold);
    }

    return success;
}

bool config_is_valid_name(const char* zName, std::string* pReason)
{
    bool is_valid = true;

    for (const char* z = zName; is_valid && *z; z++)
    {
        if (isspace(*z))
        {
            is_valid = false;

            if (pReason)
            {
                *pReason = "The name '";
                *pReason += zName;
                *pReason += "' contains whitespace.";
            }
        }
    }

    if (is_valid)
    {
        if (strncmp(zName, "@@", 2) == 0)
        {
            is_valid = false;

            if (pReason)
            {
                *pReason = "The name '";
                *pReason += zName;
                *pReason += "' starts with '@@', which is a prefix reserved for MaxScale.";
            }
        }
    }

    return is_valid;
}

bool config_set_rebalance_threshold(const char* value)
{
    bool rv = false;

    char* endptr;
    int intval = strtol(value, &endptr, 0);
    if (*endptr == '\0' && intval >= 0 && intval <= 100)
    {
        mxs::Config::get().rebalance_threshold.set(intval);
        rv = true;
    }
    else
    {
        MXB_ERROR("Invalid value (percentage expected) for '%s': %s", CN_REBALANCE_THRESHOLD, value);
    }

    return rv;
}

//static
std::recursive_mutex UnmaskPasswords::s_guard;

UnmaskPasswords::UnmaskPasswords()
{
    s_guard.lock();

    m_old_val = std::exchange(this_unit.mask_passwords, false);
}

UnmaskPasswords::~UnmaskPasswords()
{
    this_unit.mask_passwords = m_old_val;

    s_guard.unlock();
}

bool config_mask_passwords()
{
    return this_unit.mask_passwords;
}

namespace
{

std::tuple<mxb::ini::map_result::ParseResult, std::string>
post_process_config(mxb::ini::map_result::ParseResult&& res)
{
    string warning;
    if (res.errors.empty())
    {
        int first_mxs_lineno = -1;
        bool conflict_found = false;
        auto case_fix_iter = res.config.end();

        // Check that the config has only one section name that case-insensitively matches "maxscale".
        for (auto it = res.config.begin(); it != res.config.end(); ++it)
        {
            const auto& section = *it;
            const string& header = section.first;
            if (strcasecmp(header.c_str(), CN_MAXSCALE) == 0)
            {
                // Equivalent to "maxscale".
                if (first_mxs_lineno < 0)
                {
                    first_mxs_lineno = section.second.lineno;
                    if (header != CN_MAXSCALE)
                    {
                        case_fix_iter = it;
                    }
                }
                else
                {
                    string errmsg = mxb::string_printf(
                        "Section name '%s' at line %i is a duplicate as it compares case-insensitively to a "
                        "previous definition at line %i.", header.c_str(), section.second.lineno,
                        first_mxs_lineno);

                    res.errors.push_back(std::move(errmsg));
                    conflict_found = true;
                }
            }
        }

        if (first_mxs_lineno >= 0 && !conflict_found && case_fix_iter != res.config.end())
        {
            // Replace the section name so that later checks don't need to worry about case-insensitivity.
            warning = mxb::string_printf("Section header '%s' at line %i is interpreted as "
                                         "'maxscale'.", case_fix_iter->first.c_str(), first_mxs_lineno);
            auto section_data_temp = std::move(case_fix_iter->second);
            res.config.erase(case_fix_iter);
            res.config.emplace(CN_MAXSCALE, std::move(section_data_temp));
        }
    }
    return {std::move(res), std::move(warning)};
}
}

std::tuple<mxb::ini::map_result::ParseResult, std::string>
parse_mxs_config_file_to_map(const string& config_file)
{
    auto res = mxb::ini::parse_config_file_to_map(config_file);

    return post_process_config(std::move(res));
}

std::tuple<mxb::ini::map_result::ParseResult, std::string>
parse_mxs_config_text_to_map(const string& config_text)
{
    auto res = mxb::ini::parse_config_text_to_map(config_text);

    return post_process_config(std::move(res));
}

bool handle_path_arg(std::string* dest, const char* path, const char* arg, const char* arg2)
{
    mxb_assert(path);
    dest->clear();

    if (*path != '/')
    {
        char pwd[PATH_MAX + 1] = "";

        if (!getcwd(pwd, sizeof(pwd)))
        {
            MXB_ALERT("Call to getcwd() failed: %d, %s", errno, mxb_strerror(errno));
            return false;
        }

        dest->append(pwd);

        if (dest->back() != '/')
        {
            dest->append("/");
        }
    }

    for (const char* p : {path, arg, arg2})
    {
        if (p)
        {
            dest->append(p);

            if (dest->back() != '/')
            {
                dest->append("/");
            }
        }
    }

    return true;
}

namespace
{

/**
 * Read various directory paths and log settings from configuration. Variable substitution is
 * assumed to be already performed.
 *
 * @param main_config Parsed [maxscale]-section from the main configuration file.
 */
void apply_dir_log_config(const mxb::ini::map_result::ConfigSection& main_config)
{
    const string* value = nullptr;
    std::string tmp;

    auto find_helper = [&main_config, &value](const string& key) {
            bool rval = false;
            const auto& kvs = main_config.key_values;
            auto it = kvs.find(key);
            if (it != kvs.end())
            {
                value = &it->second.value;
                rval = true;
            }
            return rval;
        };

    // These will not override command line parameters but will override default values. */
    if (find_helper(CN_LOGDIR))
    {
        if (strcmp(mxs::logdir(), cmake_defaults::DEFAULT_LOGDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_logdir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_LIBDIR))
    {
        if (strcmp(mxs::libdir(), cmake_defaults::DEFAULT_LIBDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_libdir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_SHAREDIR))
    {
        if (strcmp(mxs::sharedir(), cmake_defaults::DEFAULT_SHAREDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_sharedir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_PIDDIR))
    {
        if (strcmp(mxs::piddir(), cmake_defaults::DEFAULT_PIDDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_piddir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_DATADIR))
    {
        if (strcmp(mxs::datadir(), cmake_defaults::DEFAULT_DATADIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_datadir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_CACHEDIR))
    {
        if (strcmp(mxs::cachedir(), cmake_defaults::DEFAULT_CACHEDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_cachedir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_LANGUAGE))
    {
        if (strcmp(mxs::langdir(), cmake_defaults::DEFAULT_LANGDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_langdir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_EXECDIR))
    {
        if (strcmp(mxs::execdir(), cmake_defaults::DEFAULT_EXECDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_execdir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_CONNECTOR_PLUGINDIR))
    {
        if (strcmp(mxs::connector_plugindir(), cmake_defaults::DEFAULT_CONNECTOR_PLUGINDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_connector_plugindir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_PERSISTDIR))
    {
        if (strcmp(mxs::config_persistdir(), cmake_defaults::DEFAULT_CONFIG_PERSISTDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_config_persistdir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    if (find_helper(CN_MODULE_CONFIGDIR))
    {
        if (strcmp(mxs::module_configdir(), cmake_defaults::DEFAULT_MODULE_CONFIGDIR) == 0
            && handle_path_arg(&tmp, value->c_str()))
        {
            mxs::set_module_configdir(tmp, mxs::config::Origin::CONFIG);
        }
    }

    mxs::Config& cnf = mxs::Config::get();
    if (find_helper(CN_SYSLOG))
    {
        mxs::set_syslog(config_truth_value(*value), mxs::config::Origin::CONFIG);
    }

    if (find_helper(CN_MAXLOG))
    {
        mxs::set_maxlog(config_truth_value(*value), mxs::config::Origin::CONFIG);
    }

    if (find_helper(CN_LOAD_PERSISTED_CONFIGS))
    {
        cnf.load_persisted_configs = config_truth_value(*value);
    }

    if (find_helper(CN_LOG_AUGMENTATION))
    {
        mxs::set_log_augmentation(atoi(value->c_str()), mxs::config::Origin::CONFIG);
    }
}

}

namespace
{
SniffResult sniff_configuration(std::tuple<mxb::ini::map_result::ParseResult, std::string>&& result,
                                std::string_view filepath = std::string_view {})
{
    SniffResult rval;

    mxb::ini::map_result::ParseResult& load_res = std::get<0>(result);
    std::string& warning = std::get<1>(result);

    if (load_res.errors.empty())
    {
        rval.success = true;
        // At this point, we are only interested in the "maxscale"-section.
        auto& config = load_res.config;
        auto it = config.find(CN_MAXSCALE);
        if (it != config.end())
        {
            bool substitution_ok = true;

            auto& mxs_section = it->second.key_values;
            auto it2 = mxs_section.find(CN_SUBSTITUTE_VARIABLES);
            if (it2 != mxs_section.end())
            {
                bool subst_on = config_truth_value(it2->second.value);
                // Substitution affects other config files as well so save the setting.
                if (subst_on)
                {
                    mxs::Config& cnf = mxs::Config::get();
                    cnf.substitute_variables = true;
                    auto subst_errors = mxb::ini::substitute_env_vars(config);
                    if (!subst_errors.empty())
                    {
                        string errmsg;

                        if (!filepath.empty())
                        {
                            errmsg = mxb::string_printf("Variable substitution to file '%.*s' failed. ",
                                                        (int)filepath.length(), filepath.data());
                        }
                        else
                        {
                            errmsg = mxb::string_printf("Variable substitution failed.");
                        }

                        errmsg += mxb::create_list_string(subst_errors, " ");
                        MXB_ALERT("%s", errmsg.c_str());
                        substitution_ok = false;
                    }
                }
            }

            if (substitution_ok)
            {
                apply_dir_log_config(it->second);
            }
            rval.success = substitution_ok;
        }

        if (rval.success)
        {
            rval.config = move(load_res.config);
            rval.warning = std::move(warning);
        }
    }
    else
    {
        string all_errors = mxb::create_list_string(load_res.errors, " ");
        if (!filepath.empty())
        {
            MXB_ALERT("Failed to read configuration file '%.*s': %s",
                      (int)filepath.length(), filepath.data(), all_errors.c_str());
        }
        else
        {
            MXB_ALERT("Failed to parse configuration: %s", all_errors.c_str());
        }

        rval.errors = std::move(load_res.errors);
    }
    return rval;
}

}

SniffResult sniff_configuration(const string& filepath)
{
    return sniff_configuration(parse_mxs_config_file_to_map(filepath), filepath);
}

SniffResult sniff_configuration_text(const string& config)
{
    return sniff_configuration(parse_mxs_config_text_to_map(config));
}
