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

/**
 * @file include/maxscale/config.h The configuration handling elements
 */

#include <maxscale/ccdefs.hh>

#include <unordered_map>
#include <string>
#include <limits.h>
#include <map>
#include <sys/utsname.h>
#include <time.h>
#include <vector>

#include <maxbase/jansson.hh>
#include <maxscale/modinfo.hh>
#include <maxscale/pcre2.hh>

class SERVICE;
class SERVER;

namespace maxscale
{
class Target;
}

// A mapping from a path to a percentage, e.g.: "/disk" -> 80.
using DiskSpaceLimits = std::unordered_map<std::string, int32_t>;

/** Default port where the REST API listens */
#define DEFAULT_ADMIN_HTTP_PORT 8989
#define DEFAULT_ADMIN_HOST      "127.0.0.1"

#define SYSNAME_LEN        256
#define MAX_ADMIN_USER_LEN 1024
#define MAX_ADMIN_PW_LEN   1024
#define MAX_ADMIN_HOST_LEN 1024

/** JSON Pointers to key parts of JSON objects */
#define MXS_JSON_PTR_DATA       "/data"
#define MXS_JSON_PTR_ID         "/data/id"
#define MXS_JSON_PTR_TYPE       "/data/type"
#define MXS_JSON_PTR_PARAMETERS "/data/attributes/parameters"

/** Pointers to relation lists */
#define MXS_JSON_PTR_RELATIONSHIPS          "/data/relationships"
#define MXS_JSON_PTR_RELATIONSHIPS_SERVERS  "/data/relationships/servers/data"
#define MXS_JSON_PTR_RELATIONSHIPS_SERVICES "/data/relationships/services/data"
#define MXS_JSON_PTR_RELATIONSHIPS_MONITORS "/data/relationships/monitors/data"
#define MXS_JSON_PTR_RELATIONSHIPS_FILTERS  "/data/relationships/filters/data"

/** Parameter value JSON Pointers */
#define MXS_JSON_PTR_PARAM_PORT                  MXS_JSON_PTR_PARAMETERS "/port"
#define MXS_JSON_PTR_PARAM_ADDRESS               MXS_JSON_PTR_PARAMETERS "/address"
#define MXS_JSON_PTR_PARAM_SOCKET                MXS_JSON_PTR_PARAMETERS "/socket"
#define MXS_JSON_PTR_PARAM_PROTOCOL              MXS_JSON_PTR_PARAMETERS "/protocol"
#define MXS_JSON_PTR_PARAM_AUTHENTICATOR         MXS_JSON_PTR_PARAMETERS "/authenticator"
#define MXS_JSON_PTR_PARAM_AUTHENTICATOR_OPTIONS MXS_JSON_PTR_PARAMETERS "/authenticator_options"
#define MXS_JSON_PTR_PARAM_SSL_KEY               MXS_JSON_PTR_PARAMETERS "/ssl_key"
#define MXS_JSON_PTR_PARAM_SSL_CERT              MXS_JSON_PTR_PARAMETERS "/ssl_cert"
#define MXS_JSON_PTR_PARAM_SSL_CA_CERT           MXS_JSON_PTR_PARAMETERS "/ssl_ca_cert"
#define MXS_JSON_PTR_PARAM_SSL_VERSION           MXS_JSON_PTR_PARAMETERS "/ssl_version"
#define MXS_JSON_PTR_PARAM_SSL_CERT_VERIFY_DEPTH MXS_JSON_PTR_PARAMETERS "/ssl_cert_verify_depth"
#define MXS_JSON_PTR_PARAM_SSL_VERIFY_PEER_CERT  MXS_JSON_PTR_PARAMETERS "/ssl_verify_peer_certificate"
#define MXS_JSON_PTR_PARAM_SSL_VERIFY_PEER_HOST  MXS_JSON_PTR_PARAMETERS "/ssl_verify_peer_host"

/** Non-parameter JSON pointers */
#define MXS_JSON_PTR_ROUTER   "/data/attributes/router"
#define MXS_JSON_PTR_MODULE   "/data/attributes/module"
#define MXS_JSON_PTR_PASSWORD "/data/attributes/password"
#define MXS_JSON_PTR_ACCOUNT  "/data/attributes/account"

namespace maxscale
{

namespace config
{

enum DurationUnit
{
    DURATION_IN_HOURS,
    DURATION_IN_MINUTES,
    DURATION_IN_SECONDS,
    DURATION_IN_MILLISECONDS,
};
}

namespace cfg = config;

/**
 * Config parameter container. Typically includes all parameters of a single configuration file section
 * such as a server or filter.
 */
class ConfigParameters
{
public:
    using ContainerType = std::map<std::string, std::string>;

    /**
     * Convert JSON object into mxs::ConfigParameters
     *
     * Only scalar values are converted into their string form.
     *
     * @param JSON object to convert
     *
     * @return the ConfigParameters representation of the object
     */
    static ConfigParameters from_json(json_t* json);

    /**
     * Get value of key as string.
     *
     * @param key Parameter name
     * @return Parameter value. Empty string if key not found.
     */
    std::string get_string(const std::string& key) const;

    /**
     * @brief Get a boolean value
     *
     * The existence of the parameter should be checked with config_get_param() before
     * calling this function to determine whether the return value represents an existing
     * value or a missing value.
     *
     * @param key Parameter name
     * @return The value as a boolean or false if none was found
     */
    bool get_bool(const std::string& key) const;

    /**
     * Check if a key exists.
     *
     * @param key Parameter name
     * @return True if key was found
     */
    bool contains(const std::string& key) const;

    /**
     * Set a key-value combination. If the key doesn't exist, it is added. The function is static
     * to handle the special case of params being empty. This is needed until the config management
     * has been properly refactored.
     *
     * @param key Parameter key
     * @param value Value to set
     */
    void set(const std::string& key, const std::string& value);

    /**
     * Remove a key-value pair from the container.
     *
     * @param key Key to remove
     */
    void remove(const std::string& key);

    void clear();
    bool empty() const;

    ContainerType::const_iterator begin() const;
    ContainerType::const_iterator end() const;

    void swap(mxs::ConfigParameters& other)
    {
        m_contents.swap(other.m_contents);
    }

private:
    ContainerType m_contents;
};

/**
 * Parse the authenticator options string to config parameters object.
 *
 * @param opts Options string
 * @return True on success + parsed parameters
 */
std::tuple<bool, mxs::ConfigParameters> parse_auth_options(std::string_view opts);
}

/**
 * Break a comma-separated list into a string array. Removes whitespace from list items.
 *
 * @param list_string A list of items
 * @return The array
 */
std::vector<std::string> config_break_list_string(const std::string& list_string);

/**
 * @brief Convert string truth value
 *
 * Used for truth values with @c 1, @c yes or @c true for a boolean true value and @c 0, @c no
 * or @c false for a boolean false value.
 *
 * @param str String to convert to a truth value
 *
 * @return 1 if @c value is true, 0 if value is false and -1 if the value is not
 * a valid truth value
 */
int config_truth_value(const char* value);
inline int config_truth_value(const std::string& value)
{
    return config_truth_value(value.c_str());
}

/**
 * @brief Get worker thread count
 *
 * @return Number of worker threads
 */
int config_threadcount(void);


/**
 * @brief  Get DCB write queue high water mark
 *
 * @return  Number of high water mark in bytes
 */
uint32_t config_writeq_high_water();

/**
 * @brief  Get DCB write queue low water mark
 *
 * @return @return  Number of low water mark in bytes
 */
uint32_t config_writeq_low_water();

/**
 * @brief Interpret a @disk_space_threshold configuration string.
 *
 * @param disk_space_threshold  Data structure for holding disk space configuration.
 * @param config_value          Configuration value from the configuration file.
 *
 * @return True, if @ config_value was valid, false otherwise.
 *
 */
bool config_parse_disk_space_threshold(DiskSpaceLimits* disk_space_threshold,
                                       const char* config_value);

/**
 * @brief Check whether section/object name is valid.
 *
 * @param name     The name to be checked.
 * @param reason   If non-null, will in case the name is not valid contain
 *                 the reason when the function returns.
 *
 * @return True, if the name is valid, false otherwise.
 */
bool config_is_valid_name(const char* name, std::string* reason = nullptr);

inline bool config_is_valid_name(const std::string& name, std::string* reason)
{
    return config_is_valid_name(name.c_str(), reason);
}

/**
 * Converts a string into milliseconds, intepreting in a case-insensitive manner
 * an 'h'-suffix to indicate hours, an 'm'-suffix to indicate minutes, an
 * 's'-suffix to indicate seconds and an 'ms'-suffix to indicate milliseconds.
 *
 * @param zValue          A numerical string, suffixed by 'h', 'm', 's' or 'ms'.
 * @param pDuration       Pointer, if non-NULL, where the result is stored.
 * @param pUnit           Pointer, if non-NULL, where the detected unit is stored.
 *
 * @return True on success, false on invalid input in which case @c pUnit and
 *         @c pDuration will not be modified.
 */
bool get_suffixed_duration(const char* zValue,
                           std::chrono::milliseconds* pDuration,
                           mxs::config::DurationUnit* pUnit = nullptr);

/**
 * Converts a string into seconds, intepreting in a case-insensitive manner
 * an 'h'-suffix to indicate hours, an 'm'-suffix to indicate minutes, an
 * 's'-suffix to indicate seconds and an 'ms'-suffix to indicate milliseconds.
 *
 * A value lacking a specific suffix will be interpreted as seconds.
 *
 * @param zValue     A numerical string, possibly suffixed by 'h', 'm',
 *                   's' or 'ms'.
 * @param pDuration  Pointer, if non-NULL, where the result is stored.
 * @param pUnit      Pointer, if non-NULL, where the detected unit is stored.
 *
 * @return True on success, false on invalid input in which case @c pUnit and
 *         @c pDuration will not be modified.
 */
inline bool get_suffixed_duration(const char* zValue,
                                  std::chrono::seconds* pDuration,
                                  mxs::config::DurationUnit* pUnit = nullptr)
{
    std::chrono::milliseconds ms;

    bool rv = get_suffixed_duration(zValue, &ms, pUnit);

    if (rv)
    {
        *pDuration = std::chrono::duration_cast<std::chrono::seconds>(ms);
    }

    return rv;
}

/**
 * Converts a string into the corresponding value, interpreting
 * IEC or SI prefixes used as suffixes appropriately.
 *
 * @param value A numerical string, possibly suffixed by a IEC binary prefix or
 *              SI prefix.
 * @param dest  Pointer where the result is stored. If set to NULL, only the
 *              validity of value is checked.
 *
 * @return True on success, false on invalid input in which case contents of
 *         `dest` are left in an undefined state
 */
bool get_suffixed_size(const char* value, uint64_t* dest);

inline bool get_suffixed_size(const std::string& value, uint64_t* dest)
{
    return get_suffixed_size(value.c_str(), dest);
}

/**
 * Compile a regex string using PCRE2 using the settings provided.
 *
 * @param regex_string The string to compile
 * @param jit_enabled Enable JIT compilation. If true but JIT is not available,
 * a warning is printed.
 * @param options PCRE2 compilation options
 * @param output_ovector_size Output for the match data ovector size. On error,
 * nothing is written. If NULL, the parameter is ignored.
 * @return Compiled regex code on success, NULL otherwise
 */
pcre2_code* compile_regex_string(const char* regex_string,
                                 bool jit_enabled,
                                 uint32_t options,
                                 uint32_t* output_ovector_size);
