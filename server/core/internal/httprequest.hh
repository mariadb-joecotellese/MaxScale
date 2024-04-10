/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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

#include <maxscale/ccdefs.hh>

#include <algorithm>
#include <deque>
#include <map>
#include <string>
#include <memory>
#include <cstring>
#include <cstdint>
#include <microhttpd.h>

#include <maxbase/alloc.hh>
#include <maxbase/jansson.hh>
#include <maxscale/http.hh>

// The API version part of the URL
#define MXS_REST_API_VERSION "v1"

static MHD_Result value_iterator(void* cls,
                                 enum MHD_ValueKind kind,
                                 const char* key,
                                 const char* value)
{
    std::pair<std::string, std::string>* cmp = (std::pair<std::string, std::string>*)cls;

    if (strcasecmp(cmp->first.c_str(), key) == 0 && value)
    {
        cmp->second = value;
        return MHD_NO;
    }

    return MHD_YES;
}

static MHD_Result value_collector(void* cls,
                                  enum MHD_ValueKind kind,
                                  const char* key,
                                  const char* value)
{
    std::map<std::string, std::string>* cmp = (std::map<std::string, std::string>*)cls;
    std::string k(key);
    std::transform(k.begin(), k.end(), k.begin(), ::tolower);
    cmp->emplace(k, value ? value : "");
    return MHD_YES;
}

static MHD_Result value_sum_iterator(void* cls,
                                     enum MHD_ValueKind kind,
                                     const char* key,
                                     const char* value)
{
    size_t& count = *(size_t*)cls;
    count++;
    return MHD_YES;
}

static MHD_Result value_copy_iterator(void* cls,
                                      enum MHD_ValueKind kind,
                                      const char* key,
                                      const char* value)
{
    std::string k = key;
    if (value)
    {
        k += "=";
        k += value;
    }

    char**& dest = *(char***) cls;
    *dest = MXB_STRDUP_A(k.c_str());
    dest++;

    return MHD_YES;
}

class HttpRequest
{
    HttpRequest(const HttpRequest&);
    HttpRequest& operator=(const HttpRequest);
public:
    /**
     * @brief Parse a request
     *
     * @param request Request to parse
     *
     * @return Parsed statement or NULL if request is not valid
     */
    HttpRequest(struct MHD_Connection* connection, std::string url, std::string method, json_t* data);

    ~HttpRequest();

    /**
     * @brief Return request verb type
     *
     * @return One of the HTTP verb values
     */
    const std::string& get_verb() const
    {
        return m_verb;
    }

    /**
     * @brief Get header value
     *
     * @param header Header to get
     *
     * @return Header value or empty string if the header was not found
     */
    std::string get_header(std::string header) const
    {
        std::transform(header.begin(), header.end(), header.begin(), ::tolower);
        auto it = m_headers.find(header);
        return it != m_headers.end() ? it->second : "";
    }

    /**
     * Get all headers
     *
     * @return All request headers
     */
    const std::map<std::string, std::string>& get_headers() const
    {
        return m_headers;
    }

    /**
     * @brief Get cookie value
     *
     * @param cookie Cookie to get
     *
     * @return Cookie value or empty string if the cookie was not found
     */
    std::string get_cookie(std::string cookie) const
    {
        std::transform(cookie.begin(), cookie.end(), cookie.begin(), ::tolower);
        auto it = m_cookies.find(cookie);
        return it != m_cookies.end() ? it->second : "";
    }

    /**
     * Get all cookies
     *
     * @return All request cookies
     */
    const std::map<std::string, std::string>& get_cookies() const
    {
        return m_cookies;
    }

    /**
     * @brief Get option value
     *
     * @param header Option to get
     *
     * @return Option value or empty string if the option was not found
     */
    std::string get_option(std::string option) const
    {
        std::transform(option.begin(), option.end(), option.begin(), ::tolower);
        auto it = m_options.find(option);
        return it != m_options.end() ? it->second : "";
    }

    /**
     * @brief Returns true if the option is set to `no`, `false`, `0` or `off`
     *
     * @param option The option to get
     *
     * @return True if the option is set and it contains a false value
     */
    bool is_falsy_option(std::string option) const;


    /**
     * @brief Returns true if the option is set to `yes`, `true`, `1` or `on`
     *
     * @param option The option to get
     *
     * @return True if the option is set and it contains a true value
     */
    bool is_truthy_option(std::string option) const;

    /**
     * Get all options
     *
     * @return All request options
     */
    const std::map<std::string, std::string>& get_options() const
    {
        return m_options;
    }

    /**
     * @brief Get request option count
     *
     * @return Number of options in the request
     */
    size_t get_option_count() const
    {
        size_t rval = 0;
        MHD_get_connection_values(m_connection,
                                  MHD_GET_ARGUMENT_KIND,
                                  value_sum_iterator,
                                  &rval);

        return rval;
    }

    /**
     * @brief Copy options to an array
     *
     * The @c dest parameter must be able to hold at least get_option_count()
     * pointers. The values stored need to be freed by the caller.
     *
     * @param dest Destination where options are copied
     */
    void copy_options(char** dest) const
    {
        MHD_get_connection_values(m_connection,
                                  MHD_GET_ARGUMENT_KIND,
                                  value_copy_iterator,
                                  &dest);
    }

    /**
     * @brief Return request body
     *
     * @return Request body or empty string if no body is defined
     */
    std::string get_json_str() const
    {
        return m_json ? mxb::json_dump(m_json.get()) : "";
    }

    /**
     * @brief Return raw JSON body
     *
     * @return Raw JSON body or NULL if no body is defined
     */
    json_t* get_json() const
    {
        return m_json.get();
    }

    void set_json(json_t* json)
    {
        m_json.reset(json);
    }

    /**
     * @brief Get complete request URI
     *
     * Note that the returned URI does not include a leading or a trailing slash.
     *
     * @return The complete request URI
     */
    std::string get_uri() const
    {
        return m_resource;
    }

    /**
     * Return the individual parts of the request URI
     *
     * @return std::deque<std::string>
     */
    const std::deque<std::string>& uri_parts() const
    {
        return m_resource_parts;
    }

    /**
     * @brief Get URI part
     *
     * @param idx Zero indexed part number in URI
     *
     * @return The request URI part or empty string if no part was found
     */
    std::string uri_part(uint32_t idx) const
    {
        return m_resource_parts.size() > idx ? m_resource_parts[idx] : "";
    }

    /**
     * @brief Return a segment of the URI
     *
     * Combines a range of parts into a segment of the URI. Each part is
     * separated by a forward slash.
     *
     * @param start Start of range
     * @param end   End of range, not inclusive
     *
     * @return The URI segment that matches this range
     */
    std::string uri_segment(uint32_t start, uint32_t end) const
    {
        std::string rval;

        for (uint32_t i = start; i < end && i < m_resource_parts.size(); i++)
        {
            if (i > start)
            {
                rval += "/";
            }

            rval += m_resource_parts[i];
        }

        return rval;
    }

    /**
     * @brief Return how many parts are in the URI
     *
     * @return Number of URI parts
     */
    size_t uri_part_count() const
    {
        return m_resource_parts.size();
    }

    /**
     * @brief Return the last part of the URI
     *
     * @return The last URI part
     */
    std::string last_uri_part() const
    {
        return m_resource_parts.size() > 0 ? m_resource_parts[m_resource_parts.size() - 1] : "";
    }

    /**
     * @brief Return the value of the Host header
     *
     * @return The value of the Host header
     */
    const char* host() const
    {
        return m_hostname.c_str();
    }

    /**
     * @brief Convert request to string format
     *
     * The returned string should be logically equivalent to the original request.
     *
     * @return The request in string format
     */
    std::string to_string() const;

private:
    void fix_api_version();

    /** Constants */
    static const std::string HTTP_PREFIX;
    static const std::string HTTPS_PREFIX;

    std::map<std::string, std::string> m_options;       /**< Request options */
    std::map<std::string, std::string> m_headers;       /**< Request headers */
    std::map<std::string, std::string> m_cookies;       /**< Request cookies */
    std::unique_ptr<json_t>            m_json;          /**< Request body */
    std::string                        m_resource;      /**< Requested resource */
    std::deque<std::string>            m_resource_parts;/**< @c m_resource split into parts */
    std::string                        m_verb;          /**< Request method */
    std::string                        m_hostname;      /**< The value of the Host header */
    struct MHD_Connection*             m_connection;
};
