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
#pragma once

#define MXB_MODULE_NAME "maxrows"

#include <maxscale/ccdefs.hh>
#include <maxscale/config2.hh>
#include <maxscale/filter.hh>
#include <maxscale/protocol/mariadb/module_names.hh>


class MaxRowsConfig : public mxs::config::Configuration
{
public:
    enum Mode
    {
        EMPTY, ERR, OK
    };

    MaxRowsConfig(const char* zName);

    MaxRowsConfig(MaxRowsConfig&& rhs) = default;

    mxs::config::Count      max_rows;
    mxs::config::Size       max_size;
    mxs::config::Integer    debug;
    mxs::config::Enum<Mode> mode;
};


class MaxRows;

class MaxRowsSession : public maxscale::FilterSession
{
public:
    MaxRowsSession(const MaxRowsSession&) = delete;
    MaxRowsSession& operator=(const MaxRowsSession&) = delete;

    // Create a new filter session
    static MaxRowsSession* create(MXS_SESSION* pSession, SERVICE* pService, MaxRows* pFilter)
    {
        return new(std::nothrow) MaxRowsSession(pSession, pService, pFilter);
    }

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

private:
    MaxRowsSession(MXS_SESSION* pSession, SERVICE* pService, MaxRows* pFilter);

    uint64_t            m_max_rows;
    uint64_t            m_max_size;
    int64_t             m_debug;
    MaxRowsConfig::Mode m_mode;

    GWBUF m_buffer;     // Contains the partial resultset
    bool  m_collect {true};
};

class MaxRows : public mxs::Filter
{
public:
    MaxRows(const MaxRows&) = delete;
    MaxRows& operator=(const MaxRows&) = delete;

    using Config = MaxRowsConfig;

    static constexpr uint64_t CAPABILITIES = RCAP_TYPE_REQUEST_TRACKING;

    // Creates a new filter instance
    static MaxRows* create(const char* name);

    // Creates a new session for this filter
    std::shared_ptr<mxs::FilterSession> newSession(MXS_SESSION* session, SERVICE* service) override
    {
        return std::shared_ptr<mxs::FilterSession>(MaxRowsSession::create(session, service, this));
    }

    // Returns JSON form diagnostic data
    json_t* diagnostics() const override
    {
        return nullptr;
    }

    // Get filter capabilities
    uint64_t getCapabilities() const override
    {
        return CAPABILITIES;
    }

    mxs::config::Configuration& getConfiguration() override
    {
        return m_config;
    }

    std::set<std::string> protocols() const override
    {
        return {MXS_MARIADB_PROTOCOL_NAME};
    }

    // Return reference to filter config
    const Config& config() const
    {
        return m_config;
    }

private:
    MaxRows(const char* name)
        : m_name(name)
        , m_config(name)
    {
    }

private:
    std::string m_name;
    Config      m_config;
};
