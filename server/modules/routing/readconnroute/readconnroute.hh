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

/**
 * @file readconnection.hh - The read connection balancing query module header file
 */

#define MXB_MODULE_NAME "readconnroute"

#include <maxscale/ccdefs.hh>
#include <maxscale/router.hh>
#include <maxscale/session_stats.hh>
#include <maxscale/workerlocal.hh>
#include <maxscale/config2.hh>

class RCR;
namespace config = maxscale::config;

/**
 * The client session structure used within this router.
 */
class RCRSession : public mxs::RouterSession
{
public:
    RCRSession(RCR* inst, MXS_SESSION* session, mxs::Endpoint* backend,
               const mxs::Endpoints& endpoints, uint32_t bitvalue);
    ~RCRSession();

    bool routeQuery(GWBUF&& queue) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

private:
    uint32_t       m_bitvalue;  /**< Session specific required value of server->status */
    mxs::Endpoint* m_backend;
    mxs::Endpoints m_endpoints;

    RCR*                        m_router;
    maxbase::StopWatch          m_session_timer;
    maxbase::EpollIntervalTimer m_query_timer;
    int64_t                     m_session_queries = 0;

    bool connection_is_valid() const;
    void log_closed_session(const GWBUF& buffer, mxs::Target* t);
};

/**
 * The per instance data for the router.
 */
class RCR : public mxs::Router
{
public:
    class Config : public config::Configuration
    {
    public:
        Config(const std::string& name);
        Config(const Config&&) = delete;

        static void populate(MXS_MODULE& module);

        config::EnumMask<uint32_t> router_options;
        config::Bool               master_accept_reads;
        config::Seconds            max_replication_lag;

    private:
        static config::Specification           s_specification;
        static config::ParamEnumMask<uint32_t> s_router_options;
        static config::ParamBool               s_master_accept_reads;
        static config::ParamSeconds            s_max_replication_lag;
    };

    /**
     * Create a new RadConn instance
     *
     * @param service The service this router is being create for
     * @param params  List of parameters for this service
     *
     * @return The new instance or nullptr on error
     */
    static RCR* create(SERVICE* service);

    /**
     * Create a new session for this router instance
     *
     * @param session The session object
     *
     * @return Router session or nullptr on error
     */
    mxs::RouterSession* newSession(MXS_SESSION* pSession, const mxs::Endpoints& endpoints) override;

    /**
     * Get router diagnostics in JSON
     *
     * @return JSON data representing the router instance
     */
    json_t* diagnostics() const override;

    /**
     * Get router capability bits
     *
     * @return The router capability bits
     */
    uint64_t getCapabilities() const override;

    mxs::config::Configuration& getConfiguration() override
    {
        return m_config;
    }

    std::set<std::string> protocols() const override
    {
        return {MXS_ANY_PROTOCOL};
    }

    /**
     * @brief session_stats
     *
     * @return a reference to the SessionStats of the Target (of the calling thread).
     */
    maxscale::SessionStats& session_stats(maxscale::Target* pTarget);

    /**
     * @brief Combine stats for all servers across all threads
     *
     * @return reference to the TargetSessionStats of this thread.
     */
    maxscale::TargetSessionStats combined_target_stats() const;

private:
    RCR(SERVICE* service);

    mxs::Endpoint* get_connection(const mxs::Endpoints& endpoints);

    mxs::WorkerLocal<maxscale::TargetSessionStats> m_target_stats;

    Config   m_config;
    SERVICE& m_service;
};
