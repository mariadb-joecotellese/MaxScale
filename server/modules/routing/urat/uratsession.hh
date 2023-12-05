/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"
#include "uratbackend.hh"
#include "uratrouter.hh"

#include <maxscale/backend.hh>
#include <maxscale/buffer.hh>

#include <deque>

class UratRouter;

class UratSession : public mxs::RouterSession
{
public:
    UratSession(const UratSession&) = delete;
    UratSession& operator=(const UratSession&) = delete;

    UratSession(MXS_SESSION* pSession, UratRouter* pRouter, SUratBackends backends);

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    bool handleError(mxs::ErrorType type, const std::string& message,
                     mxs::Endpoint* pProblem, const mxs::Reply& reply) override final;

private:
    SUratBackends           m_backends;
    UratBackend*            m_pMain = nullptr;
    int                     m_responses = 0;
    UratRouter&             m_router;
    std::deque<GWBUF>       m_queue;
    std::string             m_query;
    uint8_t                 m_command = 0;
    uint64_t                m_num_queries = 0;
    GWBUF                   m_last_chunk;
    mxs::ReplyRoute         m_last_route;
    std::vector<UratResult> m_results;

    void route_queued_queries();
    bool should_report() const;
    void generate_report();
    void finalize_reply();
};
