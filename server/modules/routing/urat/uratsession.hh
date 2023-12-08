/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"
#include <deque>
#include <maxscale/backend.hh>
#include <maxscale/buffer.hh>
#include "uratbackend.hh"
#include "uratround.hh"
#include "uratrouter.hh"
#include "uratresult.hh"

class UratRouter;

class UratSession : public mxs::RouterSession
{
public:
    UratSession(const UratSession&) = delete;
    UratSession& operator=(const UratSession&) = delete;

    UratSession(MXS_SESSION* pSession,
                UratRouter* pRouter,
                SUratMainBackend sMain,
                SUratOtherBackends backends);

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    bool handleError(mxs::ErrorType type, const std::string& message,
                     mxs::Endpoint* pProblem, const mxs::Reply& reply) override final;

private:
    SUratMainBackend        m_sMain;
    SUratOtherBackends      m_others;
    int                     m_responses = 0;
    UratRouter&             m_router;
    std::deque<GWBUF>       m_queue;
    uint64_t                m_num_queries = 0;
    GWBUF                   m_last_chunk;
    mxs::ReplyRoute         m_last_route;
    std::deque<UratRound>   m_rounds;

    void route_queued_queries();
    bool should_report() const;
    void generate_report(const UratRound& round);
    json_t* generate_report(const UratBackend* pBackend, const UratResult& result);
    void finalize_reply();
};
