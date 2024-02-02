/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <deque>
#include <maxscale/backend.hh>
#include <maxscale/buffer.hh>
#include <maxscale/queryclassifier.hh>
#include "cbackend.hh"
#include "cregistry.hh"
#include "cresult.hh"
#include "cstats.hh"

class CRouter;

class CSession final : public mxs::RouterSession
                     , private COtherBackend::Handler
{
public:
    using Stats = CSessionStats;

    CSession(const CSession&) = delete;
    CSession& operator=(const CSession&) = delete;

    CSession(MXS_SESSION* pSession,
             CRouter* pRouter,
             SCMainBackend sMain,
             SCOtherBackends backends);
    ~CSession();

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    bool handleError(mxs::ErrorType type, const std::string& message,
                     mxs::Endpoint* pProblem, const mxs::Reply& reply) override;

private:
    // COtherBackend::Handler
    Explain ready(COtherResult& other_result) override;
    void ready(const CExplainOtherResult& explain_other_result) override;

private:
    bool should_report(const COtherResult& result) const;

    void generate_report(const COtherResult& result);
    void generate_report(const CExplainOtherResult& result);

    void generate_report(const COtherResult& other_result,
                         json_t* pExplain_other,
                         json_t* pExplain_main);
    json_t* generate_json(const CResult& result, json_t* pExplain);

    SCMainBackend   m_sMain;
    SCOtherBackends m_others;
    int             m_responses = 0;
    CRouter&        m_router;
};
