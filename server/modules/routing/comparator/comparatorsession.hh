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
#include "comparatorbackend.hh"
#include "comparatorregistry.hh"
#include "comparatorresult.hh"
#include "comparatorstats.hh"

class ComparatorRouter;

class ComparatorSession final : public mxs::RouterSession
                              , private ComparatorOtherBackend::Handler
{
public:
    using Stats = ComparatorSessionStats;

    ComparatorSession(const ComparatorSession&) = delete;
    ComparatorSession& operator=(const ComparatorSession&) = delete;

    ComparatorSession(MXS_SESSION* pSession,
                      ComparatorRouter* pRouter,
                      SComparatorMainBackend sMain,
                      SComparatorOtherBackends backends);
    ~ComparatorSession();

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    bool handleError(mxs::ErrorType type, const std::string& message,
                     mxs::Endpoint* pProblem, const mxs::Reply& reply) override;

private:
    // ComparatorOtherBackend::Handler
    ComparatorOtherBackend::Action ready(ComparatorOtherResult& other_result) override;
    void ready(const ComparatorExplainOtherResult& explain_other_result) override;

private:
    bool should_report(const ComparatorOtherResult& result) const;

    void generate_report(const ComparatorOtherResult& result);
    void generate_report(const ComparatorExplainOtherResult& result);

    void generate_report(const ComparatorOtherResult& other_result,
                         json_t* pExplain_other,
                         json_t* pExplain_main);
    json_t* generate_json(const ComparatorResult& result, json_t* pExplain);

    SComparatorMainBackend   m_sMain;
    SComparatorOtherBackends m_others;
    int                      m_responses = 0;
    ComparatorRouter&        m_router;
};
