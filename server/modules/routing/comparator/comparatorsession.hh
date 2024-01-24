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
    ComparatorOtherBackend::Action ready(const ComparatorOtherResult& other_result) override;
    void ready(const ComparatorExplainResult& explain_result,
               const std::string& error,
               std::string_view json) override;

private:
    void generate_report(const ComparatorOtherResult& result);
    void generate_already_explained_report(const ComparatorOtherResult& result,
                                           const std::vector<int64_t>& ids);
    void generate_report_with_explain(const ComparatorExplainResult& result, std::string_view explain_json);

    void generate_report(const ComparatorOtherResult& other_result,
                         const char* zExplain,
                         json_t* pExplain);
    json_t* generate_json(const ComparatorResult& result);

    SComparatorMainBackend   m_sMain;
    SComparatorOtherBackends m_others;
    int                      m_responses = 0;
    ComparatorRouter&        m_router;
};
