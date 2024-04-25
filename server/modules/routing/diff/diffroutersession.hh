/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <deque>
#include <maxscale/backend.hh>
#include <maxscale/buffer.hh>
#include <maxscale/queryclassifier.hh>
#include "diffbackend.hh"
#include "diffhistogram.hh"
#include "diffregistry.hh"
#include "diffresult.hh"
#include "diffstats.hh"

class DiffRouter;

class DiffRouterSession final : public mxs::RouterSession
                              , private DiffOtherBackend::Handler
{
public:
    using Stats = DiffRouterSessionStats;

    DiffRouterSession(const DiffRouterSession&) = delete;
    DiffRouterSession& operator=(const DiffRouterSession&) = delete;

    DiffRouterSession(MXS_SESSION* pSession,
                      DiffRouter* pRouter,
                      SDiffMainBackend sMain,
                      SDiffOtherBackends backends);
    ~DiffRouterSession();

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    bool handleError(mxs::ErrorType type, const std::string& message,
                     mxs::Endpoint* pProblem, const mxs::Reply& reply) override;

    /**
     * Get histogram specification for a particular canonical statement.
     *
     * @param canonical  A canonical statements.
     * @param duration   The duration the current execution took, used as a sample if needed.
     *
     * @return A histogram specification, if the bins have been sampled enough
     *         for the canonical statement in question. Otherwise an empty specification.
     */
    DiffHistogram::Specification get_specification_for(std::string_view canonical,
                                                       const mxb::Duration& duration);

    /**
     * Does a histogram specification exist for a particular canonical statement.
     *
     * @param canonical  A canonical statements.
     *
     * @return True, if a canonical specification exists, false otherwise.
     */
    bool has_specification_for(std::string_view canonical) const;

private:
    // DiffOtherBackend::Handler
    Explain ready(DiffOrdinaryOtherResult& other_result) override;
    void ready(const DiffExplainOtherResult& explain_other_result) override;

private:
    bool should_report(const DiffOrdinaryOtherResult& result) const;

    void generate_report(const DiffOrdinaryOtherResult& result);
    void generate_report(const DiffExplainOtherResult& result);

    void generate_report(const DiffOrdinaryOtherResult& other_result,
                         json_t* pExplain_other,
                         json_t* pExplain_main);
    json_t* generate_json(const DiffResult& result, json_t* pExplain);

    using HSRegistry = DiffHistogram::Specification::Registry;

    SDiffMainBackend                  m_sMain;
    SDiffOtherBackends                m_others;
    int                               m_responses = 0;
    DiffRouter&                       m_router;
    std::shared_ptr<const HSRegistry> m_sHSRegistry;
};
