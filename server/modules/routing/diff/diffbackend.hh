/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <deque>
#include <vector>
#include <memory>
#include <maxbase/checksum.hh>
#include <maxscale/backend.hh>
#include <maxscale/parser.hh>
#include <maxscale/queryclassifier.hh>
#include <maxscale/router.hh>
#include "diffresult.hh"
#include "diffstats.hh"

class CMainBackend;
class COtherBackend;
using SCMainBackend = std::unique_ptr<CMainBackend>;
using SCOtherBackend = std::unique_ptr<COtherBackend>;
using SCOtherBackends = std::vector<SCOtherBackend>;

class CExporter;
class CRouter;
class CRouterSession;


/**
 * @class CBackend
 */
class CBackend : public mxs::Backend
{
public:
    ~CBackend() = default;

    using Result = CResult;
    using SResult = std::shared_ptr<Result>;
    using SCExplainResult = std::shared_ptr<CExplainResult>;


    void set_router_session(CRouterSession* pRouter_session);

    bool extraordinary_in_process() const;

    void process_result(const GWBUF& buffer, const mxs::Reply& reply);

    enum class Routing
    {
        CONTINUE, // Send the response further to the client.
        STOP      // The response relates to internal activity, do not send to client.
    };

    virtual Routing finish_result(const mxs::Reply& reply) = 0;

    void close(close_type type = CLOSE_NORMAL) override;

    int32_t nBacklog() const
    {
        return m_results.size();
    }

    const mxs::Parser& parser() const
    {
        mxb_assert(m_pParser);
        return *m_pParser;
    }

    const mxs::Parser::Helper& phelper() const
    {
        mxb_assert(m_pParser_helper);
        return *m_pParser_helper;
    }

    void execute_pending_explains();

    void schedule_explain(SCExplainResult&&);

protected:
    CBackend(mxs::Endpoint* pEndpoint);

    virtual void book_explain() = 0;

    std::unique_ptr<mariadb::QueryClassifier> m_sQc;
    const mxs::Parser*                        m_pParser { nullptr };
    const mxs::Parser::Helper*                m_pParser_helper { nullptr };
    std::deque<SResult>                       m_results;

private:
    bool execute(const SCExplainResult& sExplain_result);

    CRouterSession*             m_pRouter_session { nullptr };
    std::deque<SCExplainResult> m_pending_explains;
};


/**
 * @class CBackendWithStats
 */
template<class Stats>
class CBackendWithStats : public CBackend
{
public:
    const Stats& stats() const
    {
        return m_stats;
    }

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    Routing finish_result(const mxs::Reply& reply) override;

protected:
    CBackendWithStats(mxs::Endpoint* pEndpoint);

    void book_explain() override;

protected:
    Stats m_stats;
};


template<class Stats>
CBackendWithStats<Stats>::CBackendWithStats(mxs::Endpoint* pEndpoint)
    : CBackend(pEndpoint)
{
}

template<class Stats>
void CBackendWithStats<Stats>::book_explain()
{
    ++m_stats.nExplain_requests;

    // Tune general counters, since those related to the extra
    // EXPLAIN requests should be exluded.
    --m_stats.nRequest_packets;
    --m_stats.nRequests;
    --m_stats.nRequests_explainable;
    --m_stats.nRequests_responding;
}

template<class Stats>
bool CBackendWithStats<Stats>::write(GWBUF&& buffer, response_type type)
{
    mxb_assert(m_sQc);
    m_sQc->update_and_commit_route_info(buffer);

    ++m_stats.nRequest_packets;

    if (!extraordinary_in_process())
    {
        ++m_stats.nRequests;

        if (type != NO_RESPONSE)
        {
            ++m_stats.nRequests_responding;

            auto sql = phelper().get_sql(buffer);

            if (!sql.empty())
            {
                ++m_stats.nRequests_explainable;
            }
        }
    }

    return Backend::write(std::move(buffer), type);
}

template<class Stats>
CBackend::Routing CBackendWithStats<Stats>::finish_result(const mxs::Reply& reply)
{
    mxb_assert(reply.is_complete());
    mxb_assert(!m_results.empty());

    auto sResult = std::move(m_results.front());
    m_results.pop_front();

    auto kind = sResult->kind();

    ++m_stats.nResponses;
    m_stats.total_duration += sResult->close(reply);

    return kind == CResult::Kind::EXTERNAL ? Routing::CONTINUE : Routing::STOP;
}


/**
 * @class CMainBackend
 */
class CMainBackend final : public CBackendWithStats<CMainStats>
{
public:
    using Base = CBackendWithStats<CMainStats>;
    using Result = CMainResult;
    using SResult = std::shared_ptr<Result>;

    CMainBackend(mxs::Endpoint* pEndpoint);

    SResult prepare(const GWBUF& packet);

    uint8_t command() const
    {
        return m_command;
    }

    void ready(const CExplainMainResult& result);

private:
    uint8_t m_command { 0 };
};


/**
 * @class COtherBackend
 */
class COtherBackend final : public CBackendWithStats<COtherStats>
                          , private COtherResult::Handler
                          , private CExplainOtherResult::Handler

{
public:
    using Base = CBackendWithStats<COtherStats>;
    using Result = COtherResult;
    using SResult = std::shared_ptr<Result>;

    class Handler
    {
    public:
        virtual Explain ready(COtherResult& other_result) = 0;
        virtual void ready(const CExplainOtherResult& explain_result) = 0;
    };

    COtherBackend(mxs::Endpoint* pEndpoint,
                  const CConfig* pConfig,
                  std::shared_ptr<CExporter> sExporter);

    void bump_requests_skipped()
    {
        ++m_stats.nRequests_skipped;
    }

    void set_result_handler(Handler* pHandler)
    {
        m_pHandler = pHandler;
    }

    CExporter& exporter() const
    {
        return *m_sExporter.get();
    }

    void prepare(const CMainBackend::SResult& sMain_result);

private:
    // COtherResult::Handler
    void ready(COtherResult& other_result) override;

    // CExplainResult::Handler
    void ready(const CExplainOtherResult& other_result) override;

private:
    using SCExporter = std::shared_ptr<CExporter>;

    const CConfig& m_config;
    SCExporter     m_sExporter;
    Handler*       m_pHandler { nullptr };
};


/**
 * @namespace comparator
 */
namespace comparator
{

std::pair<SCMainBackend, SCOtherBackends>
backends_from_endpoints(const mxs::Target& main_target,
                        const mxs::Endpoints& endpoints,
                        const CRouter& router);

}
