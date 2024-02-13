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

class DiffMainBackend;
class DiffOtherBackend;
using SDiffMainBackend = std::unique_ptr<DiffMainBackend>;
using SDiffOtherBackend = std::unique_ptr<DiffOtherBackend>;
using SDiffOtherBackends = std::vector<SDiffOtherBackend>;

class DiffExporter;
class DiffRouter;
class DiffRouterSession;


/**
 * @class DiffBackend
 */
class DiffBackend : public mxs::Backend
{
public:
    ~DiffBackend() = default;

    using Result = DiffResult;
    using SResult = std::shared_ptr<Result>;
    using SDiffExplainResult = std::shared_ptr<DiffExplainResult>;


    void set_router_session(DiffRouterSession* pRouter_session);

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

    void schedule_explain(SDiffExplainResult&&);

protected:
    DiffBackend(mxs::Endpoint* pEndpoint);

    virtual void book_explain() = 0;

    std::unique_ptr<mariadb::QueryClassifier> m_sQc;
    const mxs::Parser*                        m_pParser { nullptr };
    const mxs::Parser::Helper*                m_pParser_helper { nullptr };
    std::deque<SResult>                       m_results;

private:
    bool execute(const SDiffExplainResult& sExplain_result);

    DiffRouterSession*             m_pRouter_session { nullptr };
    std::deque<SDiffExplainResult> m_pending_explains;
};


/**
 * @class DiffBackendWithStats
 */
template<class Stats>
class DiffBackendWithStats : public DiffBackend
{
public:
    const Stats& stats() const
    {
        return m_stats;
    }

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    Routing finish_result(const mxs::Reply& reply) override;

protected:
    DiffBackendWithStats(mxs::Endpoint* pEndpoint);

    void book_explain() override;

protected:
    Stats m_stats;
};


template<class Stats>
DiffBackendWithStats<Stats>::DiffBackendWithStats(mxs::Endpoint* pEndpoint)
    : DiffBackend(pEndpoint)
{
}

template<class Stats>
void DiffBackendWithStats<Stats>::book_explain()
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
bool DiffBackendWithStats<Stats>::write(GWBUF&& buffer, response_type type)
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
DiffBackend::Routing DiffBackendWithStats<Stats>::finish_result(const mxs::Reply& reply)
{
    mxb_assert(reply.is_complete());
    mxb_assert(!m_results.empty());

    auto sResult = std::move(m_results.front());
    m_results.pop_front();

    auto kind = sResult->kind();

    ++m_stats.nResponses;
    m_stats.total_duration += sResult->close(reply);

    return kind == DiffResult::Kind::EXTERNAL ? Routing::CONTINUE : Routing::STOP;
}


/**
 * @class DiffMainBackend
 */
class DiffMainBackend final : public DiffBackendWithStats<DiffMainStats>
{
public:
    using Base = DiffBackendWithStats<DiffMainStats>;
    using Result = DiffMainResult;
    using SResult = std::shared_ptr<Result>;

    DiffMainBackend(mxs::Endpoint* pEndpoint);

    SResult prepare(const GWBUF& packet);

    uint8_t command() const
    {
        return m_command;
    }

    void ready(const DiffExplainMainResult& result);

private:
    uint8_t m_command { 0 };
};


/**
 * @class DiffOtherBackend
 */
class DiffOtherBackend final : public DiffBackendWithStats<DiffOtherStats>
                             , private DiffOtherResult::Handler
                             , private DiffExplainOtherResult::Handler

{
public:
    using Base = DiffBackendWithStats<DiffOtherStats>;
    using Result = DiffOtherResult;
    using SResult = std::shared_ptr<Result>;

    class Handler
    {
    public:
        virtual Explain ready(DiffOtherResult& other_result) = 0;
        virtual void ready(const DiffExplainOtherResult& explain_result) = 0;
    };

    DiffOtherBackend(mxs::Endpoint* pEndpoint,
                     const DiffConfig* pConfig,
                     std::shared_ptr<DiffExporter> sExporter);

    void bump_requests_skipped()
    {
        ++m_stats.nRequests_skipped;
    }

    void set_result_handler(Handler* pHandler)
    {
        m_pHandler = pHandler;
    }

    DiffExporter& exporter() const
    {
        return *m_sExporter.get();
    }

    void prepare(const DiffMainBackend::SResult& sMain_result);

private:
    // DiffOtherResult::Handler
    void ready(DiffOtherResult& other_result) override;

    // DiffExplainResult::Handler
    void ready(const DiffExplainOtherResult& other_result) override;

private:
    using SDiffExporter = std::shared_ptr<DiffExporter>;

    const DiffConfig& m_config;
    SDiffExporter     m_sExporter;
    Handler*          m_pHandler { nullptr };
};


/**
 * @namespace diff
 */
namespace diff
{

std::pair<SDiffMainBackend, SDiffOtherBackends>
backends_from_endpoints(const mxs::Target& main_target,
                        const mxs::Endpoints& endpoints,
                        const DiffRouter& router);

}
