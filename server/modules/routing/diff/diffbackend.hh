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

    virtual int32_t nBacklog() const = 0;

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

protected:
    DiffBackend(mxs::Endpoint* pEndpoint);

    virtual DiffResult* front() = 0;

    virtual void book_explain() = 0;

    void lcall(std::function<bool()>&& fn);

    std::unique_ptr<mariadb::QueryClassifier> m_sQc;
    const mxs::Parser*                        m_pParser { nullptr };
    const mxs::Parser::Helper*                m_pParser_helper { nullptr };

protected:
    DiffRouterSession* m_pRouter_session { nullptr };
};


/**
 * @class DiffConcreteBackend
 */
template<class Stats, class Result, class ExplainResult>
class DiffConcreteBackend : public DiffBackend
{
public:
    using SResult = std::shared_ptr<Result>;
    using SExplainResult = std::shared_ptr<ExplainResult>;

    int32_t nBacklog() const override
    {
        return m_results.size();
    }

    const Stats& stats() const
    {
        return m_stats;
    }

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    Routing finish_result(const mxs::Reply& reply) override;

    void close(close_type type = CLOSE_NORMAL) override;

    void execute_pending_explains();

    void schedule_explain(SExplainResult&&);

protected:
    DiffConcreteBackend(mxs::Endpoint* pEndpoint);

    DiffResult* front() override;

    void book_explain() override;

protected:
    Stats                      m_stats;
    std::deque<SResult>        m_results;
    std::deque<SExplainResult> m_pending_explains;

private:
    bool execute(const SExplainResult& sExplain_result);
};


template<class Stats, class Result, class ExplainResult>
DiffConcreteBackend<Stats, Result, ExplainResult>::DiffConcreteBackend(mxs::Endpoint* pEndpoint)
    : DiffBackend(pEndpoint)
{
}

template<class Stats, class Result, class ExplainResult>
void DiffConcreteBackend<Stats, Result, ExplainResult>::book_explain()
{
    m_stats.inc_explain_requests();

    // Tune general counters, since those related to the extra
    // EXPLAIN requests should be exluded.
    m_stats.dec_request_packets();
    m_stats.dec_requests();
    m_stats.dec_requests_explainable();
    m_stats.dec_requests_responding();
}

template<class Stats, class Result, class ExplainResult>
bool DiffConcreteBackend<Stats, Result, ExplainResult>::write(GWBUF&& buffer, response_type type)
{
    mxb_assert(m_sQc);
    m_sQc->update_and_commit_route_info(buffer);

    m_stats.inc_request_packets();

    if (!extraordinary_in_process())
    {
        m_stats.inc_requests();

        if (type != NO_RESPONSE)
        {
            m_stats.inc_requests_responding();

            auto sql = phelper().get_sql(buffer);

            if (!sql.empty())
            {
                m_stats.inc_requests_explainable();
            }
        }
    }

    return Backend::write(std::move(buffer), type);
}

template<class Stats, class Result, class ExplainResult>
DiffBackend::Routing DiffConcreteBackend<Stats, Result, ExplainResult>::finish_result(const mxs::Reply& reply)
{
    mxb_assert(reply.is_complete());
    mxb_assert(!m_results.empty());

    auto sResult = std::move(m_results.front());
    m_results.pop_front();

    auto kind = sResult->kind();

    m_stats.inc_responses();

    auto canonical = sResult->canonical();
    auto duration = sResult->close(reply);

    m_stats.add_canonical_result(canonical, duration);

    return kind == DiffResult::Kind::EXTERNAL ? Routing::CONTINUE : Routing::STOP;
}

template<class Stats, class Result, class ExplainResult>
void DiffConcreteBackend<Stats, Result, ExplainResult>::close(close_type type)
{
    DiffBackend::close(type);

    m_results.clear();
}

template<class Stats, class Result, class ExplainResult>
void DiffConcreteBackend<Stats, Result, ExplainResult>::execute_pending_explains()
{
    lcall([this] {
        bool rv = true;

        if (!extraordinary_in_process())
        {
            while (rv && !m_pending_explains.empty())
            {
                auto sExplain_result = std::move(m_pending_explains.front());
                m_pending_explains.pop_front();

                rv = execute(sExplain_result);
            }
        }

        return rv;
    });
}

template<class Stats, class Result, class ExplainResult>
void DiffConcreteBackend<Stats, Result, ExplainResult>::schedule_explain(SExplainResult&& sExplain_result)
{
    m_pending_explains.emplace_back(std::move(sExplain_result));
}

template<class Stats, class Result, class ExplainResult>
DiffResult* DiffConcreteBackend<Stats, Result, ExplainResult>::front()
{
    return m_results.empty() ? nullptr : m_results.front().get();
}

template<class Stats, class Result, class ExplainResult>
bool DiffConcreteBackend<Stats, Result, ExplainResult>::execute(const SExplainResult& sExplain_result)
{
    std::string sql { "EXPLAIN FORMAT=JSON "};
    sql += sExplain_result->sql();

    m_results.emplace_back(std::move(sExplain_result));

    GWBUF packet = phelper().create_packet(sql);
    packet.set_type(static_cast<GWBUF::Type>(GWBUF::TYPE_COLLECT_RESULT | GWBUF::TYPE_COLLECT_ROWS));

    bool rv = write(std::move(packet), mxs::Backend::EXPECT_RESPONSE);

    // TODO: Need to consider just how a failure to write should affect
    // TODO: the statistics.
    book_explain();

    return rv;
}

/**
 * @class DiffMainBackend
 */
class DiffMainBackend final : public DiffConcreteBackend<DiffMainStats, DiffResult, DiffExplainMainResult>
{
public:
    using Base = DiffConcreteBackend<DiffMainStats, DiffResult, DiffExplainMainResult>;

    DiffMainBackend(mxs::Endpoint* pEndpoint);

    std::shared_ptr<DiffOrdinaryMainResult> prepare(const GWBUF& packet);

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
class DiffOtherBackend final : public DiffConcreteBackend<DiffOtherStats, DiffResult, DiffExplainOtherResult>
                             , private DiffOrdinaryOtherResult::Handler
                             , private DiffExplainOtherResult::Handler

{
public:
    using Base = DiffConcreteBackend<DiffOtherStats, DiffResult, DiffExplainOtherResult>;

    class Handler
    {
    public:
        virtual Explain ready(DiffOrdinaryOtherResult& other_result) = 0;
        virtual void ready(const DiffExplainOtherResult& explain_result) = 0;
    };

    DiffOtherBackend(mxs::Endpoint* pEndpoint,
                     const DiffConfig* pConfig,
                     std::shared_ptr<DiffExporter> sExporter);
    ~DiffOtherBackend();

    void inc_requests_skipped()
    {
        m_stats.inc_requests_skipped();
    }

    void set_result_handler(Handler* pHandler)
    {
        m_pHandler = pHandler;
    }

    DiffExporter& exporter() const
    {
        return *m_sExporter.get();
    }

    void prepare(const std::shared_ptr<DiffOrdinaryMainResult>& sMain_result);

private:
    // DiffOrdinaryOtherResult::Handler
    void ready(DiffOrdinaryOtherResult& other_result) override;

    // DiffExplainResult::Handler
    void ready(DiffExplainOtherResult& other_result) override;

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
