/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <deque>
#include <vector>
#include <memory>
#include <maxbase/checksum.hh>
#include <maxscale/backend.hh>
#include <maxscale/parser.hh>
#include <maxscale/router.hh>
#include "comparatorresult.hh"

class ComparatorMainBackend;
class ComparatorOtherBackend;
using SComparatorMainBackend = std::unique_ptr<ComparatorMainBackend>;
using SComparatorOtherBackend = std::unique_ptr<ComparatorOtherBackend>;
using SComparatorOtherBackends = std::vector<SComparatorOtherBackend>;

class ComparatorExporter;
class ComparatorRouter;

class ComparatorBackend : public mxs::Backend
{
public:
    struct Stats
    {
        std::chrono::nanoseconds total_duration { 0 };
        int64_t                  nRequest_packets { 0 };
        int64_t                  nRequests { 0 };
        int64_t                  nResponses { 0 };
    };

    using Result = ComparatorResult;
    using SResult = std::shared_ptr<Result>;

    void set_parser_helper(const mxs::Parser::Helper* pHelper)
    {
        m_pParser_helper = pHelper;
    }

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    virtual const Stats& stats() const
    {
        return *m_sStats.get();
    }

    bool multi_part_in_process() const
    {
        return m_multi_part_in_process;
    }

    void process_result(const GWBUF& buffer)
    {
        mxb_assert(!m_results.empty());

        m_results.front()->process(buffer);
    }

    void finish_result(const mxs::Reply& reply)
    {
        mxb_assert(reply.is_complete());
        mxb_assert(!m_results.empty());

        auto sResult = std::move(m_results.front());
        m_results.pop_front();

        ++m_sStats->nResponses;
        m_sStats->total_duration += sResult->close(reply);
    }

    void close(close_type type = CLOSE_NORMAL) override
    {
        mxs::Backend::close(type);

        m_results.clear();
    }

    int32_t nBacklog() const
    {
        return m_results.size();
    }

protected:
    ComparatorBackend(mxs::Endpoint* pEndpoint, std::unique_ptr<Stats> sStats)
        : mxs::Backend(pEndpoint)
        , m_sStats(std::move(sStats))
    {
    }

    const mxs::Parser::Helper& ph() const
    {
        mxb_assert(m_pParser_helper);
        return *m_pParser_helper;
    }

protected:
    const mxs::Parser::Helper* m_pParser_helper { nullptr };
    bool                       m_multi_part_in_process { false };
    std::deque<SResult>        m_results;
    std::unique_ptr<Stats>     m_sStats;
};

class ComparatorMainBackend final : public ComparatorBackend
{
public:
    struct MainStats : Stats
    {
        // TODO: Placeholder.
    };

    using Result = ComparatorMainResult;
    using SResult = std::shared_ptr<Result>;

    ComparatorMainBackend(mxs::Endpoint* pEndpoint)
        : ComparatorBackend(pEndpoint, std::make_unique<MainStats>())
    {
    }

    const MainStats& stats() const override
    {
        return static_cast<const MainStats&>(ComparatorBackend::stats());
    }

    SResult prepare(const GWBUF& packet);

    const std::string& sql() const
    {
        return m_sql;
    }

    uint8_t command() const
    {
        return m_command;
    }

private:
    std::string m_sql;
    uint8_t     m_command { 0 };
};

class ComparatorOtherBackend final : public ComparatorBackend
                                   , private ComparatorOtherResult::Handler
                                   , private ComparatorExplainResult::Handler

{
public:
    struct OtherStats : Stats
    {
        int64_t nFaster { 0 };
        int64_t nSlower { 0 };
    };

    enum Action
    {
        CONTINUE,
        EXPLAIN
    };

    class Handler
    {
    public:
        virtual Action ready(const ComparatorOtherResult& other_result) = 0;
        virtual void ready(const ComparatorExplainResult& explain_result,
                           const std::string& error,
                           std::string_view json) = 0;
    };

    using Result = ComparatorOtherResult;
    using SResult = std::shared_ptr<Result>;

    ComparatorOtherBackend(mxs::Endpoint* pEndpoint,
                           std::shared_ptr<ComparatorExporter> sExporter)
        : ComparatorBackend(pEndpoint, std::make_unique<OtherStats>())
        , m_sExporter(std::move(sExporter))
    {
    }

    void set_result_handler(Handler* pHandler)
    {
        m_pHandler = pHandler;
    }

    const OtherStats& stats() const override
    {
        return static_cast<const OtherStats&>(ComparatorBackend::stats());
    }

    ComparatorExporter& exporter() const
    {
        return *m_sExporter.get();
    }

    void prepare(const ComparatorMainBackend::SResult& sMain_result);

private:
    // ComparatorOtherResult::Handler
    void ready(const ComparatorOtherResult& other_result) override;

    // ComparatorExplainResult::Handler
    void ready(const ComparatorExplainResult& other_result,
               const std::string& error,
               std::string_view json) override;

private:
    std::shared_ptr<ComparatorExporter> m_sExporter;
    Handler*                            m_pHandler { nullptr };
};

namespace comparator
{

std::pair<SComparatorMainBackend, SComparatorOtherBackends>
backends_from_endpoints(const mxs::Target& main_target,
                        const mxs::Endpoints& endpoints,
                        const ComparatorRouter& router);

}
