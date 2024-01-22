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
#include "comparatorstats.hh"

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
    using Result = ComparatorResult;
    using SResult = std::shared_ptr<Result>;

    void set_parser_helper(const mxs::Parser::Helper* pHelper)
    {
        m_pParser_helper = pHelper;
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

    virtual void finish_result(const mxs::Reply& reply) = 0;

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
    ComparatorBackend(mxs::Endpoint* pEndpoint)
        : mxs::Backend(pEndpoint)
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
};

template<class Stats>
class ComparatorBackendWithStats : public ComparatorBackend
{
public:
    const Stats& stats() const
    {
        return m_stats;
    }

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override
    {
        bool multi_part = ph().is_multi_part_packet(buffer);

        ++m_stats.nRequest_packets;

        if (!m_multi_part_in_process)
        {
            ++m_stats.nRequests;

            if (type != NO_RESPONSE)
            {
                ++m_stats.nRequests_responding;

                auto sql = ph().get_sql(buffer);

                if (!sql.empty())
                {
                    ++m_stats.nRequests_explainable;
                }
            }
        }

        m_multi_part_in_process = multi_part;

        return Backend::write(std::move(buffer), type);
    }

    void finish_result(const mxs::Reply& reply) override
    {
        mxb_assert(reply.is_complete());
        mxb_assert(!m_results.empty());

        auto sResult = std::move(m_results.front());
        m_results.pop_front();

        ++m_stats.nResponses;
        m_stats.total_duration += sResult->close(reply);
    }

protected:
    ComparatorBackendWithStats(mxs::Endpoint* pEndpoint)
        : ComparatorBackend(pEndpoint)
    {
    }

protected:
    Stats m_stats;
};


class ComparatorMainBackend final : public ComparatorBackendWithStats<ComparatorMainStats>
{
public:
    using Base = ComparatorBackendWithStats<ComparatorMainStats>;
    using Result = ComparatorMainResult;
    using SResult = std::shared_ptr<Result>;

    ComparatorMainBackend(mxs::Endpoint* pEndpoint)
        : Base(pEndpoint)
    {
    }

    SResult prepare(const GWBUF& packet);

    uint8_t command() const
    {
        return m_command;
    }

private:
    uint8_t m_command { 0 };
};


class ComparatorOtherBackend final : public ComparatorBackendWithStats<ComparatorOtherStats>
                                   , private ComparatorOtherResult::Handler
                                   , private ComparatorExplainResult::Handler

{
public:
    using Base = ComparatorBackendWithStats<ComparatorOtherStats>;
    using Result = ComparatorOtherResult;
    using SResult = std::shared_ptr<Result>;

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

    ComparatorOtherBackend(mxs::Endpoint* pEndpoint,
                           std::shared_ptr<ComparatorExporter> sExporter)
        : Base(pEndpoint)
        , m_sExporter(std::move(sExporter))
    {
    }

    void set_result_handler(Handler* pHandler)
    {
        m_pHandler = pHandler;
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
    using SComparatorExplainResult = std::shared_ptr<ComparatorExplainResult>;
    using SComparatorExporter = std::shared_ptr<ComparatorExporter>;

    void execute_pending_explains();
    void execute(const SComparatorExplainResult& sExplain_result);

    SComparatorExporter                  m_sExporter;
    Handler*                             m_pHandler { nullptr };
    std::deque<SComparatorExplainResult> m_pending_explains;
};

namespace comparator
{

std::pair<SComparatorMainBackend, SComparatorOtherBackends>
backends_from_endpoints(const mxs::Target& main_target,
                        const mxs::Endpoints& endpoints,
                        const ComparatorRouter& router);

}
