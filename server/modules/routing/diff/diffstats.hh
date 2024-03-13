/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <memory>
#include <maxscale/response_distribution.hh>
#include <maxscale/target.hh>

class SERVICE;

class DiffConfig;
class DiffOtherResult;

struct DiffStats
{
    std::chrono::nanoseconds total_duration() const
    {
        return m_total_duration;
    }

    void add_total_duration(const std::chrono::nanoseconds& duration)
    {
        m_total_duration += duration;
        m_response_distribution.add(duration);
    }

    int64_t nRequest_packets() const
    {
        return m_nRequest_packets;
    }

    void inc_request_packets()
    {
        ++m_nRequest_packets;
    }

    void dec_request_packets()
    {
        --m_nRequest_packets;
    }

    int64_t nRequests() const
    {
        return m_nRequests;
    }

    void inc_requests()
    {
        ++m_nRequests;
    }

    void dec_requests()
    {
        --m_nRequests;
    }

    int64_t nRequests_responding() const
    {
        return m_nRequests_responding;
    }

    void inc_requests_responding()
    {
        ++m_nRequests_responding;
    }

    void dec_requests_responding()
    {
        --m_nRequests_responding;
    }

    int64_t nRequests_explainable() const
    {
        return m_nRequests_explainable;
    }

    void inc_requests_explainable()
    {
        ++m_nRequests_explainable;
    }

    void dec_requests_explainable()
    {
        --m_nRequests_explainable;
    }

    int64_t nResponses() const
    {
        return m_nResponses;
    }

    void inc_responses()
    {
        ++m_nResponses;
    }

    void dec_responses()
    {
        --m_nResponses;
    }

    std::chrono::nanoseconds explain_duration() const
    {
        return m_explain_duration;
    }

    void add_explain_duration(const std::chrono::nanoseconds& duration)
    {
        m_explain_duration += duration;
    }

    int64_t nExplain_requests() const
    {
        return m_nExplain_requests;
    }

    void inc_explain_requests()
    {
        ++m_nExplain_requests;
    }

    int64_t nExplain_responses() const
    {
        return m_nExplain_responses;
    }

    void inc_explain_responses()
    {
        ++m_nExplain_responses;
    }

    const mxs::ResponseDistribution& response_distribution() const
    {
        return m_response_distribution;
    }

    void add(const DiffStats& rhs)
    {
        m_total_duration += rhs.m_total_duration;
        m_nRequest_packets += rhs.m_nRequest_packets;
        m_nRequests += rhs.m_nRequests;
        m_nRequests_explainable += rhs.m_nRequests_explainable;
        m_nRequests_responding += rhs.m_nRequests_responding;
        m_nResponses += rhs.m_nResponses;
        m_explain_duration += rhs.m_explain_duration;
        m_nExplain_requests += rhs.m_nExplain_requests;
        m_nExplain_responses += rhs.m_nExplain_responses;

        m_response_distribution += rhs.m_response_distribution;
    }

    void fill_json(json_t* pJson) const;

protected:
    std::chrono::nanoseconds  m_total_duration { 0 };
    int64_t                   m_nRequest_packets { 0 };
    int64_t                   m_nRequests { 0 };
    int64_t                   m_nRequests_responding { 0 };
    int64_t                   m_nRequests_explainable { 0 };
    int64_t                   m_nResponses { 0 };
    std::chrono::nanoseconds  m_explain_duration { 0 };
    int64_t                   m_nExplain_requests { 0 };
    int64_t                   m_nExplain_responses { 0 };

    mxs::ResponseDistribution m_response_distribution;
};

struct DiffMainStats final : DiffStats
{
    // TODO: Placeholder.

    void add(const DiffMainStats& rhs)
    {
        DiffStats::add(rhs);
    }

    json_t* to_json() const;
};

struct DiffOtherStats final : DiffStats
{
    int64_t requests_skipped() const
    {
        return m_nRequests_skipped;
    }

    void inc_requests_skipped()
    {
        ++m_nRequests_skipped;
    }

    int64_t nFaster() const
    {
        return m_nFaster;
    }

    int64_t nSlower() const
    {
        return m_nSlower;
    }

    using ResultsByPermille = std::multimap<int64_t, std::shared_ptr<const DiffOtherResult>>;

    const ResultsByPermille& faster_requests() const
    {
        return m_faster_requests;
    }

    const ResultsByPermille& slower_requests() const
    {
        return m_slower_requests;
    }

    void add_result(const DiffOtherResult& result, const DiffConfig& config);

    void add(const DiffOtherStats& stats, const DiffConfig& config);

    json_t* to_json() const;

private:
    int64_t           m_nRequests_skipped { 0 };
    int64_t           m_nFaster { 0 };
    int64_t           m_nSlower { 0 };
    ResultsByPermille m_faster_requests;
    ResultsByPermille m_slower_requests;
};

struct DiffRouterSessionStats
{
    mxs::Target*                           pMain { nullptr };
    DiffMainStats                          main_stats;
    std::map<mxs::Target*, DiffOtherStats> other_stats;
    mxs::ResponseDistribution              response_distribution;

    void add(const DiffRouterSessionStats& rhs, const DiffConfig& config)
    {
        this->main_stats.add(rhs.main_stats);

        for (const auto& kv : rhs.other_stats)
        {
            auto it = this->other_stats.find(kv.first);

            if (it != this->other_stats.end())
            {
                it->second.add(kv.second, config);
            }
            else
            {
                other_stats.insert(kv);
            }
        }

        this->response_distribution += rhs.response_distribution;
    }

    json_t* to_json() const;
};

class DiffRouterStats
{
public:
    DiffRouterStats(const SERVICE* pService)
        : m_service(*pService)
    {
    }

    void add(const DiffRouterSessionStats& rhs, const DiffConfig& config)
    {
        mxb_assert(m_router_session_stats.pMain == rhs.pMain);

        m_router_session_stats.add(rhs, config);
    }

    void post_configure(const DiffConfig& config);

    json_t* to_json() const;

private:
    const SERVICE&         m_service;
    DiffRouterSessionStats m_router_session_stats;
};
