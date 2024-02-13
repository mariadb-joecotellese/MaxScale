/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <memory>
#include <maxscale/target.hh>

class SERVICE;

class DiffConfig;
class DiffOtherResult;

struct DiffStats
{
    std::chrono::nanoseconds total_duration { 0 };
    int64_t                  nRequest_packets { 0 };
    int64_t                  nRequests { 0 };
    int64_t                  nRequests_responding { 0 };
    int64_t                  nRequests_explainable { 0 };
    int64_t                  nResponses { 0 };
    std::chrono::nanoseconds explain_duration { 0 };
    int64_t                  nExplain_requests { 0 };
    int64_t                  nExplain_responses { 0 };

    void add(const DiffStats& rhs)
    {
        this->total_duration += rhs.total_duration;
        this->nRequest_packets += rhs.nRequest_packets;
        this->nRequests += rhs.nRequests;
        this->nRequests_explainable += rhs.nRequests_explainable;
        this->nRequests_responding += rhs.nRequests_responding;
        this->nResponses += rhs.nResponses;
        this->explain_duration += rhs.explain_duration;
        this->nExplain_requests += rhs.nExplain_requests;
        this->nExplain_responses += rhs.nExplain_responses;
    }

    void fill_json(json_t* pJson) const;
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
    int64_t nRequests_skipped { 0 };

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
