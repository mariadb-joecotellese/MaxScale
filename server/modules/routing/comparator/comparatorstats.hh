/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <maxscale/target.hh>

class ComparatorConfig;
class SERVICE;

struct ComparatorStats
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

    ComparatorStats& operator += (const ComparatorStats& rhs)
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

        return *this;
    }

    void fill_json(json_t* pJson) const;
};

struct ComparatorMainStats final : ComparatorStats
{
    // TODO: Placeholder.

    ComparatorMainStats& operator += (const ComparatorMainStats& rhs)
    {
        ComparatorStats::operator += (rhs);

        return *this;
    }

    json_t* to_json() const;
};

struct ComparatorOtherStats final : ComparatorStats
{
    int64_t                  nRequests_skipped { 0 };
    int64_t                  nFaster { 0 };
    int64_t                  nSlower { 0 };

    ComparatorOtherStats& operator += (const ComparatorOtherStats& rhs)
    {
        ComparatorStats::operator += (rhs);

        this->nRequests_skipped += rhs.nRequests_skipped;
        this->nFaster += rhs.nFaster;
        this->nSlower += rhs.nSlower;

        return *this;
    }

    json_t* to_json() const;
};

struct ComparatorSessionStats
{
    mxs::Target*                                 pMain { nullptr };
    ComparatorMainStats                          main_stats;
    std::map<mxs::Target*, ComparatorOtherStats> other_stats;

    ComparatorSessionStats& operator += (const ComparatorSessionStats& rhs)
    {
        this->main_stats += rhs.main_stats;

        for (const auto& kv : rhs.other_stats)
        {
            this->other_stats[kv.first] += kv.second;
        }

        return *this;
    }

    json_t* to_json() const;
};

class ComparatorRouterStats
{
public:
    ComparatorRouterStats(const SERVICE* pService)
        : m_service(*pService)
    {
    }

    ComparatorRouterStats& operator += (const ComparatorSessionStats& rhs)
    {
        mxb_assert(m_session_stats.pMain == rhs.pMain);

        m_session_stats += rhs;
        return *this;
    }

    void post_configure(const ComparatorConfig& config);

    json_t* to_json() const;

private:
    const SERVICE&         m_service;
    ComparatorSessionStats m_session_stats;
};
