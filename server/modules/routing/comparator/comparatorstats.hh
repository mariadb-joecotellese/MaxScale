/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <maxscale/target.hh>

struct ComparatorStats
{
    std::chrono::nanoseconds total_duration { 0 };
    int64_t                  nRequest_packets { 0 };
    int64_t                  nRequests { 0 };
    int64_t                  nResponding_requests { 0 };
    int64_t                  nResponses { 0 };

    ComparatorStats& operator += (const ComparatorStats& rhs)
    {
        this->total_duration += rhs.total_duration;
        this->nRequest_packets += rhs.nRequest_packets;
        this->nRequests += rhs.nRequests;
        this->nResponding_requests += rhs.nResponding_requests;
        this->nResponses += rhs.nResponses;

        return *this;
    }
};

struct ComparatorMainStats : ComparatorStats
{
    // TODO: Placeholder.

    ComparatorMainStats& operator += (const ComparatorMainStats& rhs)
    {
        ComparatorStats::operator += (rhs);

        return *this;
    }
};

struct ComparatorOtherStats : ComparatorStats
{
    std::chrono::nanoseconds explain_duration { 0 };
    int64_t                  nExplain_requests { 0 };
    int64_t                  nExplain_responses { 0 };
    int64_t                  nFaster { 0 };
    int64_t                  nSlower { 0 };

    ComparatorOtherStats& operator += (const ComparatorOtherStats& rhs)
    {
        ComparatorStats::operator += (rhs);

        this->explain_duration += rhs.explain_duration;
        this->nExplain_requests += rhs.nExplain_requests;
        this->nExplain_responses += rhs.nExplain_responses;
        this->nFaster += rhs.nFaster;
        this->nSlower += rhs.nSlower;

        return *this;
    }
};

struct ComparatorSessionStats
{
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
};

struct ComparatorRouterStats
{
    // TODO: Placeholder.

    ComparatorSessionStats session_stats;
};
