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
    int64_t nFaster { 0 };
    int64_t nSlower { 0 };

    ComparatorOtherStats& operator += (const ComparatorOtherStats& rhs)
    {
        ComparatorStats::operator += (rhs);

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
    ComparatorRouterStats() = default;
    ComparatorRouterStats(const ComparatorRouterStats& rhs)
        : nTotal_sessions(rhs.nTotal_sessions.load(std::memory_order_relaxed))
        , nSessions(rhs.nSessions.load(std::memory_order_relaxed))
        , session_stats(rhs.session_stats)
    {
    }

    std::atomic<int64_t>   nTotal_sessions { 0 };
    std::atomic<int64_t>   nSessions { 0 };
    ComparatorSessionStats session_stats;
};
