/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <maxscale/target.hh>

class CConfig;
class SERVICE;

struct CStats
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

    CStats& operator += (const CStats& rhs)
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

struct CMainStats final : CStats
{
    // TODO: Placeholder.

    CMainStats& operator += (const CMainStats& rhs)
    {
        CStats::operator += (rhs);

        return *this;
    }

    json_t* to_json() const;
};

struct COtherStats final : CStats
{
    int64_t                  nRequests_skipped { 0 };
    int64_t                  nFaster { 0 };
    int64_t                  nSlower { 0 };

    COtherStats& operator += (const COtherStats& rhs)
    {
        CStats::operator += (rhs);

        this->nRequests_skipped += rhs.nRequests_skipped;
        this->nFaster += rhs.nFaster;
        this->nSlower += rhs.nSlower;

        return *this;
    }

    json_t* to_json() const;
};

struct CSessionStats
{
    mxs::Target*                        pMain { nullptr };
    CMainStats                          main_stats;
    std::map<mxs::Target*, COtherStats> other_stats;

    CSessionStats& operator += (const CSessionStats& rhs)
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

class CRouterStats
{
public:
    CRouterStats(const SERVICE* pService)
        : m_service(*pService)
    {
    }

    CRouterStats& operator += (const CSessionStats& rhs)
    {
        mxb_assert(m_session_stats.pMain == rhs.pMain);

        m_session_stats += rhs;
        return *this;
    }

    void post_configure(const CConfig& config);

    json_t* to_json() const;

private:
    const SERVICE& m_service;
    CSessionStats  m_session_stats;
};
