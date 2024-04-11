/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <memory>
#include <maxscale/target.hh>
#include "diffdata.hh"

class SERVICE;

class DiffConfig;
class DiffOrdinaryOtherResult;
class DiffRouterSession;

class DiffStats
{
public:
    using Datas = std::map<std::string, DiffData, std::less<>>;

    std::chrono::nanoseconds total_duration() const
    {
        return m_total_duration;
    }

    void add_canonical_result(DiffRouterSession& router_session,
                              std::string_view canonical,
                              const std::chrono::nanoseconds& duration,
                              const mxs::Reply& reply);

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

    const Datas& datas() const
    {
        return m_datas;
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

        for (const auto& kv : rhs.m_datas)
        {
            auto it = m_datas.find(kv.first);

            if (it != m_datas.end())
            {
                it->second += kv.second;
            }
            else
            {
                m_datas.emplace(kv.first, kv.second);
            }
        }
    }

    json_t* get_statistics() const;
    json_t* get_data() const;

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
    Datas                     m_datas;
};

class DiffMainStats final : public DiffStats
{
public:
    void add(const DiffMainStats& rhs)
    {
        DiffStats::add(rhs);
    }

    json_t* to_json() const;
};

class DiffOtherStats final : public DiffStats
{
public:
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

    using ResultsByPermille = std::multimap<int64_t, std::shared_ptr<const DiffOrdinaryOtherResult>>;

    const ResultsByPermille& faster_requests() const
    {
        return m_faster_requests;
    }

    const ResultsByPermille& slower_requests() const
    {
        return m_slower_requests;
    }

    void add_result(const DiffOrdinaryOtherResult& result, const DiffConfig& config);

    void add(const DiffOtherStats& stats, const DiffConfig& config);

    json_t* to_json() const;

private:
    int64_t           m_nRequests_skipped { 0 };
    int64_t           m_nFaster { 0 };
    int64_t           m_nSlower { 0 };
    ResultsByPermille m_faster_requests;
    ResultsByPermille m_slower_requests;
};

class DiffRouterSessionStats
{
public:
    DiffRouterSessionStats() = default;
    DiffRouterSessionStats(mxs::Target* pMain, const DiffMainStats& main_stats);

    mxs::Target* main() const
    {
        return m_pMain;
    }

    void set_main(mxs::Target* pMain)
    {
        mxb_assert(!m_pMain);

        m_pMain = pMain;
    }

    void add_other(mxs::Target* pOther, const DiffOtherStats& other_stats);

    void add(const DiffRouterSessionStats& rhs, const DiffConfig& config)
    {
        m_main_stats.add(rhs.m_main_stats);

        for (const auto& kv : rhs.m_other_stats)
        {
            auto it = m_other_stats.find(kv.first);

            if (it != m_other_stats.end())
            {
                it->second.add(kv.second, config);
            }
            else
            {
                m_other_stats.insert(kv);
            }
        }
    }

    json_t* to_json() const;

    std::map<mxs::Target*, json_t*> get_data() const;

private:
    mxs::Target*                           m_pMain { nullptr };
    DiffMainStats                          m_main_stats;
    std::map<mxs::Target*, DiffOtherStats> m_other_stats;
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
        mxb_assert(m_router_session_stats.main() == rhs.main());

        m_router_session_stats.add(rhs, config);
    }

    void post_configure(const DiffConfig& config);

    json_t* to_json() const;

    std::map<mxs::Target*, json_t*> get_data() const
    {
        return m_router_session_stats.get_data();
    }

private:
    const SERVICE&         m_service;
    DiffRouterSessionStats m_router_session_stats;
};
