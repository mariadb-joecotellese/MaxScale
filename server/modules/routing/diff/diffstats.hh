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
#include "diffqps.hh"

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

    void add_explain_result(std::string_view canonical,
                            const std::chrono::nanoseconds& duration,
                            const mxb::TimePoint& now,
                            std::string_view sql,
                            json_t* pExplain);

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

    virtual json_t* to_json() const;

protected:
    DiffStats() = default;

    virtual json_t* get_statistics() const;

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

private:
};


class DiffMainStats final : public DiffStats
{
public:
    DiffMainStats() = default;
};


class DiffOtherStats final : public DiffStats
{
public:
    DiffOtherStats() = default;

    DiffOtherStats(const DiffOtherStats& other) = default;

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

    json_t* to_json() const override;

private:
    json_t* get_statistics() const override;
    json_t* get_verdict() const;

    int64_t           m_nRequests_skipped { 0 };
    int64_t           m_nFaster { 0 };
    int64_t           m_nSlower { 0 };
    ResultsByPermille m_faster_requests;
    ResultsByPermille m_slower_requests;
};


class DiffRouterSessionStats
{
public:
    DiffRouterSessionStats(mxs::Target* pMain,
                           const DiffMainStats& main_stats,
                           const DiffQps& main_qps);

    void add_other(mxs::Target* pOther,
                   const DiffOtherStats& other_stats,
                   const DiffQps& other_qps);

    mxs::Target* main() const
    {
        return m_pMain;
    }

    const DiffMainStats& main_stats() const
    {
        return m_main_stats;
    }

    const DiffQps& main_qps() const
    {
        return m_main_qps;
    }

    class Other
    {
    public:
        Other(const DiffOtherStats& s, const DiffQps& q)
            : stats(s)
            , qps(q)
        {
        }

        const DiffOtherStats& stats;
        const DiffQps& qps;
    };

    using Others = std::map<mxs::Target*, Other>;

    const Others& others() const
    {
        return m_others;
    }

private:
    mxs::Target*         m_pMain { nullptr };
    const DiffMainStats& m_main_stats;
    const DiffQps&       m_main_qps;
    Others               m_others;
};


class DiffRouterStats
{
public:
    DiffRouterStats(std::chrono::seconds qps_window)
        : m_main_qps(qps_window)
    {
    }

    void add(const DiffRouterSessionStats& rss, const DiffConfig& config)
    {
        m_main_stats.add(rss.main_stats());
        m_main_qps += rss.main_qps();

        for (const auto& kv : rss.others())
        {
            auto it = m_others.find(kv.first);

            if (it != m_others.end())
            {
                it->second.stats.add(kv.second.stats, config);
                it->second.qps += kv.second.qps;
            }
            else
            {
                m_others.insert(std::make_pair(kv.first, Other(kv.second.stats, kv.second.qps)));
            }
        }
    }

    void post_configure(const DiffConfig& config);

    std::map<mxs::Target*, json_t*> get_jsons() const;

private:
    class Other
    {
    public:
        Other(const DiffOtherStats& s, const DiffQps& q)
            : stats(s)
            , qps(q)
        {
        }

        DiffOtherStats stats;
        DiffQps        qps;
    };

    using Others = std::map<mxs::Target*, Other>;

    mxs::Target*   m_pMain { nullptr };
    DiffMainStats  m_main_stats;
    DiffQps        m_main_qps;
    Others         m_others;
};
