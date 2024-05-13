/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffstats.hh"
#include <cmath>
#include <maxscale/service.hh>
#include "diffconfig.hh"
#include "diffresult.hh"
#include "diffroutersession.hh"

using std::chrono::duration_cast;

namespace
{

void add_histogram(json_t* pDuration, const DiffHistogram& hist)
{
    json_t* pHist_bin_counts = json_array();
    json_t* pHist_bin_edges = json_array();

    const auto& bins = hist.bins();

    const auto& sos = hist.smaller_outliers();

    std::chrono::duration<double> edge = sos.left;
    json_array_append_new(pHist_bin_edges, json_real(edge.count()));

    edge = sos.right;
    json_array_append_new(pHist_bin_edges, json_real(edge.count()));
    json_array_append_new(pHist_bin_counts, json_integer(sos.count));

    for (auto it = bins.begin(); it != bins.end(); ++it)
    {
        const auto& bin = *it;
        edge = bin.right;

        json_array_append_new(pHist_bin_counts, json_integer(bin.count));
        json_array_append_new(pHist_bin_edges, json_real(edge.count()));
    }

    const auto& los = hist.larger_outliers();
    edge = los.right;
    json_array_append_new(pHist_bin_edges, json_real(edge.count()));
    json_array_append_new(pHist_bin_counts, json_integer(los.count));

    json_object_set_new(pDuration, "hist_bin_counts", pHist_bin_counts);
    json_object_set_new(pDuration, "hist_bin_edges", pHist_bin_edges);
}

json_t* create_query(int id, const std::string& sql, const DiffData& data)
{
    json_t* pQuery = json_object();

    json_object_set_new(pQuery, "id", json_integer(id));
    json_object_set_new(pQuery, "sql", json_string(sql.c_str()));
    json_object_set_new(pQuery, "errors", json_integer(data.errors()));

    json_t* pResult_rows = json_object();
    json_object_set_new(pResult_rows, "sum", json_integer(data.rr_sum()));
    json_object_set_new(pResult_rows, "min", json_integer(data.rr_min()));
    json_object_set_new(pResult_rows, "max", json_integer(data.rr_max()));
    json_object_set_new(pResult_rows, "mean", json_integer(data.rr_mean()));
    json_object_set_new(pResult_rows, "count", json_integer(data.rr_count()));
    json_object_set_new(pResult_rows, "stddev", json_real(0));

    json_object_set_new(pQuery, "result_rows", pResult_rows);

    json_t* pRows_read = json_object();
    json_object_set_new(pRows_read, "sum", json_integer(0));
    json_object_set_new(pRows_read, "min", json_integer(0));
    json_object_set_new(pRows_read, "max", json_integer(0));
    json_object_set_new(pRows_read, "mean", json_real(0));
    json_object_set_new(pRows_read, "count", json_integer(0));
    json_object_set_new(pRows_read, "stddev", json_real(0));

    json_object_set_new(pQuery, "rows_read", pRows_read);

    const auto& hist = data.histogram();

    json_t* pDuration = json_object();
    json_object_set_new(pDuration, "sum", json_real(mxb::to_secs(hist.sum())));
    json_object_set_new(pDuration, "min", json_real(mxb::to_secs(hist.min())));
    json_object_set_new(pDuration, "max", json_real(mxb::to_secs(hist.max())));
    json_object_set_new(pDuration, "mean", json_real(mxb::to_secs(hist.mean())));
    json_object_set_new(pDuration, "count", json_real(hist.count()));
    json_object_set_new(pDuration, "stddev", json_real(0));

    add_histogram(pDuration, hist);

    json_object_set_new(pQuery, "duration", pDuration);

    json_t* pExplains = json_array();
    for (const auto& kv : data.explains())
    {
        json_t* pExplain = json_object();
        const auto& sExplain = kv.second;
        std::string when = mxb::to_string(sExplain->when(), "%Y-%m-%dT%H:%M:%S");
        json_object_set_new(pExplain, "when", json_string(when.c_str()));
        json_object_set_new(pExplain, "sql", json_string(sExplain->sql().c_str()));
        json_object_set_new(pExplain, "duration", json_real(mxb::to_secs(kv.first)));
        json_object_set(pExplain, "json", kv.second->json());

        json_array_append_new(pExplains, pExplain);
    }

    json_object_set_new(pQuery, "explain", pExplains);

    return pQuery;
}

}

/**
 * DiffStats
 */
void DiffStats::add_canonical_result(DiffRouterSession& router_session,
                                     std::string_view canonical,
                                     const std::chrono::nanoseconds& duration,
                                     const mxs::Reply& reply)
{
    m_total_duration += duration;

    auto it = m_datas.find(canonical);

    if (it != m_datas.end())
    {
        it->second.add(duration, reply);
    }
    else
    {
        DiffHistogram::Specification spec = get_specification(router_session, canonical, duration);

        if (!spec.empty())
        {
            // This particular canonical statement has been sampled enough and the bins
            // are now available so the histogram of that canonical statement can now
            // be created.

            auto p = m_datas.emplace(std::string(canonical), DiffData(spec));
            it = p.first;
            it->second.add(duration, reply);
        }
    }
}

void DiffStats::add_explain_result(std::string_view canonical,
                                   const std::chrono::nanoseconds& duration,
                                   const mxb::TimePoint& now,
                                   std::string_view sql,
                                   json_t* pExplain)
{
    // This function should not be called before enough samples have been collected
    // and at that point, there should be an entry in m_data.
    auto it = m_datas.find(canonical);

    if (it != m_datas.end())
    {
        it->second.add_explain(duration, now, sql, pExplain);
    }
    else
    {
        MXB_WARNING("Ignoring EXPLAIN result, no statistics entry found for "
                    "canonical statement: %.*s", (int)sql.length(), sql.data());
    }
}

json_t* DiffStats::get_statistics() const
{
    json_t* pStatistics = json_object();

    std::chrono::duration<double> dur;

    dur = m_total_duration;
    json_object_set_new(pStatistics, "duration", json_real(dur.count()));
    json_object_set_new(pStatistics, "request_packets", json_integer(m_nRequest_packets));
    json_object_set_new(pStatistics, "requests", json_integer(m_nRequests));
    json_object_set_new(pStatistics, "requests_explainable", json_integer(m_nRequests_explainable));
    json_object_set_new(pStatistics, "requests_responding", json_integer(m_nRequests_responding));
    json_object_set_new(pStatistics, "responses", json_integer(m_nResponses));

    json_t* pExplain = json_object();
    dur = m_explain_duration;
    json_object_set_new(pExplain, "duration", json_real(dur.count()));
    json_object_set_new(pExplain, "requests", json_integer(m_nExplain_requests));
    json_object_set_new(pExplain, "responses", json_integer(m_nExplain_responses));

    json_object_set_new(pStatistics, "explain", pExplain);

    return pStatistics;
}

json_t* DiffStats::to_json() const
{
    json_t* pData = json_object();
    json_t* pQueries = json_array();

    int id = 1;

    for (const auto& kv : m_datas)
    {
        auto& sql = kv.first;
        auto& data = kv.second;

        json_array_append_new(pQueries, create_query(id++, sql, data));
    }

    json_object_set_new(pData, "queries", pQueries);
    json_object_set_new(pData, "statistics", get_statistics());

    return pData;
}

/**
 * DiffMainStats
 */
DiffHistogram::Specification DiffMainStats::get_specification(DiffRouterSession& router_session,
                                                              std::string_view canonical,
                                                              const std::chrono::nanoseconds& duration)
{
    return router_session.get_specification_for(canonical, duration);
}

/**
 * DiffOtherStats
 */
void DiffOtherStats::add_result(const DiffOrdinaryOtherResult& other_result, const DiffConfig& config)
{
    std::chrono::nanoseconds other_duration = other_result.duration();
    std::chrono::nanoseconds main_duration = other_result.main_result().duration();

    mxb_assert(main_duration != 0s);

    // Better safe than sorry.
    int64_t permille = main_duration != 0s ? ((other_duration - main_duration) * 1000) / main_duration : 0;

    if (permille > 0)
    {
        ++m_nSlower;

        // Slower
        if (m_slower_requests.size() < (size_t)config.retain_slower_statements)
        {
            m_slower_requests.emplace(permille, other_result.shared_from_this());
        }
        else
        {
            auto it = m_slower_requests.begin();

            if (permille >= it->first)
            {
                m_slower_requests.emplace(permille, other_result.shared_from_this());

                m_slower_requests.erase(m_slower_requests.begin());
            }
        }
    }
    else if (permille < 0)
    {
        ++m_nFaster;

        permille = -permille;

        // Faster
        if (m_faster_requests.size() < (size_t)config.retain_faster_statements)
        {
            m_faster_requests.insert({permille, other_result.shared_from_this()});
        }
        else
        {
            auto it = m_faster_requests.begin();

            if (permille >= it->first)
            {
                m_faster_requests.insert({permille, other_result.shared_from_this()});

                m_faster_requests.erase(m_faster_requests.begin());
            }
        }
    }
}

void DiffOtherStats::combine(const DiffOtherStats& rhs, const DiffConfig& config)
{
    DiffStats::combine_stats(rhs, config);

    m_nRequests_skipped += rhs.m_nRequests_skipped;

    m_nFaster += rhs.m_nFaster;
    m_nSlower += rhs.m_nSlower;

    for (auto& kv : rhs.m_faster_requests)
    {
        m_faster_requests.insert(kv);
    }

    for (auto& kv : rhs.m_slower_requests)
    {
        m_slower_requests.insert(kv);
    }

    // The entries are in increasing permille order. Thus, by removing from
    // the beginning we retain the faster/slower ones.

    if (m_faster_requests.size() > (size_t)config.retain_faster_statements)
    {
        auto b = m_faster_requests.begin();
        auto e = b;
        std::advance(e, m_faster_requests.size() - config.retain_faster_statements);

        m_faster_requests.erase(b, e);
    }

    if (m_slower_requests.size() > (size_t)config.retain_slower_statements)
    {
        auto b = m_slower_requests.begin();
        auto e = b;
        std::advance(e, m_slower_requests.size() - config.retain_slower_statements);

        m_slower_requests.erase(b, e);
    }
}

json_t* DiffOtherStats::to_json() const
{
    json_t* pJson = DiffStats::to_json();

    json_object_set_new(pJson, "verdict", get_verdict());

    return pJson;
}

DiffHistogram::Specification DiffOtherStats::get_specification(DiffRouterSession& router_session,
                                                               std::string_view canonical,
                                                               const std::chrono::nanoseconds& duration)
{
    // Result from 'other' is not counted as a sample, so 'duration' is not used.
    return router_session.get_specification_for(canonical);
}

json_t* DiffOtherStats::get_statistics() const
{
    json_t* pStatistics = DiffStats::get_statistics();

    json_object_set_new(pStatistics, "requests_skipped", json_integer(m_nRequests_skipped));

    return pStatistics;
}

json_t* DiffOtherStats::get_verdict() const
{
    json_t* pVerdict = json_object();
    json_object_set_new(pVerdict, "faster", json_integer(m_nFaster));
    json_object_set_new(pVerdict, "slower", json_integer(m_nSlower));

    auto create_result_array = [](const auto& result) {
        json_t* pResult = json_array();
        for (auto it = result.begin(); it != result.end(); ++it)
        {
            auto& kv = *it;
            auto percent = std::round((double)kv.first / 10);
            auto& sOther_result = kv.second;

            json_t* pEntry = json_object();
            json_object_set_new(pEntry, "duration",
                                json_integer(sOther_result->duration().count()));
            json_object_set_new(pEntry, "duration_main",
                                json_integer(sOther_result->main_result().duration().count()));
            json_object_set_new(pEntry, "percent", json_integer(percent));
            std::string_view sql = sOther_result->sql();
            json_object_set_new(pEntry, "sql", json_stringn(sql.data(), sql.length()));
            json_object_set_new(pEntry, "id", json_integer(sOther_result->id()));

            json_t* pExplainers = json_array();
            const DiffRegistry::Entries& explainers = sOther_result->explainers();

            for (const auto& explainer : explainers)
            {
                json_array_append_new(pExplainers, json_integer(explainer.id));
            }

            json_object_set_new(pEntry, "explained_by", pExplainers);

            json_array_append_new(pResult, pEntry);
        }
        return pResult;
    };

    json_object_set_new(pVerdict, "fastest", create_result_array(m_faster_requests));
    json_object_set_new(pVerdict, "slowest", create_result_array(m_slower_requests));

    return pVerdict;
}

/**
 * DiffRouterSessionStats
 */
DiffRouterSessionStats::DiffRouterSessionStats(mxs::Target* pMain,
                                               const DiffMainStats& main_stats,
                                               const DiffQps& main_qps)
    : m_pMain(pMain)
    , m_main_stats(main_stats)
    , m_main_qps(main_qps)
{
}

void DiffRouterSessionStats::add_other(mxs::Target* pOther,
                                       const DiffOtherStats& other_stats,
                                       const DiffQps& other_qps)
{
    mxb_assert(m_others.find(pOther) == m_others.end());

    m_others.insert(std::make_pair(pOther, Other { other_stats, other_qps }));
}

/**
 * DiffRouterStats
 */
void DiffRouterStats::post_configure(const DiffConfig& config)
{
    mxb_assert(!m_pMain || m_pMain == config.pMain);

    m_pMain = config.pMain;
}

namespace
{

json_t* add_qps(json_t* pData, const DiffQps& qps)
{
    json_t* pQps = json_object();

    json_t* pTime = json_array();
    json_t* pCounts = json_array();

    int second = qps.start_time() - 1;
    json_array_append_new(pTime, json_integer(second));

    for (auto count : qps.values())
    {
        json_array_append_new(pTime, json_integer(++second));
        json_array_append_new(pCounts, json_integer(count));
    }

    json_object_set_new(pQps, "time", pTime);
    json_object_set_new(pQps, "counts", pCounts);

    json_object_set_new(pData, "qps", pQps);

    return pData;
}

}

std::map<mxs::Target*, json_t*> DiffRouterStats::get_jsons() const
{
    std::map<mxs::Target*, json_t*> rv;

    rv.emplace(m_pMain, add_qps(m_main_stats.to_json(), m_main_qps));

    for (const auto& kv : m_others)
    {
        rv.emplace(kv.first, add_qps(kv.second.stats.to_json(), kv.second.qps));
    }

    return rv;
}
