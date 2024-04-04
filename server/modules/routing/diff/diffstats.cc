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

using std::chrono::duration_cast;

namespace
{

void add_histogram(json_t* pDuration, const mxs::ResponseDistribution& rd)
{
    json_t* pHist_counts = json_array();
    json_t* pHist_bins = json_array();

    for (const auto& element : rd.get())
    {
        auto count = element.count;
        std::chrono::duration<double> bin = element.limit;

        json_array_append_new(pHist_counts, json_integer(count));
        json_array_append_new(pHist_bins, json_real(bin.count()));
    }

    // TODO: One extra for the visualization.
    json_array_append_new(pHist_bins, json_real(100000.1));

    json_object_set_new(pDuration, "hist_counts", pHist_counts);
    json_object_set_new(pDuration, "hist_bins", pHist_bins);
}

json_t* create_query(int id, const std::string& sql, const mxs::ResponseDistribution& rd)
{
    json_t* pQuery = json_object();

    json_object_set_new(pQuery, "id", json_integer(id));
    json_object_set_new(pQuery, "sql", json_string(sql.c_str()));
    json_object_set_new(pQuery, "errors", json_integer(0)); // TODO

    json_t* pRows_read = json_object();
    json_t* pZero_integer = json_integer(0);
    json_object_set(pRows_read, "sum", pZero_integer);
    json_object_set(pRows_read, "min", pZero_integer);
    json_object_set(pRows_read, "max", pZero_integer);
    json_object_set_new(pRows_read, "count", pZero_integer);
    json_t* pZero_real = json_real(0.0);
    json_object_set(pRows_read, "mean", pZero_real);
    json_object_set_new(pRows_read, "stddev", pZero_real);

    json_object_set_new(pQuery, "rows_read", pRows_read);

    json_t* pDuration = json_object();

    json_object_set_new(pDuration, "sum", json_real(0));
    json_object_set_new(pDuration, "min", json_real(0));
    json_object_set_new(pDuration, "max", json_real(0));
    json_object_set_new(pDuration, "mean", json_real(0));
    json_object_set_new(pDuration, "count", json_real(0));
    json_object_set_new(pDuration, "stddev", json_real(0));

    add_histogram(pDuration, rd);

    json_object_set_new(pQuery, "duration", pDuration);

    return pQuery;
}

}

/**
 * DiffStats
 */
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

json_t* DiffStats::get_data() const
{
    json_t* pData = json_object();
    json_t* pQueries = json_array();

    int id = 1;

    json_array_append_new(pQueries, create_query(id++, "TOTAL", m_response_distribution));

    for (const auto& kv : m_response_distributions)
    {
        auto& sql = kv.first;
        auto& response_distribution = kv.second;

        json_array_append_new(pQueries, create_query(id++, sql, response_distribution));
    }

    json_object_set_new(pData, "queries", pQueries);

    json_t* pQps = json_object();
    json_t* pZero = json_real(0.0);

    json_t* pTime = json_array();
    json_array_append(pTime, pZero);
    json_array_append(pTime, pZero);

    json_t* pCounts = json_array();
    json_array_append_new(pCounts, pZero);

    json_object_set_new(pQps, "time", pTime);
    json_object_set_new(pQps, "counts", pCounts);

    json_object_set_new(pData, "qps", pQps);

    return pData;
}

/**
 * DiffMainStats
 */
json_t* DiffMainStats::to_json() const
{
    json_t* pJson = json_object();

    json_object_set_new(pJson, "statistics", get_statistics());
    json_object_set_new(pJson, "data", get_data());

    return pJson;
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
            m_slower_requests.insert(std::make_pair(permille, other_result.shared_from_this()));
        }
        else
        {
            auto it = m_slower_requests.begin();

            if (permille >= it->first)
            {
                m_slower_requests.insert({permille, other_result.shared_from_this()});

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

void DiffOtherStats::add(const DiffOtherStats& rhs, const DiffConfig& config)
{
    DiffStats::add(rhs);

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
    json_t* pJson = json_object();

    json_t* pStatistics = get_statistics();
    json_object_set_new(pStatistics, "requests_skipped", json_integer(m_nRequests_skipped));

    json_object_set_new(pJson, "statistics", pStatistics);
    json_object_set_new(pJson, "data", get_data());

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

    json_object_set_new(pJson, "verdict", pVerdict);

    return pJson;
}


/**
 * DiffRouterSessionStats
 */
DiffRouterSessionStats::DiffRouterSessionStats(mxs::Target* pMain, const DiffMainStats& main_stats)
    : m_pMain(pMain)
    , m_main_stats(main_stats)
{
    for (auto& kv : main_stats.response_distributions())
    {
        m_response_distribution += kv.second;
    }
}

void DiffRouterSessionStats::add_other(mxs::Target* pOther, const DiffOtherStats& other_stats)
{
    mxb_assert(m_other_stats.find(pOther) == m_other_stats.end());

    m_other_stats.insert(std::make_pair(pOther, other_stats));

    for (auto& kv : other_stats.response_distributions())
    {
        m_response_distribution += kv.second;
    }
}

json_t* DiffRouterSessionStats::to_json() const
{
    json_t* pJson = json_object();

    json_t* pMain = json_object();
    const char* zKey = m_pMain ? m_pMain->name() : "unknown";
    json_object_set_new(pMain, zKey, m_main_stats.to_json());

    json_t* pOthers = json_object();
    for (const auto& kv : m_other_stats)
    {
        json_t* pOther = kv.second.to_json();

        json_object_set_new(pOthers, kv.first->name(), pOther);
    }

    json_object_set_new(pJson, "main", pMain);
    json_object_set_new(pJson, "others", pOthers);

    return pJson;
}

std::map<mxs::Target*, json_t*> DiffRouterSessionStats::get_data() const
{
    std::map<mxs::Target*, json_t*> rv;

    rv.insert(std::make_pair(m_pMain, m_main_stats.get_data()));

    for (const auto& kv : m_other_stats)
    {
        rv.insert(std::make_pair(kv.first, kv.second.get_data()));
    }

    return rv;
}

/**
 * DiffRouterStats
 */
void DiffRouterStats::post_configure(const DiffConfig& config)
{
    mxb_assert(!m_router_session_stats.main());

    m_router_session_stats.set_main(config.pMain);
}

json_t* DiffRouterStats::to_json() const
{
    json_t* pJson = json_object();

    auto nTotal = m_service.stats().n_total_conns();
    auto nCurrent = m_service.stats().n_current_conns();

    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(nTotal));
    json_object_set_new(pSessions, "current", json_integer(nCurrent));
    json_object_set_new(pJson, "sessions", pSessions);

    json_object_set_new(pJson, "summary", this->m_router_session_stats.to_json());

    return pJson;
}

