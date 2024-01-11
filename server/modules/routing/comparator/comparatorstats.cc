/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorstats.hh"
#include <maxscale/service.hh>
#include "comparatorconfig.hh"

using std::chrono::duration_cast;

/**
 * ComparatorStats
 */
void ComparatorStats::fill_json(json_t* pJson) const
{
    auto ms = duration_cast<std::chrono::milliseconds>(this->total_duration);

    json_object_set_new(pJson, "total_duration", json_integer(ms.count()));
    json_object_set_new(pJson, "request_packets", json_integer(this->nRequest_packets));
    json_object_set_new(pJson, "requests", json_integer(this->nRequests));
    json_object_set_new(pJson, "responding_requests", json_integer(this->nResponding_requests));
    json_object_set_new(pJson, "responses", json_integer(this->nResponses));
}


/**
 * ComparatorMainStats
 */
json_t* ComparatorMainStats::to_json() const
{
    json_t* pJson = json_object();

    json_t* pData = json_object();
    fill_json(pData);
    json_object_set_new(pJson, "data", pData);

    return pJson;
}


/**
 * ComparatorOtherStats
 */
json_t* ComparatorOtherStats::to_json() const
{
    json_t* pJson = json_object();

    json_t* pData = json_object();
    fill_json(pData);

    json_t* pExplain = json_object();

    auto ms = duration_cast<std::chrono::milliseconds>(this->explain_duration);
    json_object_set_new(pExplain, "duration", json_integer(ms.count()));
    json_object_set_new(pExplain, "requests", json_integer(this->nExplain_requests));
    json_object_set_new(pExplain, "responses", json_integer(this->nExplain_responses));

    json_t* pVerdict = json_object();
    json_object_set_new(pVerdict, "faster", json_integer(this->nFaster));
    json_object_set_new(pVerdict, "slower", json_integer(this->nSlower));

    json_object_set_new(pJson, "data", pData);
    json_object_set_new(pJson, "explain", pExplain);
    json_object_set_new(pJson, "verdict", pVerdict);

    return pJson;
}


/**
 * ComparatorSessionStats
 */
json_t* ComparatorSessionStats::to_json() const
{
    json_t* pJson = json_object();

    json_t* pMain = json_object();
    const char* zKey = this->pMain ? this->pMain->name() : "unknown";
    json_object_set_new(pMain, zKey, this->main_stats.to_json());

    json_t* pOthers = json_object();
    for (const auto& kv : this->other_stats)
    {
        json_t* pOther = kv.second.to_json();

        json_object_set_new(pOthers, kv.first->name(), pOther);
    }

    json_object_set_new(pJson, "main", pMain);
    json_object_set_new(pJson, "others", pOthers);

    return pJson;
}

/**
 * ComparatorRouterStats
 */
void ComparatorRouterStats::post_configure(const ComparatorConfig& config)
{
    mxb_assert(!m_session_stats.pMain);

    m_session_stats.pMain = config.pMain;
}

json_t* ComparatorRouterStats::to_json() const
{
    json_t* pJson = json_object();

    auto nTotal = m_service.stats().n_total_conns();
    auto nCurrent = m_service.stats().n_current_conns();

    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(nTotal));
    json_object_set_new(pSessions, "current", json_integer(nCurrent));
    json_object_set_new(pJson, "sessions", pSessions);

    json_object_set_new(pJson, "summary", this->m_session_stats.to_json());

    return pJson;
}

