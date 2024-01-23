/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorbackend.hh"
#include <maxscale/protocol/mariadb/mysql.hh>
#include "comparatorresult.hh"
#include "comparatorrouter.hh"


/**
 * ComparatorMainBackend
 */

ComparatorMainBackend::SResult ComparatorMainBackend::prepare(const GWBUF& packet)
{
    auto sMain_result = std::make_shared<ComparatorMainResult>(this, packet);

    m_results.push_back(sMain_result);

    return sMain_result;
}

/**
 * ComparatorOtherBackend
 */

void ComparatorOtherBackend::prepare(const ComparatorMainBackend::SResult& sMain_result)
{
    // std::make_shared can't be used, because the private ComparatorOtherResult::Handler base is inaccessible.
    auto* pOther_result = new ComparatorOtherResult(this, this, sMain_result);
    auto sOther_result = std::shared_ptr<ComparatorOtherResult>(pOther_result);

    m_results.emplace_back(std::move(sOther_result));
}

void ComparatorOtherBackend::ready(const ComparatorOtherResult& other_result)
{
    mxb_assert(m_pHandler);

    auto main_duration = other_result.main_result().duration();
    auto other_duration = other_result.duration();

    if (other_duration < main_duration)
    {
        ++m_stats.nFaster;
    }
    else if (other_duration > main_duration)
    {
        ++m_stats.nSlower;
    }

    Action action = m_pHandler->ready(other_result);

    if (action == EXPLAIN)
    {
        mxb_assert(other_result.is_explainable());

        auto sOther_result = other_result.shared_from_this();

        auto* pExplain_result = new ComparatorExplainResult(this, sOther_result);
        auto sExplain_result = std::shared_ptr<ComparatorExplainResult>(pExplain_result);

        m_pending_explains.emplace_back(std::move(sExplain_result));
    }

    execute_pending_explains();
}

void ComparatorOtherBackend::ready(const ComparatorExplainResult& explain_result,
                                   const std::string& error,
                                   std::string_view json)
{
    mxb_assert(m_pHandler);

    ++m_stats.nExplain_responses;

    // Tune counters as the extra EXPLAIN requests/responses should be
    // excluded from the general book-keeping.
    --m_stats.nResponses;

    m_stats.explain_duration += explain_result.duration();

    m_pHandler->ready(explain_result, error, json);

    execute_pending_explains();
}

void ComparatorOtherBackend::execute_pending_explains()
{
    if (!extraordinary_in_process())
    {
        while (!m_pending_explains.empty())
        {
            auto sExplain_result = std::move(m_pending_explains.front());
            m_pending_explains.pop_front();

            execute(sExplain_result);
        }
    }
}

void ComparatorOtherBackend::execute(const std::shared_ptr<ComparatorExplainResult>& sExplain_result)
{
    std::string sql { "EXPLAIN FORMAT=JSON "};
    sql += sExplain_result->other_result().sql();

    m_results.emplace_back(std::move(sExplain_result));

    GWBUF packet = ph().create_packet(sql);
    packet.set_type(GWBUF::TYPE_COLLECT_ROWS);

    write(std::move(packet), mxs::Backend::EXPECT_RESPONSE);
    ++m_stats.nExplain_requests;

    // Tune general counters, since those related to the extra
    // EXPLAIN requests should be exluded.
    --m_stats.nRequest_packets;
    --m_stats.nRequests;
    --m_stats.nRequests_explainable;
    --m_stats.nRequests_responding;
}

/**
 * namespace comparator
 */

namespace comparator
{

std::pair<SComparatorMainBackend,SComparatorOtherBackends>
backends_from_endpoints(const mxs::Target& main_target,
                        const mxs::Endpoints& endpoints,
                        const ComparatorRouter& router)
{
    mxb_assert(endpoints.size() > 1);

    SComparatorMainBackend sMain;

    for (auto* pEndpoint : endpoints)
    {
        if (pEndpoint->target() == &main_target)
        {
            sMain.reset(new ComparatorMainBackend(pEndpoint));
            break;
        }
    }

    SComparatorOtherBackends others;
    others.reserve(endpoints.size() - 1);

    for (auto* pEndpoint : endpoints)
    {
        auto* pTarget = pEndpoint->target();

        if (pTarget != &main_target)
        {
            others.emplace_back(new ComparatorOtherBackend(pEndpoint, router.exporter_for(pTarget)));
        }
    }

    return std::make_pair(std::move(sMain), std::move(others));
}

}
