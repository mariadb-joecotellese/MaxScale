/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "cbackend.hh"
#include <maxscale/protocol/mariadb/mysql.hh>
#include "cresult.hh"
#include "crouter.hh"


/**
 * CBackend
 */

void CBackend::execute_pending_explains()
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

void CBackend::execute(const std::shared_ptr<CExplainResult>& sExplain_result)
{
    std::string sql { "EXPLAIN FORMAT=JSON "};
    sql += sExplain_result->sql();

    m_results.emplace_back(std::move(sExplain_result));

    GWBUF packet = phelper().create_packet(sql);
    packet.set_type(static_cast<GWBUF::Type>(GWBUF::TYPE_COLLECT_RESULT | GWBUF::TYPE_COLLECT_ROWS));

    write(std::move(packet), mxs::Backend::EXPECT_RESPONSE);

    book_explain();
}

void CBackend::schedule_explain(SCExplainResult&& sExplain_result)
{
    m_pending_explains.emplace_back(std::move(sExplain_result));
}

/**
 * CMainBackend
 */

CMainBackend::SResult CMainBackend::prepare(const GWBUF& packet)
{
    auto sMain_result = std::make_shared<CMainResult>(this, packet);

    m_results.push_back(sMain_result);

    return sMain_result;
}

void CMainBackend::ready(const CExplainMainResult& explain_result)
{
    ++m_stats.nExplain_responses;

    // Tune counters as the extra EXPLAIN requests/responses should be
    // excluded from the general book-keeping.
    --m_stats.nResponses;

    m_stats.explain_duration += explain_result.duration();

    execute_pending_explains();
}

void CMainBackend::execute_pending_explains()
{
    // TODO: Replace this with MXS_SESSION::delay_routing() as that will handle
    // TODO: session lifetime issues.
    m_worker.lcall([this] {
            CBackend::execute_pending_explains();
        });
}

/**
 * COtherBackend
 */

void COtherBackend::prepare(const CMainBackend::SResult& sMain_result)
{
    // std::make_shared can't be used, because the private COtherResult::Handler base is inaccessible.
    auto* pOther_result = new COtherResult(this, this, sMain_result);
    auto sOther_result = std::shared_ptr<COtherResult>(pOther_result);

    sOther_result->register_at_main();

    m_results.emplace_back(std::move(sOther_result));
}

void COtherBackend::ready(COtherResult& other_result)
{
    mxb_assert(m_pHandler);

    auto& main_result = other_result.main_result();
    auto main_duration = main_result.duration();
    auto other_duration = other_result.duration();

    if (other_duration < main_duration)
    {
        ++m_stats.nFaster;
    }
    else if (other_duration > main_duration)
    {
        ++m_stats.nSlower;
    }

    std::shared_ptr<CExplainMainResult> sExplain_main;

    switch (m_pHandler->ready(other_result))
    {
    case Explain::NONE:
        break;

    case Explain::BOTH:
        {
            mxb_assert(main_result.is_explainable());

            auto& main_backend = static_cast<CMainBackend&>(main_result.backend());
            auto sMain_result = main_result.shared_from_this();
            auto* pExplain_main = new CExplainMainResult(&main_backend, sMain_result);
            sExplain_main.reset(pExplain_main);

            main_backend.schedule_explain(sExplain_main);
            main_backend.execute_pending_explains();
        }
        [[fallthrough]];

    case Explain::OTHER:
        {
            mxb_assert(other_result.is_explainable());

            auto sOther_result = other_result.shared_from_this();

            auto* pExplain_result = new CExplainOtherResult(this, sOther_result, sExplain_main);
            auto sExplain_result = std::shared_ptr<CExplainOtherResult>(pExplain_result);

            sExplain_result->register_at_main();

            schedule_explain(std::move(sExplain_result));
        }
        break;
    }

    execute_pending_explains();
}

void COtherBackend::ready(const CExplainOtherResult& explain_result)
{
    mxb_assert(m_pHandler);

    ++m_stats.nExplain_responses;

    // Tune counters as the extra EXPLAIN requests/responses should be
    // excluded from the general book-keeping.
    --m_stats.nResponses;

    m_stats.explain_duration += explain_result.duration();

    m_pHandler->ready(explain_result);

    execute_pending_explains();
}

/**
 * namespace comparator
 */

namespace comparator
{

std::pair<SCMainBackend,SCOtherBackends>
backends_from_endpoints(mxb::Worker* pWorker,
                        const mxs::Target& main_target,
                        const mxs::Endpoints& endpoints,
                        const CRouter& router)
{
    mxb_assert(endpoints.size() > 1);

    SCMainBackend sMain;

    for (auto* pEndpoint : endpoints)
    {
        if (pEndpoint->target() == &main_target)
        {
            sMain.reset(new CMainBackend(pEndpoint, pWorker));
            break;
        }
    }

    SCOtherBackends others;
    others.reserve(endpoints.size() - 1);

    for (auto* pEndpoint : endpoints)
    {
        auto* pTarget = pEndpoint->target();

        if (pTarget != &main_target)
        {
            others.emplace_back(new COtherBackend(pEndpoint, router.exporter_for(pTarget)));
        }
    }

    return std::make_pair(std::move(sMain), std::move(others));
}

}
