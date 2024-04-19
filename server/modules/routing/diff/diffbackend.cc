/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffbackend.hh"
#include <maxscale/protocol/mariadb/mysql.hh>
#include "diffresult.hh"
#include "diffrouter.hh"
#include "diffroutersession.hh"


/**
 * DiffBackend
 */
DiffBackend::DiffBackend(mxs::Endpoint* pEndpoint, const SDiffQps& sQps)
    : mxs::Backend(pEndpoint)
    , m_sQps(sQps)
{
}

void DiffBackend::set_router_session(DiffRouterSession* pRouter_session)
{
    mxb_assert(!m_pRouter_session);
    m_pRouter_session = pRouter_session;

    auto& parser = pRouter_session->parser();

    m_pRouter_session = pRouter_session;
    m_sQc = std::make_unique<mariadb::QueryClassifier>(parser, &pRouter_session->session());
    m_pParser = &parser;
    m_pParser_helper = &m_pParser->helper();
}

bool DiffBackend::extraordinary_in_process() const
{
    mxb_assert(m_sQc);
    const auto& ri = m_sQc->current_route_info();

    return ri.load_data_active() || ri.multi_part_packet();
}

void DiffBackend::process_result(const GWBUF& buffer, const mxs::Reply& reply)
{
    mxb_assert(m_sQc);
    m_sQc->update_from_reply(reply);

    auto* pFront = front();
    mxb_assert(pFront);
    pFront->process(buffer);
}

void DiffBackend::close(close_type type)
{
    mxs::Backend::close(type);
}

void DiffBackend::lcall(std::function<bool()>&& fn)
{
    mxb_assert(m_pRouter_session);

    m_pRouter_session->lcall(std::move(fn));
}

/**
 * DiffMainBackend
 */
DiffMainBackend::DiffMainBackend(mxs::Endpoint* pEndpoint, const SDiffQps& sQps)
    : Base(pEndpoint, sQps)
{
}

std::shared_ptr<DiffOrdinaryMainResult> DiffMainBackend::prepare(const GWBUF& packet)
{
    auto sMain_result = std::make_shared<DiffOrdinaryMainResult>(this, packet);

    m_results.push_back(sMain_result);

    return sMain_result;
}

void DiffMainBackend::ready(const DiffExplainMainResult& explain_result)
{
    m_stats.inc_explain_responses();

    // Tune counters as the extra EXPLAIN requests/responses should be
    // excluded from the general book-keeping.
    m_stats.dec_responses();

    m_stats.add_explain_duration(explain_result.duration());

    execute_pending_explains();
}


/**
 * DiffOtherBackend
 */
DiffOtherBackend::DiffOtherBackend(mxs::Endpoint* pEndpoint,
                                   const SDiffQps& sQps,
                                   const DiffConfig* pConfig,
                                   std::shared_ptr<DiffExporter> sExporter)
    : Base(pEndpoint, sQps)
    , m_config(*pConfig)
    , m_sExporter(std::move(sExporter))
{
}

DiffOtherBackend::~DiffOtherBackend()
{
    int nStill_registered = 0;

    for (auto& sResult : m_results)
    {
        if (sResult->registered_at_main())
        {
            sResult->deregister_from_main();
            ++nStill_registered;
        }
    }

    if (nStill_registered != 0)
    {
        MXB_WARNING("Att session close, there was %d 'other' result(s) that "
                    "still waited for the 'main' result.", nStill_registered);
    }

    nStill_registered = 0;

    for (auto& sExplain_result : m_pending_explains)
    {
        DiffExplainOtherResult* pExplain_result = static_cast<DiffExplainOtherResult*>(sExplain_result.get());

        if (pExplain_result->registered_at_main())
        {
            pExplain_result->deregister_from_main();
            ++nStill_registered;
        }
    }

    if (nStill_registered != 0)
    {
        MXB_WARNING("Att session close, there was %d 'other' EXPLAIN result(s) that "
                    "still waited for the 'main' EXPLAIN result.", nStill_registered);
    }

}

void DiffOtherBackend::prepare(const std::shared_ptr<DiffOrdinaryMainResult>& sMain_result)
{
    // std::make_shared can't be used, because the private COtherResult::Handler base is inaccessible.
    auto* pOther_result = new DiffOrdinaryOtherResult(this, this, sMain_result);
    auto sOther_result = std::shared_ptr<DiffOrdinaryOtherResult>(pOther_result);

    sOther_result->register_at_main();

    m_results.emplace_back(std::move(sOther_result));
}

void DiffOtherBackend::ready(DiffOrdinaryOtherResult& other_result)
{
    mxb_assert(m_pHandler);

    auto& main_result = other_result.main_result();

    m_stats.add_result(other_result, m_config);

    std::shared_ptr<DiffExplainMainResult> sExplain_main;

    switch (m_pHandler->ready(other_result))
    {
    case Explain::NONE:
        break;

    case Explain::BOTH:
        {
            mxb_assert(main_result.is_explainable());

            auto& main_backend = static_cast<DiffMainBackend&>(main_result.backend());
            auto sMain_result = main_result.shared_from_this();
            auto* pExplain_main = new DiffExplainMainResult(&main_backend, sMain_result);
            sExplain_main.reset(pExplain_main);

            main_backend.schedule_explain(std::move(sExplain_main));
            main_backend.execute_pending_explains();
        }
        [[fallthrough]];

    case Explain::OTHER:
        {
            mxb_assert(other_result.is_explainable());

            auto sOther_result = other_result.shared_from_this();

            auto* pExplain_result = new DiffExplainOtherResult(this, sOther_result, sExplain_main);
            auto sExplain_result = std::shared_ptr<DiffExplainOtherResult>(pExplain_result);

            sExplain_result->register_at_main();

            schedule_explain(std::move(sExplain_result));
        }
        break;
    }

    execute_pending_explains();
}

void DiffOtherBackend::ready(DiffExplainOtherResult& explain_result)
{
    mxb_assert(m_pHandler);

    m_stats.inc_explain_responses();

    // Tune counters as the extra EXPLAIN requests/responses should be
    // excluded from the general book-keeping.
    m_stats.dec_responses();

    m_stats.add_explain_duration(explain_result.duration());

    m_pHandler->ready(explain_result);

    execute_pending_explains();
}


/**
 * namespace diff
 */
namespace diff
{

std::pair<SDiffMainBackend,SDiffOtherBackends>
backends_from_endpoints(const mxs::Target& main_target,
                        const mxs::Endpoints& endpoints,
                        DiffRouter& router)
{
    mxb_assert(endpoints.size() > 1);

    std::vector<const mxs::Target*> targets;

    targets.push_back(&main_target);

    for (auto* pEndpoint : endpoints)
    {
        if (pEndpoint->target() != &main_target)
        {
            targets.push_back(pEndpoint->target());
        }
    }

    std::vector<SDiffQps> qpses = router.get_qpses_for(targets);
    mxb_assert(qpses.size() == targets.size());

    auto it = qpses.begin();

    SDiffMainBackend sMain;

    for (auto* pEndpoint : endpoints)
    {
        if (pEndpoint->target() == &main_target)
        {
            sMain.reset(new DiffMainBackend(pEndpoint, *it++));
            break;
        }
    }

    SDiffOtherBackends others;
    others.reserve(endpoints.size() - 1);

    for (auto* pEndpoint : endpoints)
    {
        auto* pTarget = pEndpoint->target();

        if (pTarget != &main_target)
        {
            auto sExporter = router.exporter_for(pTarget);
            others.emplace_back(new DiffOtherBackend(pEndpoint, *it++, &router.config(), sExporter));
        }
    }

    return std::make_pair(std::move(sMain), std::move(others));
}

}
