/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorbackend.hh"
#include "comparatorresult.hh"


/**
 * ComparatorMainBackend
 */

ComparatorMainBackend::SResult ComparatorMainBackend::prepare(const std::string& sql, uint8_t command)
{
    auto sMain_result = std::make_shared<ComparatorMainResult>(this, sql, command);

    m_results.emplace_back(std::move(sMain_result));

    return m_results.back();
}

/**
 * ComparatorOtherBackend
 */

void ComparatorOtherBackend::prepare(const ComparatorMainBackend::SResult& sMain_result)
{
    auto sOther_result = std::make_shared<ComparatorOtherResult>(this, sMain_result);

    m_results.emplace_back(std::move(sOther_result));
}

void ComparatorOtherBackend::ready(const ComparatorOtherResult& other_result)
{
    if (m_pResult_handler)
    {
        m_pResult_handler->ready(other_result);
    }
}

/**
 * namespace comparator
 */

namespace comparator
{

std::pair<SComparatorMainBackend,SComparatorOtherBackends>
backends_from_endpoints(const mxs::Target& main_target, const mxs::Endpoints& endpoints)
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
        if (pEndpoint->target() != &main_target)
        {
            others.emplace_back(new ComparatorOtherBackend(pEndpoint));
        }
    }

    return std::make_pair(std::move(sMain), std::move(others));
}

}
