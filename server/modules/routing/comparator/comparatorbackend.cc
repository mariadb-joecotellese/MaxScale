/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorbackend.hh"
#include "comparatorresult.hh"

bool ComparatorBackend::write(GWBUF&& buffer, response_type type)
{
    if (type != NO_RESPONSE)
    {
        m_results.emplace_back(ComparatorResult {});
    }

    return Backend::write(std::move(buffer), type);
}

void ComparatorBackend::process_result(const GWBUF& buffer)
{
    mxb_assert(!m_results.empty());

    m_results.front().update_checksum(buffer);
}

ComparatorResult ComparatorBackend::finish_result(const mxs::Reply& reply)
{
    mxb_assert(reply.is_complete());
    mxb_assert(!m_results.empty());

    ComparatorResult result = std::move(m_results.front());
    m_results.pop_front();

    result.close(reply);

    return result;
}

namespace comparator
{

// static
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
            others.emplace_back(new ComparatorOtherBackend(pEndpoint, sMain.get()));
        }
    }

    return std::make_pair(std::move(sMain), std::move(others));
}

}
