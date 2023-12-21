/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratbackend.hh"
#include "uratresult.hh"

// static
std::pair<SUratMainBackend,SUratOtherBackends>
UratBackend::from_endpoints(const mxs::Target& main_target, const mxs::Endpoints& endpoints)
{
    mxb_assert(endpoints.size() > 1);

    SUratMainBackend sMain;

    for (auto* pEndpoint : endpoints)
    {
        if (pEndpoint->target() == &main_target)
        {
            sMain.reset(new UratMainBackend(pEndpoint));
            break;
        }
    }

    SUratOtherBackends others;
    others.reserve(endpoints.size() - 1);

    for (auto* pEndpoint : endpoints)
    {
        if (pEndpoint->target() != &main_target)
        {
            others.emplace_back(new UratOtherBackend(pEndpoint, sMain.get()));
        }
    }

    return std::make_pair(std::move(sMain), std::move(others));
}

bool UratBackend::write(GWBUF&& buffer, response_type type)
{
    if (type != NO_RESPONSE)
    {
        m_results.emplace_back(UratResult {});
    }

    return Backend::write(std::move(buffer), type);
}

void UratBackend::process_result(const GWBUF& buffer)
{
    mxb_assert(!m_results.empty());

    m_results.front().update_checksum(buffer);
}

UratResult UratBackend::finish_result(const mxs::Reply& reply)
{
    mxb_assert(reply.is_complete());
    mxb_assert(!m_results.empty());

    UratResult result = std::move(m_results.front());
    m_results.pop_front();

    result.close(reply);

    return result;
}
