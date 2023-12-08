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
    m_result.reset();
    return Backend::write(std::move(buffer), type);
}

void UratBackend::process_result(const GWBUF& buffer, const mxs::Reply& reply)
{
    mxb_assert(!reply.is_complete());

    m_result.update_checksum(buffer);
}

UratResult UratBackend::finish_result(const GWBUF& buffer, const mxs::Reply& reply)
{
    mxb_assert(reply.is_complete());

    m_result.update_checksum(buffer);
    m_result.close(reply);

    return m_result;
}
