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
    m_start = Clock::now();
    m_checksum.reset();
    return Backend::write(std::move(buffer), type);
}

void UratBackend::process_result(const GWBUF& buffer, const mxs::Reply& reply)
{
    mxb_assert(!reply.is_complete());

    m_checksum.update(buffer);
}

UratResult UratBackend::finish_result(const GWBUF& buffer, const mxs::Reply& reply)
{
    mxb_assert(reply.is_complete());

    m_reply = reply;

    m_checksum.update(buffer);
    m_checksum.finalize();
    m_end = Clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(m_end - m_start);

    return UratResult(this, m_checksum, m_reply, duration);
}
