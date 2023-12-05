/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratbackend.hh"
#include "uratresult.hh"

SUratBackends UratBackend::from_endpoints(const mxs::Endpoints& endpoints)
{
    SUratBackends backends;
    backends.reserve(endpoints.size());

    for (auto e : endpoints)
    {
        backends.emplace_back(new UratBackend(e));
    }

    return backends;
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

    return UratResult(this, checksum(), m_reply, std::chrono::milliseconds { duration() });
}
