/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"

#include <vector>
#include <memory>

#include <maxscale/backend.hh>
#include <maxscale/router.hh>
#include <maxbase/checksum.hh>

class UratBackend;
using SUratBackends = std::vector<std::unique_ptr<UratBackend>>;

class UratResult;

using Clock = std::chrono::steady_clock;

class UratBackend : public mxs::Backend
{
public:
    using mxs::Backend::Backend;

    static SUratBackends from_endpoints(const mxs::Endpoints& endpoints);

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    void process_result(const GWBUF& buffer, const mxs::Reply& reply);
    UratResult finish_result(const GWBUF& buffer, const mxs::Reply& reply);

    const mxb::CRC32& checksum() const
    {
        return m_checksum;
    }

    // Query duration in milliseconds
    uint64_t duration() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(m_end - m_start).count();
    }

    const mxs::Reply& reply() const
    {
        return m_reply;
    }

private:
    Clock::time_point m_start;
    Clock::time_point m_end;
    mxb::CRC32        m_checksum;
    mxs::Reply        m_reply;
};
