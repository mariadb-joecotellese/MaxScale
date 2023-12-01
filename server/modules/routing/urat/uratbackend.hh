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
using SUratBackend = std::unique_ptr<UratBackend>;
using SUratBackends = std::vector<SUratBackend>;

class UratResult;

using Clock = std::chrono::steady_clock;

class UratBackend : public mxs::Backend
{
public:
    using mxs::Backend::Backend;

    static std::pair<SUratBackend, SUratBackends>
    from_endpoints(const mxs::Target& main_target, const mxs::Endpoints& endpoints);

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    void process_result(const GWBUF& buffer, const mxs::Reply& reply);
    UratResult finish_result(const GWBUF& buffer, const mxs::Reply& reply);

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
