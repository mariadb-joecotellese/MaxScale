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

class UratMainBackend;
class UratOtherBackend;
using SUratMainBackend = std::unique_ptr<UratMainBackend>;
using SUratOtherBackend = std::unique_ptr<UratOtherBackend>;
using SUratOtherBackends = std::vector<SUratOtherBackend>;

class UratResult;

using Clock = std::chrono::steady_clock;

class UratBackend : public mxs::Backend
{
public:
    static std::pair<SUratMainBackend, SUratOtherBackends>
    from_endpoints(const mxs::Target& main_target, const mxs::Endpoints& endpoints);

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    void process_result(const GWBUF& buffer, const mxs::Reply& reply);
    UratResult finish_result(const GWBUF& buffer, const mxs::Reply& reply);

    const mxs::Reply& reply() const
    {
        return m_reply;
    }

protected:
    using mxs::Backend::Backend;

private:
    Clock::time_point m_start;
    Clock::time_point m_end;
    mxb::CRC32        m_checksum;
    mxs::Reply        m_reply;
};

class UratMainBackend : public UratBackend
{
public:
    using UratBackend::UratBackend;
};

class UratOtherBackend : public UratBackend
{
public:
    UratOtherBackend(mxs::Endpoint* pEndpoint, UratMainBackend* pMain)
        : UratBackend(pEndpoint)
        , m_main(*pMain)
    {
    }

private:
    UratMainBackend& m_main;
};
