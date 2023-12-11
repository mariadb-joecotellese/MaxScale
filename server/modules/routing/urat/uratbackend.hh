/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"
#include <deque>
#include <vector>
#include <memory>
#include <maxscale/backend.hh>
#include <maxscale/router.hh>
#include <maxbase/checksum.hh>
#include "uratresult.hh"

class UratMainBackend;
class UratOtherBackend;
using SUratMainBackend = std::unique_ptr<UratMainBackend>;
using SUratOtherBackend = std::unique_ptr<UratOtherBackend>;
using SUratOtherBackends = std::vector<SUratOtherBackend>;

class UratResult;

class UratBackend : public mxs::Backend
{
public:
    static std::pair<SUratMainBackend, SUratOtherBackends>
    from_endpoints(const mxs::Target& main_target, const mxs::Endpoints& endpoints);

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    void process_result(const GWBUF& buffer);
    UratResult finish_result(const mxs::Reply& reply);

    int32_t nBacklog() const
    {
        return m_results.size();
    }

protected:
    using mxs::Backend::Backend;

private:
    std::deque<UratResult> m_results;
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
