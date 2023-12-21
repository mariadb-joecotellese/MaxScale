/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <deque>
#include <vector>
#include <memory>
#include <maxscale/backend.hh>
#include <maxscale/router.hh>
#include <maxbase/checksum.hh>
#include "comparatorresult.hh"

class ComparatorMainBackend;
class ComparatorOtherBackend;
using SComparatorMainBackend = std::unique_ptr<ComparatorMainBackend>;
using SComparatorOtherBackend = std::unique_ptr<ComparatorOtherBackend>;
using SComparatorOtherBackends = std::vector<SComparatorOtherBackend>;

class ComparatorResult;

class ComparatorBackend : public mxs::Backend
{
public:
    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    void process_result(const GWBUF& buffer);
    ComparatorResult finish_result(const mxs::Reply& reply);

    int32_t nBacklog() const
    {
        return m_results.size();
    }

protected:
    using mxs::Backend::Backend;

private:
    std::deque<ComparatorResult> m_results;
};

class ComparatorMainBackend : public ComparatorBackend
{
public:
    using ComparatorBackend::ComparatorBackend;
};

class ComparatorOtherBackend : public ComparatorBackend
{
public:
    ComparatorOtherBackend(mxs::Endpoint* pEndpoint, ComparatorMainBackend* pMain)
        : ComparatorBackend(pEndpoint)
        , m_main(*pMain)
    {
    }

private:
    ComparatorMainBackend& m_main;
};

namespace comparator
{

std::pair<SComparatorMainBackend, SComparatorOtherBackends>
backends_from_endpoints(const mxs::Target& main_target, const mxs::Endpoints& endpoints);

}
