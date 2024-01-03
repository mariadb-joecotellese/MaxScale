/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorbackend.hh"
#include <maxscale/protocol/mariadb/mysql.hh>
#include "comparatorresult.hh"


/**
 * ComparatorBackend
 */
bool ComparatorBackend::write(GWBUF&& buffer, response_type type)
{
    mxb_assert(m_pParser_helper);

    bool multi_part = m_pParser_helper->is_multi_part_packet(buffer);

    bool rv = Backend::write(std::move(buffer), type);

    if (rv)
    {
        m_multi_part_in_process = multi_part;
    }

    return rv;
}


/**
 * ComparatorMainBackend
 */

ComparatorMainBackend::SResult ComparatorMainBackend::prepare(std::string_view sql, uint8_t command)
{
    auto sMain_result = std::make_shared<ComparatorMainResult>(this, sql, command);

    m_results.emplace_back(std::move(sMain_result));

    return m_results.back();
}

/**
 * ComparatorOtherBackend
 */

void ComparatorOtherBackend::prepare(const ComparatorMainBackend::SResult& sMain_result)
{
    // std::make_shared can't be used, because the private ComparatorOtherResult::Handler base is inaccessible.
    auto sOther_result = std::shared_ptr<ComparatorOtherResult>(new ComparatorOtherResult(this, this, sMain_result));

    m_results.emplace_back(std::move(sOther_result));
}

void ComparatorOtherBackend::ready(const ComparatorOtherResult& other_result)
{
    Action action = CONTINUE;

    if (m_pHandler)
    {
        action = m_pHandler->ready(other_result);

        mxb_assert_message(action != EXPLAIN, "There is some explaining to do.");
    }
}

/**
 * namespace comparator
 */

namespace comparator
{

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
            others.emplace_back(new ComparatorOtherBackend(pEndpoint));
        }
    }

    return std::make_pair(std::move(sMain), std::move(others));
}

}
