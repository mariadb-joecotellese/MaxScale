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
    bool multi_part = ph().is_multi_part_packet(buffer);

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

ComparatorMainBackend::SResult ComparatorMainBackend::prepare(const GWBUF& packet)
{
    std::string_view sql = ph().get_sql(packet);
    uint8_t command = ph().get_command(packet);

    auto sMain_result = std::make_shared<ComparatorMainResult>(this, sql, command);

    m_results.push_back(sMain_result);

    return sMain_result;
}

/**
 * ComparatorOtherBackend
 */

void ComparatorOtherBackend::prepare(const ComparatorMainBackend::SResult& sMain_result)
{
    // std::make_shared can't be used, because the private ComparatorOtherResult::Handler base is inaccessible.
    auto* pOther_result = new ComparatorOtherResult(this, this, sMain_result);
    auto sOther_result = std::shared_ptr<ComparatorOtherResult>(pOther_result);

    m_results.emplace_back(std::move(sOther_result));
}

void ComparatorOtherBackend::ready(const ComparatorOtherResult& other_result)
{
    mxb_assert(m_pHandler);

    Action action = m_pHandler->ready(other_result);

    if (action == EXPLAIN)
    {
        if (other_result.main_result().is_explainable())
        {
            auto sOther = other_result.shared_from_this();

            auto* pExplain_result = new ComparatorExplainResult(this, sOther);
            auto sExplain_result = std::shared_ptr<ComparatorExplainResult>(pExplain_result);

            mxb_assert(!m_multi_part_in_process); // TODO: Deal with this.

            m_results.emplace_back(std::move(sExplain_result));

            std::string sql { "EXPLAIN FORMAT=JSON "};
            sql += other_result.main_result().sql();

            GWBUF packet = ph().create_packet(sql);
            packet.set_type(GWBUF::TYPE_COLLECT_ROWS);

            write(std::move(packet), mxs::Backend::EXPECT_RESPONSE);
        }
    }
}

void ComparatorOtherBackend::ready(const ComparatorExplainResult& explain_result,
                                   const std::string& error,
                                   std::string_view json)
{
    mxb_assert(m_pHandler);

    m_pHandler->ready(explain_result, error, json);
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
