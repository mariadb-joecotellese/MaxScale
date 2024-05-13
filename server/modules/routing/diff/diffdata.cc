/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "diffdata.hh"
#include "diffconfig.hh"

namespace
{

const size_t MAX_SIZE = 1024 * 1024;

std::string copy_sql(std::string_view sql)
{
    return sql.size() <= MAX_SIZE ? std::string(sql) : std::string(sql.data(), MAX_SIZE);
}

}

DiffData::Explain::Explain(const mxb::TimePoint& when,
                           std::string_view sql,
                           json_t* pExplain)
    : m_when(when)
    , m_sql(copy_sql(sql))
    , m_pExplain(pExplain)
{
}

DiffData::Explain::~Explain()
{
    if (m_pExplain)
    {
        json_decref(m_pExplain);
    }
}


DiffData::DiffData(const DiffHistogram::Specification& specification)
    : m_histogram(specification)
{
}

void DiffData::add(const mxb::Duration& duration, const mxs::Reply& reply)
{
    if (reply.error())
    {
        ++m_errors;
    }
    else if (reply.is_resultset())
    {
        ++m_rr_count;

        int64_t rr = reply.rows_read();

        if (rr < m_rr_min)
        {
            m_rr_min = rr;
        }

        if (rr > m_rr_max)
        {
            m_rr_max = rr;
        }

        m_rr_sum += rr;
    }

    m_histogram.add(duration);
}

void DiffData::add_explain(const mxb::Duration& duration,
                           const mxb::TimePoint& when,
                           std::string_view sql,
                           json_t* pExplain)
{
    m_explains.emplace(duration, std::make_shared<Explain>(when, sql, pExplain));
}

void DiffData::combine(const DiffData& rhs, const DiffConfig& config)
{
    m_errors += rhs.m_errors;
    m_rr_count += rhs.m_rr_count;

    if (m_rr_max < rhs.m_rr_max)
    {
        m_rr_max = rhs.m_rr_max;
    }

    if (m_rr_min > rhs.m_rr_min)
    {
        m_rr_min = rhs.m_rr_min;
    }

    m_rr_sum += rhs.m_rr_sum;

    m_histogram += rhs.m_histogram;

    for (const auto& kv : rhs.m_explains)
    {
        m_explains.emplace(kv);
    }

    size_t explain_entries = config.explain_entries;
    if (m_explains.size() > explain_entries)
    {
        auto b = m_explains.begin();
        auto e = b;
        std::advance(e, m_explains.size() - explain_entries);
        m_explains.erase(b, e);
    }
}
