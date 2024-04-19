/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "diffdata.hh"

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

DiffData& DiffData::operator += (const DiffData& rhs)
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

    m_rr_sum += m_rr_sum;

    m_histogram += rhs.m_histogram;

    return *this;
}
