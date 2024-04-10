/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffhistogram.hh"

void DiffHistogram::add(mxb::Duration dur)
{
    if (dur < m_bins.front().limit)
    {
        ++m_nSmaller_outliers;

        auto& bin = *(m_bins.begin() + 1);

        ++bin.count;
        bin.total += dur;
    }
    else if (dur >= m_bins.back().limit)
    {
        ++m_nLarger_outliers;

        auto& bin = *(m_bins.end() - 2);

        ++bin.count;
        bin.total += dur;
    }
    else
    {
        auto begin = m_bins.begin() + 1;
        auto end = m_bins.end();
        auto it = begin;

        for (; it < end; ++it)
        {
            auto& bin = *it;

            if (dur <= bin.limit)
            {
                ++bin.count;
                bin.total += dur;
                break;
            }
        }

        mxb_assert(it != end);
    }
}

DiffHistogram& DiffHistogram::operator += (const DiffHistogram& rhs)
{
    mxb_assert(m_bins.size() == rhs.m_bins.size());

    auto it = m_bins.begin();
    auto jt = rhs.m_bins.begin();

    while (it != m_bins.end())
    {
        Bin& l = *it;
        const Bin& r = *jt;

        mxb_assert(l.limit == r.limit);
        l.count += r.count;
        l.total += r.total;

        ++it;
        ++jt;
    }

    return *this;
}
