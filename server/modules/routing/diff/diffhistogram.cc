/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffhistogram.hh"


DiffHistogram::DiffHistogram(const Specification& specification)
{
    mxb_assert(specification.bins() >= 2);

    m_bins.reserve(specification.bins());

    for (int i = 0; i <= specification.bins(); ++i)
    {
        auto l = specification.min() + i * specification.delta();
        auto r = l + specification.delta();

        m_bins.emplace_back(l, r);
    }

    const auto& front = m_bins.front();

    m_smaller_outliers.right = front.left;
    m_smaller_outliers.left = std::max(mxb::Duration {0}, front.left - specification.delta());

    const auto& back = m_bins.back();

    m_larger_outliers.left = back.right;
    m_larger_outliers.right = back.right + specification.delta();
}

void DiffHistogram::add(mxb::Duration dur)
{
    if (dur < m_smaller_outliers.right)
    {
        ++m_smaller_outliers.count;
        m_smaller_outliers.total += dur;
    }
    else if (dur >= m_larger_outliers.left)
    {
        ++m_larger_outliers.count;
        m_larger_outliers.total += dur;
    }
    else
    {
        auto begin = m_bins.begin() + 1;
        auto end = m_bins.end();
        auto it = begin;

        for (; it < end; ++it)
        {
            auto& bin = *it;

            if (dur < bin.right)
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

        mxb_assert(l.left == r.left && l.right == r.right);
        l.count += r.count;
        l.total += r.total;

        ++it;
        ++jt;
    }

    return *this;
}
