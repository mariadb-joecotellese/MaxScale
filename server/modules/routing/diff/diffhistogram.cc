/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffhistogram.hh"

void DiffHistogram::add(mxb::Duration dur)
{
    if (dur < m_elements.front().limit)
    {
        ++m_nSmaller_outliers;

        if (m_outlier_approach == OutlierApproach::CLAMP)
        {
            auto& element = *(m_elements.begin() + 1);

            ++element.count;
            element.total += dur;
        }
    }
    else if (dur >= m_elements.back().limit)
    {
        ++m_nLarger_outliers;

        if (m_outlier_approach == OutlierApproach::CLAMP)
        {
            auto& element = *(m_elements.end() - 2);

            ++element.count;
            element.total += dur;
        }
    }
    else
    {
        auto begin = m_elements.begin() + 1;
        auto end = m_elements.end();
        auto it = begin;

        for (; it < end; ++it)
        {
            auto& element = *it;

            if (dur <= element.limit)
            {
                ++element.count;
                element.total += dur;
                break;
            }
        }

        mxb_assert(it != end);
    }
}

DiffHistogram& DiffHistogram::operator += (const DiffHistogram& rhs)
{
    mxb_assert(m_elements.size() == rhs.m_elements.size());

    auto it = m_elements.begin();
    auto jt = rhs.m_elements.begin();

    while (it != m_elements.end())
    {
        Element& l = *it;
        const Element& r = *jt;

        mxb_assert(l.limit == r.limit);
        l.count += r.count;
        l.total += r.total;

        ++it;
        ++jt;
    }

    return *this;
}
