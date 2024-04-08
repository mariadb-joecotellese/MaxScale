/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <set>
#include <vector>

class DiffHistogram
{
public:
    enum OutlierApproach
    {
        IGNORE,
        CLAMP
    };

    struct Element
    {
        Element()
        {
        }

        Element(mxb::Duration l)
            : limit(l)
        {
        }

        mxb::Duration limit {0};
        int64_t       count {0};
        mxb::Duration total {0};
    };

    DiffHistogram(const std::vector<mxb::Duration>& bins,
                  OutlierApproach outlier_approach)
        : m_elements(bins.begin(), bins.end())
        , m_outlier_approach(outlier_approach)
    {
        mxb_assert(bins.size() >= 2);
    }

    DiffHistogram(const DiffHistogram& other) = default;

    int64_t smaller_outliers() const
    {
        return m_nSmaller_outliers;
    }

    int64_t larger_outliers() const
    {
        return m_nLarger_outliers;
    }

    void add(mxb::Duration dur);

    const std::vector<Element>& elements() const
    {
        return m_elements;
    }

    DiffHistogram& operator += (const DiffHistogram& rhs);

private:
    std::vector<Element> m_elements;
    OutlierApproach      m_outlier_approach;
    int64_t              m_nSmaller_outliers { 0 };
    int64_t              m_nLarger_outliers { 0 };
};
