/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <maxscale/target.hh>
#include "diffhistogram.hh"

class DiffData
{
public:
    DiffData(const DiffHistogram::Specification& specification);
    DiffData(const DiffData& other) = default;

    int64_t errors() const
    {
        return m_errors;
    }

    int64_t rr_count() const
    {
        return m_rr_count;
    }

    int64_t rr_max() const
    {
        return m_rr_max;
    }

    int64_t rr_mean() const
    {
        return m_rr_count ? m_rr_sum / m_rr_count : 0;
    }

    int64_t rr_min() const
    {
        return m_rr_min;
    }

    int64_t rr_sum() const
    {
        return m_rr_sum;
    }

    const DiffHistogram& histogram() const
    {
        return m_histogram;
    }

    void add(const mxb::Duration& duration, const mxs::Reply& reply);

    DiffData& operator += (const DiffData& rhs);

private:
    int64_t       m_errors { 0 };
    int64_t       m_rr_count { 0 };
    int64_t       m_rr_max { 0 };
    int64_t       m_rr_min { std::numeric_limits<int64_t>::max() };
    int64_t       m_rr_sum { 0 };
    DiffHistogram m_histogram;
};
