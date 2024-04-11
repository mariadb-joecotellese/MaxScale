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

    const DiffHistogram& histogram() const
    {
        return m_histogram;
    }

    void add(const mxb::Duration& duration, const mxs::Reply& reply);

    DiffData& operator += (const DiffData& rhs);

private:
    DiffHistogram m_histogram;
};
