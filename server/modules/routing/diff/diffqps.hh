/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <vector>

class DiffQps
{
public:
    using Values = std::vector<uint32_t>;

    DiffQps(time_t start_time)
        : m_start_time(start_time)
    {
    }

    time_t start_time() const
    {
        return m_start_time;
    }

    void inc()
    {
        auto now = time(nullptr);

        auto index = now - m_start_time;

        m_values.resize(index + 1);

        ++m_values[index];
    }

    const Values& values() const
    {
        return m_values;
    }

    DiffQps& operator += (const DiffQps& rhs)
    {
        mxb_assert(m_start_time == rhs.m_start_time);

        if (m_values.size() < rhs.m_values.size())
        {
            m_values.resize(rhs.m_values.size());
        }

        auto it = m_values.begin();
        for (auto value : rhs.m_values)
        {
            *it++ += value;
        }

        return *this;
    }

private:
    time_t                m_start_time;
    std::vector<uint32_t> m_values;
};
