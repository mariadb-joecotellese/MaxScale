/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <memory>
#include <boost/circular_buffer.hpp>

class DiffQps
{
public:
    // TODO: circular_buffer_space_optimized would be better with qps calculated
    // TODO: separately for each session, but that does not compile and a session
    // TODO: specific calculation will be removed anyway.
    using Values = boost::circular_buffer<uint32_t>;
    using iterator = Values::iterator;

    DiffQps(std::chrono::seconds qps_window)
        : m_capacity(qps_window.count())
        , m_values(m_capacity)
    {
        mxb_assert(m_values.capacity() > 0);
    }

    void clear()
    {
        m_values.clear();
    }

    iterator begin()
    {
        return m_values.begin();
    }

    iterator end()
    {
        return m_values.end();
    }

    time_t start_time() const
    {
        return m_end_time - m_values.size();
    }

    time_t end_time() const
    {
        return m_end_time;
    }

    size_t size() const
    {
        return m_values.size();
    }

    void inc();

    const Values& values() const
    {
        return m_values;
    }

    DiffQps& operator += (const DiffQps& rhs);

private:
    const size_t m_capacity;
    Values       m_values;
    time_t       m_end_time {0};
};

using SDiffQps = std::shared_ptr<DiffQps>;
