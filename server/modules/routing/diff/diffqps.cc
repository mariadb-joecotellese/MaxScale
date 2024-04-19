/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffqps.hh"

void DiffQps::inc()
{
    auto now = time(nullptr);

    if (now < m_end_time)
    {
        now = m_end_time;
    }

    size_t n = now - m_end_time;

    if (n == 0)
    {
        // More replies in the current second.
        if (m_values.empty())
        {
            m_values.push_back(1);
        }
        else
        {
            ++m_values.back();
        }
    }
    else if (n > m_capacity)
    {
        // Previous update was so far back in history that no value is valid anymore.
        m_values.clear();
        m_values.push_back(1);
    }
    else
    {
        if (n > 1)
        {
            // No responses between last update and now. (n - 1) zero entries must be added.
            for (size_t i = 0; i < n - 1; ++i)
            {
                m_values.push_back(0);
            }
        }

        m_values.push_back(1);
    }

    m_end_time = now;
}

namespace
{

void copy_qps(DiffQps::Values& to, time_t to_st, const DiffQps::Values& from, time_t from_st)
{
    auto it = to.begin();
    auto jt = from.begin();

    if (to_st < from_st)
    {
        it += (from_st - to_st);
    }
    else
    {
        jt += (to_st - from_st);
    }

    mxb_assert(it + (from.end() - jt) <= to.end());

    while (jt != from.end())
    {
        *it += *jt;

        ++it;
        ++jt;
    }
}

}

DiffQps& DiffQps::operator += (const DiffQps& rhs)
{
    mxb_assert(m_capacity == rhs.m_capacity);

    if (m_values.empty())
    {
        m_values = rhs.m_values;
        m_end_time = rhs.m_end_time;
    }
    else
    {
        auto lst = start_time();
        auto rst = rhs.start_time();
        auto let = end_time();
        auto ret = rhs.end_time();

        if (rst >= lst && ret <= let)
        {
            // The easy case, the values of rhs fits into the values of *this. This
            // should relatively quickly become the norm.

            int i = rst - lst;
            for (auto count : rhs.m_values)
            {
                m_values[i++] += count;
            }
        }
        else
        {
            // The messy case.
            Values values;

            auto end_time = let > ret ? let : ret;
            auto start_time = lst < rst ? lst : rst;
            mxb_assert(end_time > start_time);
            time_t capacity = end_time - start_time;

            if (capacity > (time_t)m_capacity)
            {
                start_time += (capacity - m_capacity);
                capacity = m_capacity;
            }

            mxb_assert(end_time - start_time <= (time_t)m_capacity);

            values.resize(capacity);

            copy_qps(values, start_time, m_values, lst);
            copy_qps(values, start_time, rhs.m_values, rst);

            m_values.swap(values);
            m_end_time = end_time;

            mxb_assert((time_t)m_values.size() == m_end_time - start_time);
        }
    }

    return *this;
}
