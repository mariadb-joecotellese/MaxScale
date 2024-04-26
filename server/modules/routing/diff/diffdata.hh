/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <maxbase/stopwatch.hh>
#include <maxscale/target.hh>
#include "diffhistogram.hh"

class DiffConfig;

class DiffData
{
public:
    class Explain
    {
    public:
        Explain(const mxb::TimePoint& when,
                std::string_view sql,
                json_t* pExplain);

        Explain(const Explain& other) = delete;
        Explain& operator = (const Explain& rhs) = delete;

        ~Explain();

        const mxb::TimePoint& when() const
        {
            return m_when;
        }

        const std::string& sql() const
        {
            return m_sql;
        }

        json_t* json() const
        {
            return m_pExplain;
        }

    private:
        mxb::TimePoint m_when;
        std::string    m_sql;
        json_t*        m_pExplain;
    };

    using SExplain = std::shared_ptr<Explain>;
    using Explains = std::multimap<mxb::Duration, SExplain, std::less<>>;

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

    const Explains& explains() const
    {
        return m_explains;
    }

    void add(const mxb::Duration& duration, const mxs::Reply& reply);

    void add_explain(const mxb::Duration& duration,
                     const mxb::TimePoint& when,
                     std::string_view sql,
                     json_t* pExplain);

    void combine(const DiffData& rhs, const DiffConfig& config);

private:
    int64_t       m_errors { 0 };
    int64_t       m_rr_count { 0 };
    int64_t       m_rr_max { 0 };
    int64_t       m_rr_min { std::numeric_limits<int64_t>::max() };
    int64_t       m_rr_sum { 0 };
    DiffHistogram m_histogram;
    Explains      m_explains;
};
