/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capdefs.hh"
#include "maxbase/stopwatch.hh"
#include "maxbase/assert.hh"
#include <atomic>
#include <memory>
#include <type_traits>

// Should always succeed (maybe even by definition)
static_assert(std::is_same_v<mxb::Duration, wall_time::Duration>);
using DurationRep = mxb::Duration::rep;     // int64_t for anything we use

/**
 * @brief SimTime is like a system_clock that starts at the given time,
 *        but progresses as a steady_clock after that.
 *
 *        The progress of time can be slowed down or sped up.
 *
 *        Assume that time is Unix Time, guaranteed in C++20 but probably
 *        true on all relevant systems (TODO make sure).
 */
class SimTime
{
public:
    // Non-thread safe reset singleton
    static void     reset_sim_time(wall_time::TimePoint begin_time, float speed = 1.0);
    static SimTime& sim_time();

    // Steady time since begin_time.
    // The returned time stays the same between tick() calls.
    wall_time::TimePoint now();

    // Duration since begin_time
    wall_time::Duration delta();

    // Tick forwards. The more often this is called, the more
    // precise the time is.
    SimTime& tick();
private:
    SimTime(wall_time::TimePoint begin_time, float speed = 1.0);
    DurationRep speed_adjusted_delta();

    static inline std::unique_ptr<SimTime> m_sSimTime;

    float                    m_speed;
    DurationRep              m_wall_start;
    DurationRep              m_steady_start;
    std::atomic<DurationRep> m_steady_delta;
};

// IMPL
inline SimTime::SimTime(wall_time::TimePoint begin_time, float speed)
    : m_speed{speed}
    , m_wall_start(begin_time.time_since_epoch().count())
    , m_steady_start(mxb::Clock::now().time_since_epoch().count())
    , m_steady_delta{0}
{
}

inline void SimTime::reset_sim_time(wall_time::TimePoint begin_time, float speed)
{
    m_sSimTime = std::unique_ptr<SimTime>(new SimTime(begin_time, speed));
}

inline SimTime& SimTime::sim_time()
{
    mxb_assert(m_sSimTime);
    return *m_sSimTime;
}

inline DurationRep SimTime::speed_adjusted_delta()
{
    return m_speed * m_steady_delta.load(std::memory_order_acquire);
}

inline wall_time::TimePoint SimTime::now()
{
    wall_time::Duration wall_dur{m_wall_start + speed_adjusted_delta()};
    return wall_time::TimePoint{wall_dur};
}

inline wall_time::Duration SimTime::delta()
{
    return wall_time::Duration{speed_adjusted_delta()};
}

inline SimTime& SimTime::tick()
{
    DurationRep steady_now = mxb::Clock::now().time_since_epoch().count();
    DurationRep steady_delta = steady_now - m_steady_start;
    DurationRep expected_delta = m_steady_delta.load(std::memory_order_relaxed);

    while (steady_delta > expected_delta
           && !m_steady_delta.compare_exchange_weak(
               expected_delta,
               steady_delta,
               std::memory_order_release,
               std::memory_order_relaxed))
    {
    }

    return *this;
}
