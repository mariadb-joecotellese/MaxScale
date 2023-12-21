/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <maxbase/checksum.hh>
#include <maxscale/target.hh>

class ComparatorBackend;

/**
 * @class ComparatorResult
 *
 * The result of executing one particular statement.
 */
class ComparatorResult final
{
public:
    using Clock = std::chrono::steady_clock;

    ComparatorResult()
        : m_start(Clock::now())
        , m_end(Clock::time_point::max())
    {
    }

    bool closed() const
    {
        return m_end != Clock::time_point::max();
    }

    void update_checksum(const GWBUF& buffer)
    {
        mxb_assert(!closed());
        m_checksum.update(buffer);
    }

    void close(const mxs::Reply& reply)
    {
        mxb_assert(!closed());
        m_reply = reply;
        m_end = Clock::now();
    }

    void reset()
    {
        m_start = Clock::now();
        m_end = m_start;
        m_checksum.reset();
        m_reply.clear();
    }

    const mxb::CRC32& checksum() const
    {
        mxb_assert(closed());
        return m_checksum;
    }

    const mxs::Reply& reply() const
    {
        mxb_assert(closed());
        return m_reply;
    }

    std::chrono::milliseconds duration() const
    {
        mxb_assert(closed());
        return std::chrono::duration_cast<std::chrono::milliseconds>(m_end - m_start);
    }

private:
    Clock::time_point m_start;
    Clock::time_point m_end;
    mxb::CRC32        m_checksum;
    mxs::Reply        m_reply;
};
