/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"
#include <maxbase/checksum.hh>
#include <maxscale/target.hh>

class UratBackend;

/**
 * @class UratResult
 *
 * The result of executing one particular statement.
 */
class UratResult final
{
public:
    using Clock = std::chrono::steady_clock;

    UratResult()
        : m_start(Clock::now())
    {
    }

    void update_checksum(const GWBUF& buffer)
    {
        m_checksum.update(buffer);
    }

    void close(const mxs::Reply& reply)
    {
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
        return m_checksum;
    }

    const mxs::Reply& reply() const
    {
        return m_reply;
    }

    std::chrono::milliseconds duration() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(m_end - m_start);
    }

private:
    Clock::time_point m_start;
    Clock::time_point m_end;
    mxb::CRC32        m_checksum;
    mxs::Reply        m_reply;
};
