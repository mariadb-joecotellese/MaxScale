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
    UratResult() = default;

    UratResult(const mxb::CRC32&                checksum,
               const mxs::Reply&                reply,
               const std::chrono::milliseconds& duration)
        : m_checksum(checksum)
        , m_reply(reply)
        , m_duration(duration)
    {
    }

    void clear()
    {
        m_checksum.reset();
        m_reply.clear();
        m_duration = std::chrono::milliseconds {0};
    }

    const mxb::CRC32& checksum() const
    {
        return m_checksum;
    }

    const mxs::Reply& reply() const
    {
        return m_reply;
    }

    const std::chrono::milliseconds& duration() const
    {
        return m_duration;
    }

private:
    mxb::CRC32                m_checksum;
    mxs::Reply                m_reply;
    std::chrono::milliseconds m_duration { 0 };
};
