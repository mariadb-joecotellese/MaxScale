/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratdefs.hh"
#include <maxbase/checksum.hh>
#include <maxscale/target.hh>

class UratBackend;

class UratResult final
{
public:
    UratResult(UratBackend*                     pBackend,
               const mxb::CRC32&                checksum,
               const mxs::Reply&                reply,
               const std::chrono::milliseconds& duration)
        : m_pBackend(pBackend)
        , m_checksum(checksum)
        , m_reply(reply)
        , m_duration(duration)
    {
        mxb_assert(pBackend);
    }

    UratBackend& backend() const
    {
        return *m_pBackend;
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
    UratBackend*              m_pBackend;
    mxb::CRC32                m_checksum;
    mxs::Reply                m_reply;
    std::chrono::milliseconds m_duration;
};
