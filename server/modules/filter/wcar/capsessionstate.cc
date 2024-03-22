/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "capsessionstate.hh"
#include "maxscale/protocol/mariadb/mysql.hh"
#include <sstream>
#include <maxbase/log.hh>
#include <maxscale/parser.hh>
#include <maxscale/boost_spirit_utils.hh>

using namespace maxscale::sql;

namespace
{
inline Gtid gtid_from_string(std::string_view gtid_str)
{
    if (gtid_str.empty())
    {
        return Gtid{0, 0, 0};
    }

    namespace x3 = boost::spirit::x3;

    const auto gtid_parser = x3::uint32 >> '-' >> x3::uint32 >> '-' >> x3::uint64;

    std::tuple<uint32_t, uint32_t, uint64_t> result;    // intermediary to avoid boost-fusionizing Gtid.

    auto first = begin(gtid_str);
    auto success = parse(first, end(gtid_str), gtid_parser, result);

    if (success && first == end(gtid_str))
    {

        return Gtid{std::get<0>(result), std::get<1>(result), std::get<2>(result)};
    }
    else
    {
        MXB_SERROR("Invalid gtid string: '" << gtid_str);
        return Gtid();
    }
}

constexpr auto WRITE_FLAGS = TYPE_WRITE;
// These are defined somewhere in the connector-c headers but including the header directly doesn't work.
// For the sake of simplicity, just declare them here.
constexpr uint16_t STATUS_IN_TRX = 1;
constexpr uint16_t STATUS_AUTOCOMMIT = 2;
constexpr uint16_t STATUS_IN_RO_TRX = 8192;

// autocommit, begin and STATUS_IN_TRX flag:
// begin always starts a trx, while setting autocommit=0 does
// not, rather the trx starts on the first write (or begin).
// Other than that, the trx flag behavior is the same for any
// mix of "set autocommit=0/begin/commit/set autocommit=1".
// No need to track autocommit state.
// An optimization could be to postpone the begin induced
// trx until there actually is a (MaxScale) write-flag.
// If there is a pattern where begin is followed by a lot of
// reads, then a write, this could have a large impact on
// replay speed.

// m_trx_start_id extra reset:
// This sequence: "begin; select; commit; select; insert;"
// sets the starting id to "begin", but the commit produces no
// gtid while the insert following it does. The starting id of the
// insert should be that of the insert itself, so the code will
// reset the starting id when it sees a trx-end without a gtid.

// Read-only trxns:
// are ignored as they do not cause dependencies to other
// sessions. TODO: They could still be handled - a session
// in READ ONLY does not depend on other sessions and thus
// events can be queued to it without regard to other trxns.
}

/* This code has only two purposes, determine the span [event_id,event_id]
 * of a transaction and always generate a transaction on a valid gtid.
 */
std::unique_ptr<Trx> CapSessionState::update(int64_t event_id, const mxs::Reply& reply)
{
    uint32_t status = reply.server_status();
    auto gtid = gtid_from_string(reply.get_variable(MXS_LAST_GTID));

    if (status != mxs::Reply::NO_SERVER_STATUS)
    {
        bool new_in_trx = (status & STATUS_IN_TRX) && !(status & STATUS_IN_RO_TRX);
        bool trx_starting = !m_in_trx && new_in_trx;
        bool trx_ending = m_in_trx && !new_in_trx;
        if (trx_starting)
        {
            m_in_trx = true;
            m_trx_start_id = event_id;
        }
        else if (trx_ending)
        {   // see comment above
            m_in_trx = false;
            if (!gtid.is_valid())
            {
                m_trx_start_id = -1;
            }
        }
    }

    if (event_id <= 0)
    {   // capture is not on, maintaining state.
        return {};
    }

    std::unique_ptr<Trx> sTrx;
    if (gtid.is_valid())
    {
        auto start_id = (m_trx_start_id >= 0) ? m_trx_start_id : event_id;
        sTrx = std::make_unique<Trx>(start_id, gtid);
        m_trx_start_id = -1;
    }

    return sTrx;
}
