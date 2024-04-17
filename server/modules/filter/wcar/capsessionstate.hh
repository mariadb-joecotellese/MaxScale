/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capstorage.hh"
#include <maxscale/target.hh>

class CapSessionState
{
public:
    CapSessionState() = default;

    /** Return a valid ptr if the reply ended a trx where
     *  a trx begin had been detected.
     */
    std::unique_ptr<Trx> update(int64_t event_id, const mxs::Reply& reply);

    /** For the case where capture is stopped in the middle of a trx and
     *  a rollback (or whatever) is generated.
     */
    std::unique_ptr<Trx> make_fake_trx(int64_t event_id);

    bool in_trx();

private:
    int64_t m_trx_start_id = -1;
    bool    m_in_trx = false;
};

inline bool CapSessionState::in_trx()
{
    return m_in_trx;
}
