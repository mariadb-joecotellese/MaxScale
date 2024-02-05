/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarplayerconfig.hh"
#include "wcarplayersession.hh"

class Player
{
public:
    Player(const PlayerConfig* pConfig);

    void replay();

    // PlayerSession callback
    void trxn_finished(int64_t event_id)
    {
    }
    // PlayerSession callback
    void session_finished(const PlayerSession& session)
    {
    }

private:
    const PlayerConfig& m_config;
};
