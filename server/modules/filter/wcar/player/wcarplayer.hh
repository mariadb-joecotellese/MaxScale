/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarplayerconfig.hh"

bool execute_stmt(MYSQL* pConn, const std::string& sql);

class Player
{
public:
    Player(const PlayerConfig* pConfig);

    void replay();
private:
    const PlayerConfig& m_config;
};
