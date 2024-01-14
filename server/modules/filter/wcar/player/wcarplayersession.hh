/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxbase/stopwatch.hh>
#include "wcarplayer.hh"

bool execute_stmt(MYSQL* pConn, const std::string& sql);


/**
 * @brief A Session is a single thread performing queries one by one
 *        in sync with the Player.
 *        The player calls Session::queue_query() to initiate a query,
 *        the Session respond with a callback (not implemented).
 */
class PlayerSession
{
public:
    PlayerSession(const PlayerConfig* pConfig, Player* pPlayer, int64_t session_id);
    ~PlayerSession();
    PlayerSession(PlayerSession&&) = delete;

    void queue_query(const std::string& sql);

private:
    const PlayerConfig& m_config;
    Player&             m_player;
    int64_t             m_session_id;
    MYSQL*              m_pConn;
};
