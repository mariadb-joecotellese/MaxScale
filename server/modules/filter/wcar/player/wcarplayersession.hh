/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxbase/stopwatch.hh>
#include "wcarplayer.hh"
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

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

    void queue_query(QueryEvent&& qevent);
    void stop();

private:

    void run();

    const PlayerConfig&     m_config;
    Player&                 m_player;
    int64_t                 m_session_id;
    MYSQL*                  m_pConn;
    std::thread             m_thread;
    std::mutex              m_mutex;
    std::condition_variable m_condition;
    std::deque<QueryEvent>  m_queue;
    bool                    m_request_stop = false;
};
