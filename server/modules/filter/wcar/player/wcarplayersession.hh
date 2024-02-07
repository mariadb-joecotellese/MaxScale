/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarplayerconfig.hh"
#include <maxbase/stopwatch.hh>
#include <maxbase/assert.hh>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>

class Player;

/**
 * @brief A PlayerSession is a single thread performing queries off of a queue.
 *
 *        The player calls Session::queue_query() to initiate a query, and
 *        can ask for a callback on a future event_id.
 *
 *        Once the session ends (a close-event is seen) the db connection
 *        is closed, Player::session_finished() is called, and
 *        the thread function, PlayerSession::run(), returns.
 *
 *        There is currently no PlayerSession::stop(), which will be needed once
 *        an overall simulation timeout is implemented (or to gracefully respond
 *        to a kill signal).
 */
class PlayerSession
{
public:
    PlayerSession(const PlayerConfig* pConfig, Player* pPlayer, int64_t session_id);
    ~PlayerSession();
    PlayerSession(PlayerSession&&) = delete;

    int64_t session_id() const;
    void    queue_query(QueryEvent&& qevent, int64_t commit_event_id = -1);

    // The functions below are called only from the Player thread.
    bool    in_trxn() const;
    int64_t commit_event_id();
    void    reset_commit_event_id();

    void              add_pending(QueryEvent&& qevent);
    bool              has_pending_events() const;
    const QueryEvent& front_pending() const;
    void              queue_front_pending(int64_t commit_event_id = -1);

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

    // These are only used by the Player thread, so no synch needed.
    int64_t                m_commit_event_id = -1;
    std::deque<QueryEvent> m_pending_events;
};

inline int64_t PlayerSession::session_id() const
{
    return m_session_id;
}

inline bool PlayerSession::in_trxn() const
{
    return m_commit_event_id != -1;
}

inline int64_t PlayerSession::commit_event_id()
{
    return m_commit_event_id;
}

inline void PlayerSession::reset_commit_event_id()
{
    mxb_assert(m_commit_event_id != -1);
    m_commit_event_id = -1;
}

inline void PlayerSession::add_pending(QueryEvent&& qevent)
{
    m_pending_events.push_back(std::move(qevent));
}

inline bool PlayerSession::has_pending_events() const
{
    return !m_pending_events.empty();
}

inline const QueryEvent& PlayerSession::front_pending() const
{
    return m_pending_events.front();
}

inline void PlayerSession::queue_front_pending(int64_t commit_event_id)
{
    queue_query(std::move(m_pending_events.front()), commit_event_id);
    m_pending_events.pop_front();
}
