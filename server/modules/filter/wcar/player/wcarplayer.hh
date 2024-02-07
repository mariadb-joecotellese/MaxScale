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
    void trxn_finished(int64_t event_id);

    // PlayerSession callback
    void session_finished(const PlayerSession& session);

private:
    // Simulated time corresponding to the original timeline,
    // sim_time() can be directly compared to a captured time.
    mxb::TimePoint sim_time();

    const PlayerConfig& m_config;

    // Delta between start of simulation and capture_time (positive)
    mxb::Duration m_timeline_delta = mxb::Duration::zero();

    // For event_finished() callbacks.
    std::mutex                  m_trxn_mutex;
    std::condition_variable     m_trxn_condition;
    std::unordered_set<int64_t> m_finished_trxns;

    // For session_finished() callbacks.
    std::mutex                  m_session_mutex;
    std::unordered_set<int64_t> m_finished_sessions;
};
