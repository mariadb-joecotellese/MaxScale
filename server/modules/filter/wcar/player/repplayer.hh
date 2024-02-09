/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "repconfig.hh"
#include "repsession.hh"
#include "reptransform.hh"

class RepPlayer
{
public:
    RepPlayer(const RepConfig* pConfig);

    void replay();

    // PlayerSession callback
    void trxn_finished(int64_t event_id);

    // RepSession callback
    void session_finished(const RepSession& session);

private:
    // Simulated time corresponding to the original timeline,
    // sim_time() can be directly compared to a captured time.
    mxb::TimePoint sim_time();

    // Wait for qevent start to reach sim_time(), then schedule_event().
    void timeline_add(RepSession& session, QueryEvent&& qevent);

    struct ExecutionInfo
    {
        bool                   can_execute;
        Transactions::iterator trx_start_ite;
    };

    // This is called for events that can be scheduled according to the timeline.
    ExecutionInfo get_execution_info(RepSession& session, const QueryEvent& qevent);

    // Schedule an event.
    void schedule_event(RepSession& session, QueryEvent&& qevent);

    // The m_session_mutex is locked when this is called,
    // it is unlocked before return.
    bool schedule_pending_events(std::unique_lock<std::mutex>& lock);

    // Mark completed transactions, move m_front_trxn forwards
    void mark_completed_trxns(const std::unordered_set<int64_t>& finished_trxns);

    // Remove finished session. This can be called lazily, but is
    // necessary for waiting at simulation end.
    void remove_finished_sessions();

    // After all events have been read from storage,
    // schedule pending events until none remain,
    // then wait for sessions to finish.
    void wait_for_sessions_to_finish();

    const RepConfig& m_config;
    RepTransform     m_transform;

    // Delta between start of simulation and capture_time (positive)
    mxb::Duration m_timeline_delta = mxb::Duration::zero();

    // Active sessions
    std::unordered_map<int64_t, std::unique_ptr<RepSession>> m_sessions;

    // Iterator to the first incomplete transaction.
    Transactions::const_iterator m_front_trxn;

    // For event_finished() callbacks.
    std::mutex                  m_trxn_mutex;
    std::condition_variable     m_trxn_condition;
    std::unordered_set<int64_t> m_finished_trxns;

    // For session_finished() callbacks.
    std::mutex                  m_session_mutex;
    std::condition_variable     m_session_condition;
    std::unordered_set<int64_t> m_finished_sessions;

    // Currently for ad-hoc measuring time
    mxb::StopWatch m_stopwatch;
};
