/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capdefs.hh"
#include "caprecorder.hh"
#include "capsessionstate.hh"
#include "capstorage.hh"
#include "capsessionstate.hh"
#include <maxscale/filter.hh>
#include <maxscale/protocol/mariadb/trackers.hh>

enum class CapState {DISABLED, PENDING_ENABLE, ENABLED};
enum class CapSignal {START, STOP, QEVENT, CLOSE_SESSION};

class CapFilter;

class CapFilterSession final : public maxscale::FilterSession
{
public:
    ~CapFilterSession();

    // Starts with capture disabled
    static CapFilterSession* create(MXS_SESSION* pSession, SERVICE* pService, const CapFilter* pFilter);

    void start_capture(const std::shared_ptr<CapRecorder>& sRecorder);
    void stop_capture();

    bool routeQuery(GWBUF&& buffer) override;
    bool clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    void handle_cap_state(CapSignal signal);

private:
    CapFilterSession(MXS_SESSION* pSession, SERVICE* pService, const CapFilter* pFilter);
    CapFilterSession(CapFilterSession&&) = delete;

private:
    enum class InitState
    {
        SEND_QUERY,
        READ_RESULT,
        INIT_DONE,
    };

    /**
     * @brief generate_canonical_for - Fill *pQuery_event with canonical and args
     *                                 for a non-sql buffer, if possible.
     * @param buffer
     * @param query_event
     * @return true if the event should be captured
     */
    bool generate_canonical_for(const GWBUF& buffer, QueryEvent* pQuery_event);

    // Index of the worker passed in because there are cases where
    // no RoutingWorker is involved.
    enum Who {CURRENT_WORKER, MAIN_WORKER};
    void                    send_event(QueryEvent&& qevent, Who who = CURRENT_WORKER);
    std::vector<QueryEvent> make_opening_events(wall_time::TimePoint start_time);
    QueryEvent              make_rollback_event();
    QueryEvent              make_closing_event();

    const CapFilter&             m_filter;
    std::shared_ptr<CapRecorder> m_sRecorder;
    std::atomic<CapState>        m_state {CapState::DISABLED};
    std::mutex                   m_state_mutex;

    CapSessionState m_session_state;
    bool            m_inside_initial_trx = false;   // A trx was active when capture started
    bool            m_capture = false;
    InitState       m_init_state = InitState::SEND_QUERY;
    QueryEvent      m_query_event;

    mariadb::PsTracker m_ps_tracker;
};
