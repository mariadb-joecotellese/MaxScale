/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "capfiltersession.hh"
#include "capfilter.hh"
#include "simtime.hh"
#include <maxbase/log.hh>
#include <maxsimd/canonical.hh>
#include <maxscale/protocol/mariadb/mysql.hh>
#include <maxscale/protocol/mariadb/protocol_classes.hh>

namespace
{
const auto set_last_gtid = mariadb::create_query(
    "SET @@session.session_track_system_variables = CASE @@session.session_track_system_variables "
    "WHEN '*' THEN '*' WHEN '' THEN 'last_gtid' ELSE "
    "CONCAT(@@session.session_track_system_variables, ',last_gtid') END;");
}

// static
CapFilterSession* CapFilterSession::create(MXS_SESSION* pSession, SERVICE* pService,
                                           const CapFilter* pFilter)
{
    return new CapFilterSession(pSession, pService, pFilter);
}

CapFilterSession::CapFilterSession(MXS_SESSION* pSession, SERVICE* pService, const CapFilter* pFilter)
    : maxscale::FilterSession(pSession, pService)
    , m_filter(*pFilter)
{
}

void CapFilterSession::handle_cap_state(CapSignal signal)
{
    std::lock_guard guard{m_state_mutex};
    bool handled = false;

    switch (m_state.load(std::memory_order_relaxed))
    {
    case CapState::DISABLED:
        switch (signal)
        {
        case CapSignal::START:
            m_state.store(CapState::PENDING_ENABLE, std::memory_order_release);
            handled = true;
            break;

        case CapSignal::CLOSE_SESSION:
            handled = true;
            break;

        case CapSignal::QEVENT:
            // Ignore
            handled = true;
            break;

        default:
            break;
        }
        break;

    case CapState::PENDING_ENABLE:
        switch (signal)
        {
        case CapSignal::QEVENT:
            for (auto&& e : make_opening_events(m_query_event.start_time))
            {
                send_event(std::move(e));
            }
            send_event(std::move(m_query_event));

            m_state.store(CapState::ENABLED, std::memory_order_release);
            handled = true;
            break;

        case CapSignal::CLOSE_SESSION:
            m_state.store(CapState::DISABLED, std::memory_order_release);
            handled = true;
            break;

        default:
            break;
        }
        break;

    case CapState::ENABLED:
        switch (signal)
        {
        case CapSignal::QEVENT:
            send_event(std::move(m_query_event));
            handled = true;
            break;

        case CapSignal::CLOSE_SESSION:
            send_event(make_closing_event());
            m_state.store(CapState::DISABLED, std::memory_order_release);
            handled = true;
            break;

        case CapSignal::STOP:
            send_event(make_closing_event(), MAIN_WORKER);
            m_state.store(CapState::DISABLED, std::memory_order_release);
            handled = true;
            break;

        default:
            break;
        }
        break;
    }

#ifdef SS_DEBUG
    if (!handled)
    {
        MXB_SERROR("Capture: Unhandled signal " << int(signal) << " in state " << int(m_state.load()));
        mxb_assert(!true);
    }
#endif
}

void CapFilterSession::start_capture(const std::shared_ptr<CapRecorder>& sRecorder)
{
    m_inside_initial_trx = m_session_state.in_trx();
    m_sRecorder = std::atomic_load_explicit(&sRecorder, std::memory_order_relaxed);
    handle_cap_state(CapSignal::START);
}

void CapFilterSession::stop_capture()
{
    handle_cap_state(CapSignal::STOP);
    m_sRecorder.reset();
}

CapFilterSession::~CapFilterSession()
{
    if (m_sRecorder)
    {
        handle_cap_state(CapSignal::CLOSE_SESSION);
    }
}

void CapFilterSession::send_event(QueryEvent&& qevent, Who who)
{
    mxb_assert(m_sRecorder);

    int idx = 0;    // Index of the first SharedData.
    if (who == CURRENT_WORKER)
    {
        auto* pWorker = mxs::RoutingWorker::get_current();
        idx = pWorker->index();
    }

    auto* pShared_data = m_sRecorder->get_shared_data_by_index(idx);
    pShared_data->send_update(std::move(qevent));
}

std::vector<QueryEvent> CapFilterSession::make_opening_events(wall_time::TimePoint start_time)
{
    std::vector<QueryEvent> events;

    const auto& maria_ses = static_cast<const MYSQL_session&>(protocol_data());
    mxb_assert(maria_ses.auth_data);
    QueryEvent opening_event;
    opening_event.session_id = m_pSession->id();
    opening_event.flags = 0;
    auto now = SimTime::sim_time().now();
    // The "- 1ns" is there to avoid having to take the flag into account in later sorting
    opening_event.start_time = start_time - 1ns;
    opening_event.end_time = start_time;
    opening_event.flags = CAP_ARTIFICIAL;

    if (!maria_ses.current_db.empty())
    {
        opening_event.sCanonical = std::make_shared<std::string>("use " + maria_ses.current_db);
        opening_event.event_id = m_filter.get_next_event_id();
        events.push_back(std::move(opening_event));
    }

    const auto& collations = m_pSession->connection_metadata().collations;

    if (auto it = collations.find(maria_ses.auth_data->collation); it != collations.end())
    {
        auto sql = "set names '"s + it->second.character_set + "' "
            + "collate '" + it->second.collation + "'";
        opening_event.sCanonical = std::make_shared<std::string>(std::move(sql));
        opening_event.event_id = m_filter.get_next_event_id();
        events.push_back(std::move(opening_event));
    }

    return events;
}

QueryEvent CapFilterSession::make_closing_event()
{
    QueryEvent closing_event;
    // Non empty canonical to avoid checking, with a message for debug.
    closing_event.sCanonical = std::make_shared<std::string>("Close session");
    closing_event.session_id = m_pSession->id();
    closing_event.flags = CAP_SESSION_CLOSE;
    closing_event.start_time = SimTime::sim_time().now() + 1ns;
    closing_event.end_time = closing_event.start_time;
    closing_event.event_id = m_filter.get_next_event_id();

    return closing_event;
}

bool CapFilterSession::routeQuery(GWBUF&& buffer)
{
    if (m_init_state == InitState::SEND_QUERY)
    {
        m_init_state = InitState::READ_RESULT;

        if (!mxs::FilterSession::routeQuery(set_last_gtid.shallow_clone()))
        {
            return false;
        }
    }

    SimTime::sim_time().tick();

    m_capture = m_state.load(std::memory_order_relaxed) != CapState::DISABLED;

    m_ps_tracker.track_query(buffer);

    if (m_ps_tracker.is_multipart() || m_ps_tracker.should_ignore())
    {
        // TODO: This does not work if multiple queries are pending. A small COM_QUERY followed by a very
        // TODO: big COM_QUERY will cause both to not be recorded.
        m_capture = false;
        return mxs::FilterSession::routeQuery(std::move(buffer));
    }

    if (auto [canonical, args] = m_ps_tracker.get_args(buffer); !canonical.empty())
    {
        m_query_event.sCanonical = std::make_shared<std::string>(std::move(canonical));
        m_query_event.canonical_args = std::move(args);
    }
    else
    {
        if (!generate_canonical_for(buffer, &m_query_event))
        {
            m_capture = false;
        }
    }

    if (m_capture)
    {
        const auto& maria_ses = static_cast<const MYSQL_session&>(protocol_data());
        m_query_event.session_id = m_pSession->id();
        m_query_event.start_time = SimTime::sim_time().now();
        m_query_event.flags = parser().get_type_mask(buffer);
    }

    return mxs::FilterSession::routeQuery(std::move(buffer));
}

bool CapFilterSession::clientReply(GWBUF&& buffer,
                                   const maxscale::ReplyRoute& down,
                                   const maxscale::Reply& reply)
{
    if (m_init_state == InitState::READ_RESULT)
    {
        if (reply.is_complete())
        {
            m_init_state = InitState::INIT_DONE;
        }

        // Ignore the response to the generated SET command. The protocol module guarantees that only one
        // result per clientReply call is delivered.
        return true;
    }

    SimTime::sim_time().tick();

    m_ps_tracker.track_reply(reply);

    if (m_ps_tracker.is_ldli())
    {
        mxb_assert(m_ps_tracker.should_ignore());
        // LOAD DATA LOCAL INFILE is starting, ignore it.
        m_capture = false;
    }

    if (reply.is_complete())
    {
        if (m_capture)
        {
            m_query_event.end_time = SimTime::sim_time().now();
            m_query_event.event_id = m_filter.get_next_event_id();
            m_query_event.sTrx = m_session_state.update(m_query_event.event_id, reply);
            // This implicitely implements CaptureStartMethod::IGNORE_ACTIVE_TRANSACTIONS
            if (!m_inside_initial_trx)
            {
                handle_cap_state(CapSignal::QEVENT);
            }
        }
        else
        {
            m_session_state.update(-1, reply);      // maintain state
        }
    }

    if (m_inside_initial_trx)
    {
        m_inside_initial_trx = m_session_state.in_trx();
    }

    return mxs::FilterSession::clientReply(std::move(buffer), down, reply);
}

bool CapFilterSession::generate_canonical_for(const GWBUF& buffer, QueryEvent* pQuery_event)
{
    auto cmd = mariadb::get_command(buffer);
    bool generated = true;

    switch (cmd)
    {
    case MXS_COM_CREATE_DB:
        {
            const uint8_t* data = buffer.data();
            auto* pStart = data + MYSQL_HEADER_LEN + 1;
            auto* pEnd = data + buffer.length();
            pQuery_event->sCanonical =
                std::make_shared<std::string>("create database "s + std::string(pStart, pEnd));
        }
        break;

    case MXS_COM_DROP_DB:
        {
            const uint8_t* data = buffer.data();
            auto* pStart = data + MYSQL_HEADER_LEN + 1;
            auto* pEnd = data + buffer.length();
            pQuery_event->sCanonical =
                std::make_shared<std::string>("drop database "s + std::string(pStart, pEnd));
        }
        break;

    case MXS_COM_INIT_DB:
        {
            const uint8_t* data = buffer.data();
            auto* pStart = data + MYSQL_HEADER_LEN + 1;
            auto* pEnd = data + buffer.length();
            pQuery_event->sCanonical =
                std::make_shared<std::string>("use "s + std::string(pStart, pEnd));
        }
        break;

        /* TODO These need special handling, add cmd into QueryEvent */
    case MXS_COM_QUIT:
    case MXS_COM_RESET_CONNECTION:
    case MXS_COM_SET_OPTION:
    case MXS_COM_STATISTICS:

        /* These can be ignored during replay and so are
         * not captured.
         */
    case MXS_COM_FIELD_LIST:
    case MXS_COM_DEBUG:
    case MXS_COM_PING:
    case MXS_COM_PROCESS_INFO:
    case MXS_COM_PROCESS_KILL:
    case MXS_COM_SHUTDOWN:
    default:
        generated = false;
        MXB_SDEV("Ignore " << mariadb::cmd_to_string(cmd));
    }

    return generated;
}
