/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarfiltersession.hh"
#include "wcarfilter.hh"
#include <maxbase/log.hh>
#include <maxsimd/canonical.hh>
#include <maxscale/protocol/mariadb/mysql.hh>
#include <maxscale/protocol/mariadb/protocol_classes.hh>

// static
WcarFilterSession* WcarFilterSession::create(MXS_SESSION* pSession, SERVICE* pService,
                                             const WcarFilter* pFilter)
{
    return new WcarFilterSession(pSession, pService, pFilter);
}

WcarFilterSession::WcarFilterSession(MXS_SESSION* pSession, SERVICE* pService, const WcarFilter* pFilter)
    : maxscale::FilterSession(pSession, pService)
    , m_filter(*pFilter)
{
    const auto& maria_ses = static_cast<const MYSQL_session&>(protocol_data());

    if (!maria_ses.current_db.empty())
    {
        auto* pWorker = mxs::RoutingWorker::get_current();
        auto* pShared_data = m_filter.recorder().get_shared_data_by_index(pWorker->index());

        QueryEvent event;
        event.sCanonical = std::make_shared<std::string>("use " + maria_ses.current_db);
        pShared_data->send_update(event);
    }
}

WcarFilterSession::~WcarFilterSession()
{
}

bool WcarFilterSession::routeQuery(GWBUF&& buffer)
{
    m_query_event.canonical_args.clear();
    m_skip_capture = false;

    if (mariadb::is_com_query_or_prepare(buffer))
    {
        m_query_event.sCanonical = std::make_shared<std::string>(parser().get_sql(buffer));
        maxsimd::get_canonical_args(&*m_query_event.sCanonical, &m_query_event.canonical_args);
    }
    else
    {
        if (!generate_event_for(buffer, &m_query_event))
        {
            m_skip_capture = true;
        }
    }

    return mxs::FilterSession::routeQuery(std::move(buffer));
}

bool WcarFilterSession::clientReply(GWBUF&& buffer,
                                    const maxscale::ReplyRoute& down,
                                    const maxscale::Reply& reply)
{
    if (!m_skip_capture)
    {
        auto* pWorker = mxs::RoutingWorker::get_current();
        auto* pShared_data = m_filter.recorder().get_shared_data_by_index(pWorker->index());
        pShared_data->send_update(m_query_event);
    }

    return mxs::FilterSession::clientReply(std::move(buffer), down, reply);
}

bool WcarFilterSession::generate_event_for(const GWBUF& buffer, QueryEvent* pQuery_event)
{
    auto cmd = mariadb::get_command(buffer);

    // TODO add prepared statement handling (text and binary)
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
        MXB_SDEV("Ignore " << mariadb::cmd_to_string(cmd));
    }
    return false;
}
