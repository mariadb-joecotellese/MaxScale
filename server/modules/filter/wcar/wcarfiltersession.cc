/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarfiltersession.hh"
#include "wcarfilter.hh"
#include <maxsimd/canonical.hh>

WcarFilterSession::WcarFilterSession(MXS_SESSION* pSession, SERVICE* pService, const WcarFilter* pFilter)
    : maxscale::FilterSession(pSession, pService)
    , m_filter(*pFilter)
{
}

WcarFilterSession::~WcarFilterSession()
{
}

// static
WcarFilterSession* WcarFilterSession::create(MXS_SESSION* pSession, SERVICE* pService,
                                             const WcarFilter* pFilter)
{
    return new WcarFilterSession(pSession, pService, pFilter);
}

bool WcarFilterSession::routeQuery(GWBUF&& buffer)
{
    m_query_event.canonical= parser().get_sql(buffer);
    m_query_event.canonical_args.clear();
    maxsimd::get_canonical(&m_query_event.canonical, &m_query_event.canonical_args);

    return mxs::FilterSession::routeQuery(std::move(buffer));
}

bool WcarFilterSession::clientReply(GWBUF&& buffer,
                                    const maxscale::ReplyRoute& down,
                                    const maxscale::Reply& reply)
{
    auto pWorker = mxs::RoutingWorker::get_current();
    auto pShared_data = m_filter.recorder().get_shared_data_by_index(pWorker->index());
    pShared_data->send_update(m_query_event);

    return mxs::FilterSession::clientReply(std::move(buffer), down, reply);
}
