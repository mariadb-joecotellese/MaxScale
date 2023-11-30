/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

// All log messages from this module are prefixed with this
#define MXB_MODULE_NAME "examplefilter"

#include "examplefiltersession.hh"
#include "examplefilter.hh"
#include <maxscale/session.hh>

ExampleFilterSession::ExampleFilterSession(MXS_SESSION* pSession, SERVICE* pService, ExampleFilter& filter)
    : mxs::FilterSession(pSession, pService)
    , m_filter(filter)
    , m_session_id(pSession->id())
{
}

ExampleFilterSession::~ExampleFilterSession()
{
}

// static
ExampleFilterSession* ExampleFilterSession::create(MXS_SESSION* pSession, SERVICE* pService,
                                                   ExampleFilter& filter)
{
    return new ExampleFilterSession(pSession, pService, filter);
}

void ExampleFilterSession::close()
{
    // When the session is closed, report the numbers to the log.
    MXB_NOTICE("Session %lu routed %i queries and %i replies.", m_session_id, m_queries, m_replies);
}

bool ExampleFilterSession::routeQuery(GWBUF&& packet)
{
    m_queries++;
    m_filter.query_seen();

    // Pass the query forward.
    return mxs::FilterSession::routeQuery(std::move(packet));
}

bool ExampleFilterSession::clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    m_replies++;
    m_filter.reply_seen();

    // Pass the reply forward.
    return mxs::FilterSession::clientReply(std::move(packet), down, reply);
}
