/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "maxscale/mock/routersession.hh"
#include "maxscale/mock/backend.hh"

namespace maxscale
{

namespace mock
{

RouterSession::RouterSession(Backend* pBackend, maxscale::mock::Session* session)
    : m_pBackend(pBackend)
    , m_pSession(session)
{
}

RouterSession::~RouterSession()
{
}

void RouterSession::set_upstream(FilterModule::Session* pFilter_session)
{
    m_pUpstream_filter_session = pFilter_session;
}

bool RouterSession::respond()
{
    return m_pBackend->respond(this, mxs::Reply());
}

bool RouterSession::idle() const
{
    return m_pBackend->idle(this);
}

bool RouterSession::discard_one_response()
{
    return m_pBackend->discard_one_response(this);
}

void RouterSession::discard_all_responses()
{
    return m_pBackend->discard_all_responses(this);
}

bool RouterSession::routeQuery(GWBUF&& statement)
{
    m_pBackend->handle_statement(this, std::move(statement));
    return 1;
}

bool RouterSession::clientReply(GWBUF&& response, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    return m_pUpstream_filter_session->clientReply(std::move(response), reply);
}
}
}
