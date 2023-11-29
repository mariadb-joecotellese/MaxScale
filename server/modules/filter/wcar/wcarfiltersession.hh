/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcardefs.hh"
#include "wcarstorage.hh"
#include <maxscale/filter.hh>

class WcarFilter;

class WcarFilterSession final : public maxscale::FilterSession
{
public:
    ~WcarFilterSession();

    static WcarFilterSession* create(MXS_SESSION* pSession, SERVICE* pService, const WcarFilter* pFilter);

    bool routeQuery(GWBUF&& buffer) override;
    bool clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

private:
    WcarFilterSession(MXS_SESSION* pSession, SERVICE* pService, const WcarFilter* pFilter);
    WcarFilterSession(WcarFilterSession&&) = delete;

private:
    const WcarFilter& m_filter;

    QueryEvent m_query_event;
};
