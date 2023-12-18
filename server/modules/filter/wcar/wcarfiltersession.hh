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
    /**
     * @brief generate_canonical_for - Fill *pQuery_event with canonical and args
     *                                 for a non-sql buffer, if possible.
     * @param buffer
     * @param query_event
     * @return true if the event should be captured
     */
    bool generate_canonical_for(const GWBUF& buffer, QueryEvent* pQuery_event);

    const WcarFilter& m_filter;

    // TODO take into account a streaming client (writes without waits)
    bool       m_capture = false;
    QueryEvent m_query_event;
};
