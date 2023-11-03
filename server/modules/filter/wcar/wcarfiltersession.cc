/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarfiltersession.hh"

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
