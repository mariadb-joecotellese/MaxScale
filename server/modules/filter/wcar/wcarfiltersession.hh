/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcardefs.hh"
#include <maxscale/filter.hh>

class WcarFilter;

class WcarFilterSession : public maxscale::FilterSession
{
public:
    ~WcarFilterSession();

    static WcarFilterSession* create(MXS_SESSION* pSession, SERVICE* pService, const WcarFilter* pFilter);

private:
    WcarFilterSession(MXS_SESSION* pSession, SERVICE* pService, const WcarFilter* pFilter);

    WcarFilterSession(const WcarFilterSession&);
    WcarFilterSession& operator=(const WcarFilterSession&);

private:
    const WcarFilter& m_filter;
};
