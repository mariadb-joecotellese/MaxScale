/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarfilter.hh"
#include <string>


WcarFilter::WcarFilter(const std::string& name)
    : m_config(name)
{
    m_recorder.start();
}

WcarFilter::~WcarFilter()
{
    m_recorder.stop();
}


// static
WcarFilter* WcarFilter::create(const char* zName)
{
    return new WcarFilter(zName);
}

WcarFilterSession* WcarFilter::newSession(MXS_SESSION* pSession, SERVICE* pService)
{
    return WcarFilterSession::create(pSession, pService, this);
}

json_t* WcarFilter::diagnostics() const
{
    return m_config.to_json();
}
