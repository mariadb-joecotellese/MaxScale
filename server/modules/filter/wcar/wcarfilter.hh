/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcardefs.hh"
#include <maxscale/filter.hh>
#include <maxscale/routing.hh>
#include "wcarconfig.hh"
#include "wcarrecorder.hh"
#include "wcarfiltersession.hh"

class WcarFilterSession;

class WcarFilter : public mxs::Filter
{
public:
    // TODO: This probably needs tuning.
    static constexpr uint64_t CAPABILITIES = RCAP_TYPE_REQUEST_TRACKING;

    WcarFilter(WcarFilter&&) = delete;

    static WcarFilter* create(const char* zName);

    WcarFilterSession* newSession(MXS_SESSION* pSession, SERVICE* pService) override;

    json_t* diagnostics() const override;

    uint64_t getCapabilities() const override
    {
        return CAPABILITIES;
    }

    WcarConfig& getConfiguration() override
    {
        return m_config;
    }

    std::set<std::string> protocols() const override
    {
        return {MXS_ANY_PROTOCOL};
    }

    WcarRecorder& recorder() const
    {
        return m_recorder;
    }

private:
    WcarFilter(const std::string& name);
    ~WcarFilter();

private:
    WcarConfig           m_config;
    mutable WcarRecorder m_recorder;
};
