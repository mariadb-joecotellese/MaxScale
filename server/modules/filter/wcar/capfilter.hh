/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capdefs.hh"
#include "caprecorder.hh"
#include "capstorage.hh"
#include <maxscale/filter.hh>
#include <maxscale/routing.hh>
#include "capconfig.hh"
#include "caprecorder.hh"
#include "capfiltersession.hh"

class CapFilterSession;

class CapFilter : public mxs::Filter
{
public:
    // TODO: This probably needs tuning.
    static constexpr uint64_t CAPABILITIES = RCAP_TYPE_REQUEST_TRACKING;

    CapFilter(CapFilter&&) = delete;

    static CapFilter* create(const char* zName);

    std::shared_ptr<mxs::FilterSession> newSession(MXS_SESSION* pSession, SERVICE* pService) override;

    json_t* diagnostics() const override;

    uint64_t getCapabilities() const override
    {
        return CAPABILITIES;
    }

    CapConfig& getConfiguration() override
    {
        return m_config;
    }

    std::set<std::string> protocols() const override
    {
        return {MXS_ANY_PROTOCOL};
    }

    CapRecorder& recorder() const
    {
        return *m_sRecorder;
    }

    int64_t get_next_event_id() const;

private:
    CapFilter(const std::string& name);
    bool post_configure();
    ~CapFilter();

private:
    CapConfig                    m_config;
    std::unique_ptr<Storage>     m_sStorage;
    std::unique_ptr<CapRecorder> m_sRecorder;
    mutable std::atomic<int64_t> m_event_id{1};
};
