/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"

#include <maxbase/shared_mutex.hh>
#include <maxbase/worker.hh>
#include <maxscale/router.hh>
#include <maxscale/backend.hh>
#include <maxscale/protocol/mariadb/module_names.hh>
#include <maxscale/service.hh>

#include "uratconfig.hh"
#include "uratexporter.hh"

class UratSession;

class UratRouter : public mxs::Router
                 , private mxb::Worker::Callable
{
public:
    enum class UratState
    {
        PREPARED,      // Setup for action.
        SYNCHRONIZING, // Started, suspending sessions, stopping replication, etc.
        CAPTURING      // Sessions restarted, capturing in process.
    };

    static const char* to_string(UratState state);

    UratRouter(const UratRouter&) = delete;
    UratRouter& operator=(const UratRouter&) = delete;

    ~UratRouter() = default;
    static UratRouter*  create(SERVICE* pService);
    mxs::RouterSession* newSession(MXS_SESSION* pSession, const mxs::Endpoints& endpoints) override;
    json_t*             diagnostics() const override;
    uint64_t            getCapabilities() const override;

    void ship(json_t* obj);

    mxs::Target* get_main() const
    {
        return m_config.pMain;
    }

    mxs::config::Configuration& getConfiguration() override
    {
        return m_config;
    }

    std::set<std::string> protocols() const override
    {
        return {MXS_MARIADB_PROTOCOL_NAME};
    }

    const UratConfig& config() const
    {
        return m_config;
    }

    bool post_configure();

    bool start(json_t** ppOutput);
    bool status(json_t** ppOutput);
    bool stop(json_t** ppOutput);

private:
    bool all_sessions_suspended(mxs::RoutingWorker::SuspendResult sr)
    {
        return sr.total == sr.suspended;
    }

    void get_status(mxs::RoutingWorker::SuspendResult sr, json_t** ppOutput);

    bool rewire_service();
    bool rewire_service_dcall();
    bool rewire_and_restart();

    UratRouter(SERVICE* pService);

    UratState                     m_urat_state;
    UratConfig                    m_config;
    std::unique_ptr<UratExporter> m_sExporter;
    mxb::shared_mutex             m_rw_lock;
    SERVICE&                      m_service;
    mxb::Worker::DCId             m_dcstart { mxb::Worker::NO_CALL };
};
