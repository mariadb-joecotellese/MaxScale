/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"

#include <maxbase/shared_mutex.hh>
#include <maxbase/worker.hh>
#include <maxscale/router.hh>
#include <maxscale/backend.hh>
#include <maxscale/protocol/mariadb/module_names.hh>
#include <maxscale/service.hh>

#include "comparatorconfig.hh"
#include "comparatorexporter.hh"

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

    enum class SyncState
    {
        IDLE,                   // No synchronization in progress.
        SUSPENDING,             // Sessions are being suspended.
        REWIRING,               // Service is being rewired.
        STOPPING_REPLICATION,   // Replication is being stopped.
        RESTARTING_AND_RESUMING // Sessions are being restarted and resumed.
    };

    static const char* to_string(UratState urat_state);
    static const char* to_string(SyncState sync_state);

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
    bool all_sessions_suspended(mxs::RoutingWorker::SessionResult sr)
    {
        return sr.total == sr.affected;
    }

    mxs::RoutingWorker::SessionResult restart_sessions();
    mxs::RoutingWorker::SessionResult suspend_sessions();
    mxs::RoutingWorker::SessionResult resume_sessions();
    mxs::RoutingWorker::SessionResult suspended_sessions();

    void get_status(mxs::RoutingWorker::SessionResult sr, json_t** ppOutput);

    bool rewire_service();
    bool stop_replication(const SERVER& server);

    bool synchronize();
    void sync_suspend();
    void sync_rewire();
    void sync_stop_replication();
    void sync_restart_and_resume();

    void start_synchronize_dcall();

    UratRouter(SERVICE* pService);

    UratState                     m_urat_state { UratState::PREPARED };
    SyncState                     m_sync_state { SyncState::IDLE };
    UratConfig                    m_config;
    std::unique_ptr<UratExporter> m_sExporter;
    mxb::shared_mutex             m_rw_lock;
    SERVICE&                      m_service;
    mxb::Worker::DCId             m_dcstart { mxb::Worker::NO_CALL };
};
