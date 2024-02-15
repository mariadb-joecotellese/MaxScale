/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <shared_mutex>
#include <maxbase/worker.hh>
#include <maxscale/router.hh>
#include <maxscale/backend.hh>
#include <maxscale/protocol/mariadb/module_names.hh>
#include <maxscale/service.hh>
#include "diffconfig.hh"
#include "diffexporter.hh"
#include "diffregistry.hh"
#include "diffstats.hh"

class DiffRouterSession;

class DiffRouter : public mxs::Router
                 , private mxb::Worker::Callable
{
public:
    using Stats = DiffRouterStats;

    enum class DiffState
    {
        PREPARED,      // Setup for action.
        SYNCHRONIZING, // Started, suspending sessions, stopping replication, etc.
        COMPARING,     // Sessions restarted, comparing in process.
        STOPPING       // Stopping
    };

    enum class SyncState
    {
        NOT_APPLICABLE,       // The diff state is not SYNCHRONIZING.
        STOPPING_REPLICATION, // The replication is being stopped. Mat be delayed due to lag.
        SUSPENDING_SESSIONS   // The sessions are being suspended.
    };

    static const char* to_string(DiffState diff_state);
    static const char* to_string(SyncState sync_state);

    DiffRouter(const DiffRouter&) = delete;
    DiffRouter& operator=(const DiffRouter&) = delete;

    ~DiffRouter();
    static DiffRouter*  create(SERVICE* pService);
    mxs::RouterSession* newSession(MXS_SESSION* pSession, const mxs::Endpoints& endpoints) override;
    json_t*             diagnostics() const override;
    uint64_t            getCapabilities() const override;

    std::shared_ptr<DiffExporter> exporter_for(const mxs::Target* pTarget) const;

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

    const DiffConfig& config() const
    {
        return m_config;
    }

    bool post_configure();

    bool check_configuration();

    enum class Summary
    {
        RETURN,
        SAVE,
        BOTH
    };

    bool start(json_t** ppOutput);
    bool status(json_t** ppOutput);
    bool stop(json_t** ppOutput);
    bool summary(Summary summary, json_t** ppOutput);

    void collect(const DiffRouterSessionStats& stats);

    DiffRegistry& registry()
    {
        return m_registry;
    }

private:
    void set_state(DiffState diff_state,
                   SyncState sync_state = SyncState::NOT_APPLICABLE);
    void set_sync_state(SyncState sync_state);

    bool all_sessions_suspended(mxs::RoutingWorker::SessionResult sr)
    {
        return sr.total == sr.affected;
    }

    mxs::RoutingWorker::SessionResult restart_sessions();
    mxs::RoutingWorker::SessionResult suspend_sessions();
    mxs::RoutingWorker::SessionResult resume_sessions();
    mxs::RoutingWorker::SessionResult suspended_sessions();

    void get_status(mxs::RoutingWorker::SessionResult sr, json_t** ppOutput);

    bool rewire_service(const std::set<std::string>& from_targets, const std::set<std::string>& to_targets);
    bool rewire_service_for_comparison();
    bool rewire_service_for_normalcy();

    enum class ReplicationMode
    {
        RESET_AND_START,
        START_ONLY
    };

    bool start_replication(const SERVER& server, ReplicationMode mode);
    void start_replication(ReplicationMode mode);
    void start_replication();
    void reset_replication();

    enum class ReplicationState
    {
        READY,   // Replication has been stopped or it did not need to be stopped.
        LAGGING, // Replication not stopped, as replica still lags behind.
        ERROR,   // Either the replica cannot be connected to, or the stopping failed.
    };

    bool stop_replication(const SERVER& server);
    ReplicationState stop_replication();

    void restart_and_resume();

    void setup(const mxs::RoutingWorker::SessionResult& sr);
    bool setup_dcall();
    void start_setup_dcall();

    void teardown(const mxs::RoutingWorker::SessionResult& sr);
    bool teardown_dcall();
    void start_teardown_dcall();

    bool update_exporters();

    bool collect_servers_to_be_stopped();

    DiffRouter(SERVICE* pService);

    using SExporter = std::shared_ptr<DiffExporter>;

    DiffState                               m_diff_state { DiffState::PREPARED };
    SyncState                               m_sync_state { SyncState::NOT_APPLICABLE };
    DiffConfig                              m_config;
    SERVICE&                                m_service;
    mxb::Worker::DCId                       m_dcstart { mxb::Worker::NO_CALL };
    std::map<const mxs::Target*, SExporter> m_exporters;
    mutable std::shared_mutex               m_exporters_rwlock;
    Stats                                   m_stats;
    std::mutex                              m_stats_lock;
    DiffRegistry                            m_registry;
    std::vector<SERVER*>                    m_stop_replication;
    std::vector<SERVER*>                    m_start_replication;
};
