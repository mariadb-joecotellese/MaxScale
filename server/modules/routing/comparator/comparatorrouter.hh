/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <shared_mutex>
#include <maxbase/worker.hh>
#include <maxscale/router.hh>
#include <maxscale/backend.hh>
#include <maxscale/protocol/mariadb/module_names.hh>
#include <maxscale/service.hh>
#include "comparatorconfig.hh"
#include "comparatorexporter.hh"
#include "comparatorstats.hh"

class ComparatorSession;

class ComparatorRouter : public mxs::Router
                       , private mxb::Worker::Callable
{
public:
    using Stats = ComparatorRouterStats;

    enum class ComparatorState
    {
        PREPARED,      // Setup for action.
        SYNCHRONIZING, // Started, suspending sessions, stopping replication, etc.
        CAPTURING,     // Sessions restarted, capturing in process.
        STOPPING       // Stopping
    };

    static const char* to_string(ComparatorState comparator_state);

    ComparatorRouter(const ComparatorRouter&) = delete;
    ComparatorRouter& operator=(const ComparatorRouter&) = delete;

    ~ComparatorRouter() = default;
    static ComparatorRouter*  create(SERVICE* pService);
    mxs::RouterSession* newSession(MXS_SESSION* pSession, const mxs::Endpoints& endpoints) override;
    json_t*             diagnostics() const override;
    uint64_t            getCapabilities() const override;

    std::shared_ptr<ComparatorExporter> exporter_for(const mxs::Target* pTarget) const;

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

    const ComparatorConfig& config() const
    {
        return m_config;
    }

    bool post_configure();

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

    void collect(const ComparatorSessionStats& stats);

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
    bool dewire_service();
    bool stop_replication(const SERVER& server);

    bool synchronize_dcall();
    void synchronize(const mxs::RoutingWorker::SessionResult& sr);
    bool sync_stop_replication();
    void restart_and_resume();
    void start_synchronize_dcall();

    bool decapture_dcall();
    void decapture(const mxs::RoutingWorker::SessionResult& sr);
    void start_decapture_dcall();

    ComparatorRouter(SERVICE* pService);

    using SExporter = std::shared_ptr<ComparatorExporter>;

    ComparatorState                         m_comparator_state { ComparatorState::PREPARED };
    ComparatorConfig                        m_config;
    SERVICE&                                m_service;
    mxb::Worker::DCId                       m_dcstart { mxb::Worker::NO_CALL };
    std::map<const mxs::Target*, SExporter> m_exporters;
    mutable std::shared_mutex               m_exporters_rwlock;
    Stats                                   m_stats;
    std::mutex                              m_stats_lock;
};
