/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorrouter.hh"
#include <fstream>
#include <iomanip>
#include <maxbase/format.hh>
#include <maxscale/mainworker.hh>
#include <maxscale/routingworker.hh>
#include <maxsql/mariadb_connector.hh>
#include "comparatorsession.hh"
#include "../../../core/internal/config_runtime.hh"
#include "../../../core/internal/service.hh"

using namespace mxs;
using std::chrono::duration_cast;


ComparatorRouter::ComparatorRouter(SERVICE* pService)
    : mxb::Worker::Callable(mxs::MainWorker::get())
    , m_config(pService->name(), this)
    , m_service(*pService)
    , m_stats(pService)
{
}

// static
const char* ComparatorRouter::to_string(ComparatorState comparator_state)
{
    switch (comparator_state)
    {
    case ComparatorState::PREPARED:
        return "prepared";

    case ComparatorState::SYNCHRONIZING:
        return "synchronizing";

    case ComparatorState::COMPARING:
        return "comparing";

    case ComparatorState::STOPPING:
        return "stopping";
    }

    mxb_assert(!true);
    return "unknown";
}

// static
const char* ComparatorRouter::to_string(SyncState sync_state)
{
    switch (sync_state)
    {
    case SyncState::NOT_APPLICABLE:
        return "not_applicable";

    case SyncState::STOPPING_REPLICATION:
        return "stopping_replication";

    case SyncState::SUSPENDING_SESSIONS:
        return "suspending_sessions";

    }

    mxb_assert(!true);
    return "unknown";
}

// static
ComparatorRouter* ComparatorRouter::create(SERVICE* pService)
{
    return new ComparatorRouter(pService);
}

RouterSession* ComparatorRouter::newSession(MXS_SESSION* pSession, const Endpoints& endpoints)
{
    const auto& children = m_service.get_children();

    if (std::find(children.begin(), children.end(), m_config.pMain) == children.end())
    {
        MXB_ERROR("Main target '%s' is not listed in `targets`", m_config.pMain->name());
        return nullptr;
    }

    auto [ sMain, backends ] = comparator::backends_from_endpoints(*m_config.pMain, endpoints, *this);
    bool connected = false;

    if (sMain->can_connect() && sMain->connect())
    {
        connected = true;

        for (const auto& sBackend : backends)
        {
            if (sBackend->can_connect())
            {
                // TODO: Ignore if it cannot connect?
                sBackend->connect();
            }
        }
    }

    RouterSession* pRouter_session = nullptr;

    if (connected)
    {
        pRouter_session = new ComparatorSession(pSession, this, std::move(sMain), std::move(backends));
    }

    return pRouter_session;
}

std::shared_ptr<ComparatorExporter> ComparatorRouter::exporter_for(const mxs::Target* pTarget) const
{
    // TODO: Remove this once the servers have been put into place before
    // TODO: post_configure() is called.
    const_cast<ComparatorRouter*>(this)->update_exporters();

    std::shared_lock<std::shared_mutex> guard(m_exporters_rwlock);

    auto it = m_exporters.find(pTarget);
    mxb_assert(it != m_exporters.end());

    return it->second;
}

json_t* ComparatorRouter::diagnostics() const
{
    return nullptr;
}

uint64_t ComparatorRouter::getCapabilities() const
{
    return COMPARATOR_CAPABILITIES;
}

bool ComparatorRouter::post_configure()
{
    m_stats.post_configure(m_config);

    return update_exporters();
}

bool ComparatorRouter::start(json_t** ppOutput)
{
    mxb_assert(MainWorker::is_current());

    if (m_comparator_state != ComparatorState::PREPARED)
    {
        MXB_ERROR("State of '%s' is '%s'. Can be started only when in state '%s'.",
                  m_service.name(), to_string(m_comparator_state), to_string(ComparatorState::PREPARED));
        return false;
    }

    set_state(ComparatorState::SYNCHRONIZING, SyncState::SUSPENDING_SESSIONS);

    RoutingWorker::SessionResult sr = suspend_sessions();

    MainWorker::get()->lcall([this, sr]() {
            setup(sr);

            if (m_comparator_state == ComparatorState::SYNCHRONIZING)
            {
                start_setup_dcall();
            }
        });

    get_status(sr, ppOutput);

    return true;
}

bool ComparatorRouter::status(json_t** ppOutput)
{
    RoutingWorker::SessionResult sr = suspended_sessions();

    get_status(sr, ppOutput);

    return true;
}

bool ComparatorRouter::stop(json_t** ppOutput)
{
    mxb_assert(MainWorker::is_current());

    bool rv = false;

    switch (m_comparator_state)
    {
    case ComparatorState::PREPARED:
        MXB_ERROR("The state of '%s' is '%s' and hence it cannot be stopped.",
                  m_service.name(), to_string(m_comparator_state));
        break;

    case ComparatorState::SYNCHRONIZING:
        mxb_assert(m_dcstart != 0);
        cancel_dcall(m_dcstart);
        m_dcstart = 0;

        resume_sessions();

        set_state(ComparatorState::PREPARED);
        rv = true;
        break;

    case ComparatorState::STOPPING:
        MXB_ERROR("'%s' is already being stopped.", m_service.name());
        break;

    case ComparatorState::COMPARING:
        {
            set_state(ComparatorState::STOPPING, SyncState::SUSPENDING_SESSIONS);

            RoutingWorker::SessionResult sr = suspend_sessions();

            MainWorker::get()->lcall([this, sr]() {
                    teardown(sr);

                    if (m_comparator_state == ComparatorState::STOPPING)
                    {
                        start_teardown_dcall();
                    }
                });

            get_status(sr, ppOutput);
            rv = true;
        }
    }

    return rv;
}

namespace
{

bool save_stats(const std::string& path, json_t* pOutput)
{
    std::ofstream out(path);

    if (out)
    {
        auto str = mxb::json_dump(pOutput, JSON_INDENT(2)) + '\n';

        out << str;

        if (!out)
        {
            MXB_ERROR("Could not write summary to file '%s'.", path.c_str());
        }
    }
    else
    {
        MXB_ERROR("Could not create file '%s'.", path.c_str());
    }

    return !out.fail();
}

}

bool ComparatorRouter::summary(Summary summary, json_t** ppOutput)
{
    bool rv = true;

    std::unique_lock<std::mutex> guard(m_stats_lock);
    Stats stats = m_stats;
    guard.unlock();

    std::string path = mxs::datadir();
    path += "/";
    path += MXB_MODULE_NAME;
    path += "/";
    path += m_config.pService->name();
    path += "/summary_";

    time_t now = time(nullptr);
    std::stringstream time;
    time << std::put_time(std::localtime(&now),"%Y-%m-%dT%H-%M-%S");
    path += time.str();
    path += ".json";

    json_t* pOutput = stats.to_json();

    if (summary == Summary::SAVE || summary == Summary::BOTH)
    {
        rv = save_stats(path, pOutput);
    }

    if (summary == Summary::RETURN)
    {
        *ppOutput = pOutput;
        rv = true;
    }
    else
    {
        json_decref(pOutput);
    }

    return rv;
}

void ComparatorRouter::collect(const ComparatorSessionStats& stats)
{
    std::lock_guard<std::mutex> guard(m_stats_lock);

    m_stats += stats;
}

void ComparatorRouter::set_state(ComparatorState comparator_state, SyncState sync_state)
{
    m_comparator_state = comparator_state;
    m_sync_state = sync_state;

#ifdef SS_DEBUG
    switch (m_comparator_state)
    {
    case ComparatorState::PREPARED:
    case ComparatorState::COMPARING:
        mxb_assert(m_sync_state == SyncState::NOT_APPLICABLE);
        break;

    case ComparatorState::SYNCHRONIZING:
        mxb_assert(m_sync_state != SyncState::NOT_APPLICABLE);
        break;

    case ComparatorState::STOPPING:
        mxb_assert(m_sync_state == SyncState::SUSPENDING_SESSIONS);
    }
#endif
}

void ComparatorRouter::set_sync_state(SyncState sync_state)
{
    m_sync_state = sync_state;

    mxb_assert(m_comparator_state == ComparatorState::SYNCHRONIZING
               && m_sync_state != SyncState::NOT_APPLICABLE);
}

mxs::RoutingWorker::SessionResult ComparatorRouter::restart_sessions()
{
    return mxs::RoutingWorker::restart_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult ComparatorRouter::suspend_sessions()
{
    return mxs::RoutingWorker::suspend_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult ComparatorRouter::resume_sessions()
{
    return mxs::RoutingWorker::resume_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult ComparatorRouter::suspended_sessions()
{
    return mxs::RoutingWorker::suspended_sessions(m_config.pService->name());
}

void ComparatorRouter::get_status(mxs::RoutingWorker::SessionResult sr, json_t** ppOutput)
{
    json_t* pOutput = json_object();
    json_object_set_new(pOutput, "state", json_string(to_string(m_comparator_state)));
    json_object_set_new(pOutput, "sync_state", json_string(to_string(m_sync_state)));
    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(sr.total));
    json_object_set_new(pSessions, "suspended", json_integer(sr.affected));
    json_object_set_new(pOutput, "sessions", pSessions);

    *ppOutput = pOutput;
}

bool ComparatorRouter::rewire_service(const std::set<std::string>& from_targets,
                                      const std::set<std::string>& to_targets)
{
    bool rv = false;

    Service* pService = static_cast<Service*>(m_config.pService);
    rv = runtime_unlink_service(pService, from_targets);

    if (rv)
    {
        rv = runtime_link_service(pService, to_targets);

        if (!rv)
        {
            MXB_ERROR("Could not link targets %s to service '%s'.",
                      mxb::join(to_targets, ",", "'").c_str(), pService->name());
        }
    }
    else
    {
        MXB_ERROR("Could not unlink targets %s from service '%s'.",
                  mxb::join(from_targets, ",", "'").c_str(), pService->name());
    }

    return rv;
}

bool ComparatorRouter::rewire_service_for_comparison()
{
    bool rv = false;

    std::set<std::string> from_targets { m_config.pMain->name() };
    std::set<std::string> to_targets { m_service.name() };

    rv = rewire_service(from_targets, to_targets);

    if (!rv)
    {
        MXB_ERROR("Could not rewire service '%s' for comparison.", m_config.pService->name());
    }

    return rv;
}

bool ComparatorRouter::rewire_service_for_normalcy()
{
    bool rv = false;

    std::set<std::string> from_targets { m_service.name() };
    std::set<std::string> to_targets { m_config.pMain->name() };

    rv = rewire_service(from_targets, to_targets);

    if (!rv)
    {
        MXB_ERROR("Could not rewire service '%s' for normalcy.", m_config.pService->name());
    }

    return rv;
}

bool ComparatorRouter::stop_replication(const SERVER& server)
{
    bool rv = false;

    mxq::MariaDB mdb;

    const auto& sConfig = m_config.pService->config();

    auto& settings = mdb.connection_settings();
    settings.user = sConfig->user;
    settings.password = sConfig->password;

    if (mdb.open(server.address(), server.port()))
    {
        if (mdb.cmd("STOP ALL SLAVES"))
        {
            rv = true;
        }
        else
        {
            MXB_ERROR("Could not stop replication on %s:%d, error: %s",
                      server.address(), server.port(), mdb.error());
        }
    }
    else
    {
        MXB_ERROR("Could not open connection to %s:%d, error: %s",
                  server.address(), server.port(), mdb.error());
    }

    return rv;
}

ComparatorRouter::ReplicationStatus ComparatorRouter::stop_replication()
{
    ReplicationStatus rv = ReplicationStatus::ERROR;

    std::vector<SERVER*> servers = m_service.reachable_servers();

    // TODO: Now assuming there must be exactly two.
    if (servers.size() == 2)
    {
        SERVER* pMain = servers.front();
        SERVER* pReplica = servers.back();

        if (pMain == m_config.pMain)
        {
            using GtiPosByDomain = std::unordered_map<uint32_t, uint64_t>;

            GtiPosByDomain from = pMain->get_gtid_list();
            GtiPosByDomain to = pReplica->get_gtid_list();

            bool behind = false;
            for (auto kv : from)
            {
                auto domain = kv.first;
                auto position = kv.second;

                auto it = to.find(domain);

                if (it == to.end())
                {
                    MXB_DEV("Replica '%s' lacks domain %u, which is found in '%s'.",
                            pReplica->name(), domain, pMain->name());
                    behind = true;
                }
                else
                {
                    if (it->second < position)
                    {
                        MXB_DEV("The position %lu of domain %u in server '%s' is behind "
                                "the position %lu in server '%s'.",
                                it->second, domain, pReplica->name(), position, pMain->name());
                        behind = true;
                    }
                }
            }

            if (!behind)
            {
                if (stop_replication(*pReplica))
                {
                    rv = ReplicationStatus::STOPPED;
                }
            }
            else
            {
                MXB_DEV("'%s' is behind '%s', not breaking replication yet.",
                        pReplica->name(), pMain->name());
                rv = ReplicationStatus::LAGGING;
            }
        }
        else
        {
            MXB_ERROR("First server of '%s' is '%s', although expected to be '%s'.",
                      m_service.name(), pMain->name(), m_config.pMain->name());
        }
    }
    else
    {
        MXB_ERROR("'%s' has currently %d reachable servers, while 2 is expected.",
                  m_service.name(), (int)servers.size());
    }

    return rv;
}

void ComparatorRouter::restart_and_resume()
{
    RoutingWorker::SessionResult sr = restart_sessions();

    if (sr.total != sr.affected)
    {
        MXB_WARNING("Could only restart %ld out of %ld sessions of service '%s'.",
                    sr.affected, sr.total, m_config.pService->name());
    }

    sr = resume_sessions();

    if (sr.total != sr.affected)
    {
        MXB_WARNING("%ld sessions of a total of %ld of service '%s' were not suspended "
                    "when the sessions again were resumed.",
                    sr.total - sr.affected, sr.total, m_config.pService->name());
    }
}

void ComparatorRouter::setup(const RoutingWorker::SessionResult& sr)
{
    if (all_sessions_suspended(sr))
    {
        set_sync_state(SyncState::STOPPING_REPLICATION);

        switch (stop_replication())
        {
        case ReplicationStatus::STOPPED:
            if (rewire_service_for_comparison())
            {
                restart_and_resume();
                set_state(ComparatorState::COMPARING);
            }
            else
            {
                // Not sure whether rewiring actually can fail, if the arguments are ok.

                MXB_ERROR("Could not rewire '%s' service for comparison of servers. "
                          "Now attempting to reset the configuration.",
                          m_config.pService->name());

                if (rewire_service_for_normalcy())
                {
                    MXB_NOTICE("Service '%s' reset to original configuration, resuming sessions.",
                               m_config.pService->name());

                    resume_sessions();
                    set_state(ComparatorState::PREPARED);
                }
                else
                {
                    MXB_ERROR("Could not reset configuration of service '%s', cannot resume "
                              "sessions. This will need manual intervention.",
                              m_config.pService->name());
                }

            }
            break;

        case ReplicationStatus::LAGGING:
            break;

        case ReplicationStatus::ERROR:
            MXB_ERROR("Could not stop replication, cannot rewire service '%s'. "
                      "Resuming sessions according to original configuration.",
                      m_config.pService->name());
            resume_sessions();
            set_state(ComparatorState::PREPARED);
            break;
        }
    }
}

bool ComparatorRouter::setup_dcall()
{
    RoutingWorker::SessionResult sr = suspend_sessions();

    setup(sr);

    bool call_again = (m_comparator_state == ComparatorState::SYNCHRONIZING);

    if (!call_again)
    {
        m_dcstart = 0;
    }

    return call_again;
}

void ComparatorRouter::start_setup_dcall()
{
    mxb_assert(m_dcstart == 0);

    m_dcstart = dcall(std::chrono::milliseconds { 1000 }, [this]() {
            return setup_dcall();
        });
}

void ComparatorRouter::teardown(const mxs::RoutingWorker::SessionResult& sr)
{
    if (all_sessions_suspended(sr))
    {
        if (rewire_service_for_normalcy())
        {
            restart_and_resume();
        }
        else
        {
            // TODO: An ERROR state is needed also.
            mxb_assert(!true);
        }

        set_state(ComparatorState::PREPARED);
    }
}

bool ComparatorRouter::teardown_dcall()
{
    RoutingWorker::SessionResult sr = suspend_sessions();

    teardown(sr);

    bool call_again = (m_comparator_state == ComparatorState::STOPPING);

    if (!call_again)
    {
        m_dcstart = 0;
    }

    return call_again;
}

void ComparatorRouter::start_teardown_dcall()
{
    mxb_assert(m_dcstart == 0);

    m_dcstart = dcall(std::chrono::milliseconds { 1000 }, [this]() {
            return teardown_dcall();
        });
}

bool ComparatorRouter::update_exporters()
{
    bool rv = true;

    std::shared_lock<std::shared_mutex> shared_guard(m_exporters_rwlock);

    std::map<const mxs::Target*, SExporter> exporters;

    for (const mxs::Target* pTarget : m_service.get_children())
    {
        if (pTarget != m_config.pMain)
        {
            auto it = m_exporters.find(pTarget);

            if (it != m_exporters.end())
            {
                exporters.insert(*it);
            }
            else
            {
                SExporter sExporter = build_exporter(m_config, *pTarget);

                if (sExporter)
                {
                    exporters.insert(std::make_pair(pTarget, sExporter));
                }
                else
                {
                    rv = false;
                    break;
                }
            }
        }
    }

    if (rv)
    {
        shared_guard.unlock();

        std::lock_guard<std::shared_mutex> guard(m_exporters_rwlock);

        m_exporters = std::move(exporters);
    }

    return rv;
}
