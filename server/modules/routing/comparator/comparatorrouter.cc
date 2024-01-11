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
    , m_comparator_state(ComparatorState::PREPARED)
    , m_config(pService->name(), this)
    , m_service(*pService)
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

    case ComparatorState::CAPTURING:
        return "capturing";
    }

    mxb_assert(!true);
    return "unknown";
}

const char* ComparatorRouter::to_string(SyncState sync_state)
{
    switch (sync_state)
    {
    case SyncState::IDLE:
        return "idle";

    case SyncState::SUSPENDING:
        return "suspending";

    case SyncState::REWIRING:
        return "rewiring";

    case SyncState::STOPPING_REPLICATION:
        return "stopping_replication";

    case SyncState::RESTARTING_AND_RESUMING:
        return "restarting_and_resuming";
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
    bool rval = true;

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
                    rval = false;
                    break;
                }
            }
        }
    }

    if (rval)
    {
        shared_guard.unlock();

        std::lock_guard<std::shared_mutex> guard(m_exporters_rwlock);

        m_exporters = std::move(exporters);
    }

    return rval;
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

    m_comparator_state = ComparatorState::SYNCHRONIZING;
    m_sync_state = SyncState::SUSPENDING;

    RoutingWorker::SessionResult sr = suspend_sessions();

    MainWorker::get()->lcall([this, sr]() {
            if (all_sessions_suspended(sr))
            {
                m_sync_state = SyncState::REWIRING;

                synchronize();
            }
            else
            {
                start_synchronize_dcall();
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
    bool rv = false;

    json_t* pOutput = nullptr;

    switch (m_comparator_state)
    {
    case ComparatorState::PREPARED:
        MXB_ERROR("The state of '%s' is '%s' and hence it cannot be stopped.",
                  m_service.name(), to_string(m_comparator_state));
        break;

    case ComparatorState::SYNCHRONIZING:
        mxb_assert(false);
        // TODO: Handle stop when synchronizing.
        MXB_ERROR("Not implemented yet.");
        break;

    case ComparatorState::CAPTURING:
        mxb_assert(false);
        // TODO: Handle stop when capturing.
        MXB_ERROR("Not implemented yet.");
        break;
    }

    *ppOutput = pOutput;

    return rv;
}

namespace
{

json_t* generate_stats(int64_t nTotal, int64_t nCurrent, const ComparatorRouter::Stats& stats)
{
    json_t* pOutput = json_object();

    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(nTotal));
    json_object_set_new(pSessions, "current", json_integer(nCurrent));
    json_object_set_new(pOutput, "sessions", pSessions);

    json_t* pBackends = json_array();

    for (const auto& kv : stats.session_stats.other_stats)
    {
        json_t* pBackend = json_object();

        auto total_ms = duration_cast<std::chrono::milliseconds>(kv.second.total_duration);
        auto explain_ms = duration_cast<std::chrono::milliseconds>(kv.second.explain_duration);

        json_object_set_new(pBackend, "total_duration", json_integer(total_ms.count()));
        json_object_set_new(pBackend, "requests", json_integer(kv.second.nRequests));
        json_object_set_new(pBackend, "responding_requests", json_integer(kv.second.nResponding_requests));
        json_object_set_new(pBackend, "responses", json_integer(kv.second.nResponses));
        json_object_set_new(pBackend, "faster_requests", json_integer(kv.second.nFaster));
        json_object_set_new(pBackend, "slower_requests", json_integer(kv.second.nSlower));
        json_object_set_new(pBackend, "explain_duration", json_integer(explain_ms.count()));
        json_object_set_new(pBackend, "explain_requests", json_integer(kv.second.nExplain_requests));
        json_object_set_new(pBackend, "explain_responses", json_integer(kv.second.nExplain_responses));

        json_array_append_new(pBackends, pBackend);
    }

    json_object_set_new(pOutput, "backends", pBackends);

    return pOutput;
}

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

    json_t* pOutput = generate_stats(m_service.stats().n_total_conns(),
                                     m_service.stats().n_current_conns(),
                                     stats);

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

    m_stats.session_stats += stats;
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
    if (m_comparator_state == ComparatorState::SYNCHRONIZING)
    {
        json_object_set_new(pOutput, "sync_state", json_string(to_string(m_sync_state)));
    }
    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(sr.total));
    json_object_set_new(pSessions, "suspended", json_integer(sr.affected));
    json_object_set_new(pOutput, "sessions", pSessions);

    *ppOutput = pOutput;
}

bool ComparatorRouter::rewire_service()
{
    bool rv = false;

    std::set<std::string> servers { m_config.pMain->name() };

    Service* pService = static_cast<Service*>(m_config.pService);
    rv = runtime_unlink_service(pService, servers);

    if (rv)
    {
        std::set<std::string> targets { m_service.name() };

        rv = runtime_link_service(pService, targets);

        if (!rv)
        {
            MXB_ERROR("Could not link comparator service '%s' to service '%s'. Now restoring situation.",
                      m_service.name(), pService->name());

            targets.clear();
            targets.insert(m_config.name());

            if (!runtime_link_service(pService, targets))
            {
                MXB_ERROR("Could not link original server '%s' back to service '%s.",
                          m_config.pMain->name(), m_config.pService->name());
            }
        }
    }
    else
    {
        MXB_ERROR("Could not unlink server '%s' from service '%s'.",
                  m_config.pMain->name(), m_config.pService->name());
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

bool ComparatorRouter::synchronize()
{
    mxb_assert(m_comparator_state == ComparatorState::SYNCHRONIZING);

    bool rv = true;

    switch (m_sync_state)
    {
    case SyncState::IDLE:
        mxb_assert(!true);
        break;

    case SyncState::SUSPENDING:
        sync_suspend();
        break;

    case SyncState::REWIRING:
        sync_rewire();
        break;

    case SyncState::STOPPING_REPLICATION:
        sync_stop_replication();
        break;

    case SyncState::RESTARTING_AND_RESUMING:
        sync_restart_and_resume();
        break;
    };

    if (m_comparator_state == ComparatorState::SYNCHRONIZING)
    {
        mxb_assert(m_sync_state != SyncState::IDLE);

        if (m_dcstart == 0) // Will be 0, after the first lcall.
        {
            start_synchronize_dcall();
        }
    }
    else
    {
        m_dcstart = 0;
        rv = false;
    }

    return rv;
}

void ComparatorRouter::sync_suspend()
{
    mxb_assert(m_sync_state == SyncState::SUSPENDING);

    RoutingWorker::SessionResult sr = suspend_sessions();

    if (all_sessions_suspended(sr))
    {
        m_sync_state = SyncState::REWIRING;
        synchronize();
    }
}

void ComparatorRouter::sync_rewire()
{
    mxb_assert(m_sync_state == SyncState::REWIRING);

    if (rewire_service())
    {
        m_sync_state = SyncState::STOPPING_REPLICATION;
        synchronize();
    }
    else
    {
        m_sync_state = SyncState::IDLE;
        m_comparator_state = ComparatorState::PREPARED;
    }
}

void ComparatorRouter::sync_stop_replication()
{
    mxb_assert(m_sync_state == SyncState::STOPPING_REPLICATION);

    bool ok = true;

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
                    m_sync_state = SyncState::RESTARTING_AND_RESUMING;
                    synchronize();
                }
            }
            else
            {
                MXB_DEV("'%s' is behind '%s', not breaking replication yet.",
                        pReplica->name(), pMain->name());
            }
        }
        else
        {
            MXB_ERROR("First server of '%s' is '%s', although expected to be '%s'.",
                      m_service.name(), pMain->name(), m_config.pMain->name());
            ok = false;
        }
    }
    else
    {
        MXB_ERROR("'%s' has currently %d reachable servers, while 2 is expected.",
                  m_service.name(), (int)servers.size());
        ok = false;
    }

    if (!ok)
    {
        m_sync_state = SyncState::IDLE;
        m_comparator_state = ComparatorState::PREPARED;
    }
}

void ComparatorRouter::sync_restart_and_resume()
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

    m_sync_state = SyncState::IDLE;
    m_comparator_state = ComparatorState::CAPTURING;
}

void ComparatorRouter::start_synchronize_dcall()
{
    mxb_assert(m_dcstart == 0);

    m_dcstart = dcall(std::chrono::milliseconds { 1000 }, [this]() {
            return synchronize();
        });
}
