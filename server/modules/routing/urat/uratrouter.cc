/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratrouter.hh"
#include <maxbase/format.hh>
#include <maxscale/mainworker.hh>
#include <maxscale/routingworker.hh>
#include <maxsql/mariadb_connector.hh>
#include "uratsession.hh"
#include "../../../core/internal/config_runtime.hh"
#include "../../../core/internal/service.hh"

using namespace mxs;


UratRouter::UratRouter(SERVICE* pService)
    : mxb::Worker::Callable(mxs::MainWorker::get())
    , m_urat_state(UratState::PREPARED)
    , m_config(pService->name(), this)
    , m_service(*pService)
{
}

// static
const char* UratRouter::to_string(UratState urat_state)
{
    switch (urat_state)
    {
    case UratState::PREPARED:
        return "prepared";

    case UratState::SYNCHRONIZING:
        return "synchronizing";

    case UratState::CAPTURING:
        return "capturing";
    }

    mxb_assert(!true);
    return "unknown";
}

const char* UratRouter::to_string(SyncState sync_state)
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
UratRouter* UratRouter::create(SERVICE* pService)
{
    return new UratRouter(pService);
}

RouterSession* UratRouter::newSession(MXS_SESSION* pSession, const Endpoints& endpoints)
{
    const auto& children = m_service.get_children();

    if (std::find(children.begin(), children.end(), m_config.pMain) == children.end())
    {
        MXB_ERROR("Main target '%s' is not listed in `targets`", m_config.pMain->name());
        return nullptr;
    }

    auto backends = UratBackend::from_endpoints(endpoints);
    bool connected = false;

    for (const auto& a : backends)
    {
        if (a->can_connect() && a->connect())
        {
            connected = true;
        }
    }

    return connected ? new UratSession(pSession, this, std::move(backends)) : NULL;
}

json_t* UratRouter::diagnostics() const
{
    return nullptr;
}

uint64_t UratRouter::getCapabilities() const
{
    return URAT_CAPABILITIES;
}

bool UratRouter::post_configure()
{
    bool rval = false;
    std::lock_guard<mxb::shared_mutex> guard(m_rw_lock);

    if (auto sExporter = build_exporter(m_config))
    {
        m_sExporter = std::move(sExporter);
        rval = true;
    }

    return rval;
}

bool UratRouter::start(json_t** ppOutput)
{
    mxb_assert(MainWorker::is_current());

    if (m_urat_state != UratState::PREPARED)
    {
        MXB_ERROR("State of '%s' is '%s'. Can be started only when in state '%s'.",
                  m_service.name(), to_string(m_urat_state), to_string(UratState::PREPARED));
        return false;
    }

    m_urat_state = UratState::SYNCHRONIZING;
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

bool UratRouter::status(json_t** ppOutput)
{
    RoutingWorker::SessionResult sr = suspended_sessions();

    get_status(sr, ppOutput);

    return true;
}

bool UratRouter::stop(json_t** ppOutput)
{
    bool rv = false;

    json_t* pOutput = nullptr;

    switch (m_urat_state)
    {
    case UratState::PREPARED:
        MXB_ERROR("The state of '%s' is '%s' and hence it cannot be stopped.",
                  m_service.name(), to_string(m_urat_state));
        break;

    case UratState::SYNCHRONIZING:
        mxb_assert(false);
        // TODO: Handle stop when synchronizing.
        MXB_ERROR("Not implemented yet.");
        break;

    case UratState::CAPTURING:
        mxb_assert(false);
        // TODO: Handle stop when capturing.
        MXB_ERROR("Not implemented yet.");
        break;
    }

    *ppOutput = pOutput;

    return rv;
}

mxs::RoutingWorker::SessionResult UratRouter::restart_sessions()
{
    return mxs::RoutingWorker::restart_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult UratRouter::suspend_sessions()
{
    return mxs::RoutingWorker::suspend_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult UratRouter::resume_sessions()
{
    return mxs::RoutingWorker::resume_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SessionResult UratRouter::suspended_sessions()
{
    return mxs::RoutingWorker::suspended_sessions(m_config.pService->name());
}

void UratRouter::get_status(mxs::RoutingWorker::SessionResult sr, json_t** ppOutput)
{
    json_t* pOutput = json_object();
    json_object_set_new(pOutput, "state", json_string(to_string(m_urat_state)));
    if (m_urat_state == UratState::SYNCHRONIZING)
    {
        json_object_set_new(pOutput, "sync_state", json_string(to_string(m_sync_state)));
    }
    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(sr.total));
    json_object_set_new(pSessions, "suspended", json_integer(sr.affected));
    json_object_set_new(pOutput, "sessions", pSessions);

    *ppOutput = pOutput;
}

bool UratRouter::rewire_service()
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
            MXB_ERROR("Could not link urat service '%s' to service '%s'. Now restoring situation.",
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

bool UratRouter::stop_replication(const SERVER& server)
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

bool UratRouter::synchronize()
{
    mxb_assert(m_urat_state == UratState::SYNCHRONIZING);

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

    if (m_urat_state == UratState::SYNCHRONIZING)
    {
        mxb_assert(m_sync_state != SyncState::IDLE);

        if (m_dcstart == 0) // Will be 0, after the first lcall.
        {
            start_synchronize_dcall();
        }
    }
    else
    {
        if (m_dcstart)
        {
            cancel_dcall(m_dcstart);
        }

        rv = false;
    }

    return rv;
}

void UratRouter::sync_suspend()
{
    mxb_assert(m_sync_state == SyncState::SUSPENDING);

    RoutingWorker::SessionResult sr = suspend_sessions();

    if (all_sessions_suspended(sr))
    {
        m_sync_state = SyncState::REWIRING;
        synchronize();
    }
}

void UratRouter::sync_rewire()
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
        m_urat_state = UratState::PREPARED;
    }
}

void UratRouter::sync_stop_replication()
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
        m_urat_state = UratState::PREPARED;
    }
}

void UratRouter::sync_restart_and_resume()
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
    m_urat_state = UratState::CAPTURING;

    cancel_dcall(m_dcstart);
    m_dcstart = 0;
}

void UratRouter::start_synchronize_dcall()
{
    mxb_assert(m_dcstart == 0);

    m_dcstart = dcall(std::chrono::milliseconds { 1000 }, [this]() {
            bool call_again = synchronize();

            if (!call_again)
            {
                m_dcstart = 0;
            }

            return call_again;
        });
}

void UratRouter::ship(json_t* pJson)
{
    {
        std::shared_lock<mxb::shared_mutex> guard(m_rw_lock);
        m_sExporter->ship(pJson);
    }

    json_decref(pJson);
}
