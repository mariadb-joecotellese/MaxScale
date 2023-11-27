/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratrouter.hh"
#include <maxbase/format.hh>
#include <maxscale/mainworker.hh>
#include <maxscale/routingworker.hh>
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
const char* UratRouter::to_string(UratState state)
{
    switch (state)
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

    bool rv = true;

    m_urat_state = UratState::SYNCHRONIZING;

    RoutingWorker::SuspendResult sr = suspend_sessions();

    if (all_sessions_suspended(sr))
    {
        rv = rewire_and_restart();
    }
    else
    {
        m_dcstart = dcall(std::chrono::milliseconds { 1000 }, [this]() {
                return rewire_service_dcall();
            });
    }

    if (rv)
    {
        get_status(sr, ppOutput);
    }

    return rv;
}

bool UratRouter::status(json_t** ppOutput)
{
    RoutingWorker::SuspendResult sr = suspended_sessions();

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

mxs::RoutingWorker::SuspendResult UratRouter::suspend_sessions()
{
    return mxs::RoutingWorker::suspend_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SuspendResult UratRouter::resume_sessions()
{
    return mxs::RoutingWorker::resume_sessions(m_config.pService->name());
}

mxs::RoutingWorker::SuspendResult UratRouter::suspended_sessions()
{
    return mxs::RoutingWorker::suspended_sessions(m_config.pService->name());
}

void UratRouter::get_status(mxs::RoutingWorker::SuspendResult sr, json_t** ppOutput)
{
    json_t* pOutput = json_object();
    json_object_set_new(pOutput, "state", json_string(to_string(m_urat_state)));
    json_t* pSessions = json_object();
    json_object_set_new(pSessions, "total", json_integer(sr.total));
    json_object_set_new(pSessions, "suspended", json_integer(sr.suspended));
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

bool UratRouter::rewire_service_dcall()
{
    mxb_assert(m_urat_state == UratState::SYNCHRONIZING);

    bool rv = true;

    RoutingWorker::SuspendResult sr = suspend_sessions();

    if (all_sessions_suspended(sr))
    {
        rewire_and_restart();

        // And the dcall cancelled.
        m_dcstart = 0;
        rv = false;
    }

    return rv;
}

bool UratRouter::rewire_and_restart()
{
    mxb_assert(m_urat_state == UratState::SYNCHRONIZING);

    if (rewire_service())
    {
        // TODO: Break replication and restart sessions.

        m_urat_state = UratState::CAPTURING;
    }
    else
    {
        MXB_ERROR("Could not rewire service, resuming the sessions with original setup.");
        m_urat_state = UratState::PREPARED;
    }

    // Regardless of anything, the sessions are resumed.
    resume_sessions();

    return m_urat_state == UratState::CAPTURING;
}

void UratRouter::ship(json_t* pJson)
{
    {
        std::shared_lock<mxb::shared_mutex> guard(m_rw_lock);
        m_sExporter->ship(pJson);
    }

    json_decref(pJson);
}
