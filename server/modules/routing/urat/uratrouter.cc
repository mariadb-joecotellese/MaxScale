/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratrouter.hh"
#include "uratsession.hh"

// static
UratRouter* UratRouter::create(SERVICE* pService)
{
    return new UratRouter(pService);
}

mxs::RouterSession* UratRouter::newSession(MXS_SESSION* pSession, const mxs::Endpoints& endpoints)
{
    const auto& children = m_service->get_children();

    if (std::find(children.begin(), children.end(), m_config.main) == children.end())
    {
        MXB_ERROR("Main target '%s' is not listed in `targets`", m_config.main->name());
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

const uint64_t caps = RCAP_TYPE_REQUEST_TRACKING | RCAP_TYPE_RUNTIME_CONFIG;

uint64_t UratRouter::getCapabilities() const
{
    return caps;
}

bool UratRouter::post_configure()
{
    bool rval = false;
    std::lock_guard<mxb::shared_mutex> guard(m_rw_lock);

    if (auto exporter = build_exporter(m_config))
    {
        m_exporter = std::move(exporter);
        rval = true;
    }

    return rval;
}

void UratRouter::ship(json_t* obj)
{
    {
        std::shared_lock<mxb::shared_mutex> guard(m_rw_lock);
        m_exporter->ship(obj);
    }

    json_decref(obj);
}

extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    const char* desc = "Mirrors SQL statements to multiple targets";

    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        "urat",
        mxs::ModuleType::ROUTER,
        mxs::ModuleStatus::ALPHA,
        MXS_ROUTER_VERSION,
        desc,
        "V1.0.0",
        caps,
        &mxs::RouterApi<UratRouter>::s_api,
        NULL,
        NULL,
        NULL,
        NULL,
        UratConfig::spec()
    };

    return &info;
}
