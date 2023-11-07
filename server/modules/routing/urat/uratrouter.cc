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
    const auto& children = m_service.get_children();

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

uint64_t UratRouter::getCapabilities() const
{
    return urat::CAPABILITIES;
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

void UratRouter::ship(json_t* pJson)
{
    {
        std::shared_lock<mxb::shared_mutex> guard(m_rw_lock);
        m_sExporter->ship(pJson);
    }

    json_decref(pJson);
}
