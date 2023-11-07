/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"

#include <maxscale/router.hh>
#include <maxscale/backend.hh>
#include <maxscale/protocol/mariadb/module_names.hh>
#include <maxbase/shared_mutex.hh>
#include <maxscale/service.hh>

#include "uratconfig.hh"
#include "uratexporter.hh"

class UratSession;

class UratRouter : public mxs::Router
{
public:
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
        return m_config.main;
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

private:
    UratRouter(SERVICE* pService)
        : m_config(pService->name(), this)
        , m_service(*pService)
    {
    }

    UratConfig                    m_config;
    std::unique_ptr<UratExporter> m_sExporter;
    mxb::shared_mutex             m_rw_lock;
    SERVICE&                      m_service;
};
