/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratcommands.hh"
#include <set>
#include <vector>
#include <maxbase/string.hh>
#include <maxsql/mariadb_connector.hh>
#include <maxscale/config.hh>
#include <maxscale/json_api.hh>
#include <maxscale/modulecmd.hh>
#include <maxscale/utils.hh>
#include <maxscale/protocol/mariadb/maxscale.hh>
#include "../../../core/internal/config_runtime.hh"
#include "../../../core/internal/service.hh"

using namespace mxq;
using namespace std;

typedef std::set<std::string> StringSet;

namespace
{

inline void check_args(const MODULECMD_ARG* pArgs, modulecmd_arg_type_t argv[], int argc)
{
    mxb_assert(pArgs->argc == argc);

    for (int i = 0; i < argc; ++i)
    {
        mxb_assert(pArgs->argv[i].type.type == argv[i].type);
    }
}

}

/*
 * call command prepare
 */
namespace
{

static modulecmd_arg_type_t command_prepare_argv[] =
{
    {MODULECMD_ARG_SERVICE, "Service name"},
    {MODULECMD_ARG_SERVER,  "Server name"}
};

static int command_prepare_argc = MXS_ARRAY_NELEMS(command_prepare_argv);

bool check_prepare_prerequisites(const SERVICE& service,
                                 const SERVER& primary,
                                 const SERVER& replica,
                                 json_t** ppOutput)
{
    bool rv = false;

    MariaDB mdb;

    MariaDB::ConnectionSettings& settings = mdb.connection_settings();

    const auto& sConfig = service.config();
    settings.user = sConfig->user;
    settings.password = sConfig->password;

    if (mdb.open(replica.address(), replica.port()))
    {
        auto sResult = mdb.query("SHOW SLAVE STATUS");

        if (sResult)
        {
            if (sResult->get_col_count() != 0 && sResult->next_row())
            {
                auto master_host = sResult->get_string("Master_Host");
                int master_port = sResult->get_int("Master_Port");

                // TODO: One may be expressed using an IP and the other using a hostname.
                if (master_host == primary.address() && master_port == primary.port())
                {
                    // The server to test replicates from the server used, so all things green.
                    rv = true;
                }
                else
                {
                    MXB_ERROR("Server %s replicates from %s:%d and not from %s (%s:%d).",
                              replica.name(),
                              master_host.c_str(), master_port,
                              primary.name(), primary.address(), primary.port());

                }
            }
            else
            {
                MXB_ERROR("Server %s does not replicate from any server.", replica.name());
            }
        }
    }
    else
    {
        MXB_ERROR("Could not connect to server at %s:%d: %s",
                  replica.address(), replica.port(), mdb.error());
    }

    return rv;
}

Service* create_urat_service(const string& name,
                                 const SERVICE& service,
                                 const SERVER& primary,
                                 const SERVER& replica,
                                 json_t** ppOutput)
{
    mxs::ConfigParameters params;
    mxs::ConfigParameters unknown;

    auto& sValues = service.config();

    vector<string> servers { primary.name(), replica.name() };

    // TODO: 'exporter' and parameters dependent on its value, must be provided somehow.
    params.set("user", sValues->user);
    params.set("password", sValues->password);
    params.set("router", "urat");
    params.set("main", primary.name());
    params.set("exporter", "file");
    params.set("file", "urat.txt");
    params.set("servers", mxb::join(servers, ","));

    Service* pUrat_service = Service::create(name.c_str(), params);

    if (pUrat_service)
    {
        json_t* pOutput = json_object();
        json_object_set_new(pOutput, "status", json_string("Urat service created."));
        *ppOutput = pOutput;
    }
    else
    {
        MXB_ERROR("Could not create Urat service, please check earlier errors.");
    }

    return pUrat_service;
}

Service* create_urat_service(const SERVICE& service,
                             const SERVER& primary,
                             const SERVER& replica,
                             json_t** ppOutput)
{
    Service* pUrat_service = nullptr;

    string name { "Urat" };
    name += service.name();

    if (const char* zType = mxs::Config::get_object_type(name))
    {
        MXB_ERROR("Cannot create Urat service for the service '%s', a %s "
                  "with the name '%s' exists already.",
                  service.name(), zType, name.c_str());
    }
    else
    {
        pUrat_service = create_urat_service(name, service, primary, replica, ppOutput);
    }

    return pUrat_service;
}

bool rewire_service(Service& service, SERVER& server, const Service& urat_service)
{
    bool rv = false;

    std::set<std::string> servers { server.name() };

    rv = runtime_unlink_service(&service, servers);

    if (rv)
    {
        std::set<std::string> targets { urat_service.name() };

        rv = runtime_link_service(&service, targets);
    }

    return rv;
}

bool command_prepare(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    check_args(pArgs, command_prepare_argv, command_prepare_argc);

    bool rv = false;

    SERVICE* pService = pArgs->argv[0].value.service;
    SERVER* pReplica = pArgs->argv[1].value.server;

    vector<mxs::Target*> targets = pService->get_children();

    if (targets.size() == 1)
    {
        vector<SERVER*> servers = pService->reachable_servers();

        if (servers.size() == 1)
        {
            SERVER* pPrimary = servers.front();

            if (pPrimary == servers.front())
            {
                if (check_prepare_prerequisites(*pService, *pPrimary, *pReplica, ppOutput))
                {
                    Service* pUrat_service = create_urat_service(*pService, *pPrimary, *pReplica, ppOutput);

                    if (pUrat_service)
                    {
                        rv = rewire_service(*static_cast<Service*>(pService), *pPrimary, *pUrat_service);
                    }
                }
            }
            else
            {
                MXB_ERROR("The immediate target of the service %s is not a server.", pService->name());
            }
        }
        else
        {
            MXB_ERROR("The service %s has more reachable servers than 1.", pService->name());
        }
    }
    else
    {
        MXB_ERROR("The service %s has more targets than 1.", pService->name());
    }

    return rv;
}

}

void urat_register_commands()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "prepare",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_prepare,
                                                  MXS_ARRAY_NELEMS(command_prepare_argv),
                                                  command_prepare_argv,
                                                  "Prepare Urat for Service");
    mxb_assert(rv);
}
