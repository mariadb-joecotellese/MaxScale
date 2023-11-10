/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratcommands.hh"
#include <maxbase/string.hh>
#include <maxsql/mariadb_connector.hh>
#include <maxscale/json_api.hh>
#include <maxscale/modulecmd.hh>
#include <maxscale/utils.hh>
#include <maxscale/protocol/mariadb/maxscale.hh>

using namespace mxq;
using namespace std;

#define URAT_ERROR_JSON(ppJson, format, ...) \
    do { \
        MXB_ERROR(format, ##__VA_ARGS__); \
        if (ppJson) \
        { \
            *ppJson = mxs_json_error_append(*ppJson, format, ##__VA_ARGS__); \
        } \
    } while (false)

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
                    json_t* pOutput = json_object();
                    json_object_set_new(pOutput, "status", json_string("Server replicates from master."));
                    *ppOutput = pOutput;
                    rv = true;
                }
                else
                {
                    URAT_ERROR_JSON(ppOutput, "Server %s replicates from %s:%d and not from %s (%s:%d).",
                                    replica.name(),
                                    master_host.c_str(), master_port,
                                    primary.name(), primary.address(), primary.port());

                }
            }
            else
            {
                URAT_ERROR_JSON(ppOutput, "Server %s does not replicate from any server.",
                                replica.name());
            }
        }
    }
    else
    {
        URAT_ERROR_JSON(ppOutput, "Could not connect to server at %s:%d: %s",
                        replica.address(), replica.port(), mdb.error());
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
                rv = check_prepare_prerequisites(*pService, *pPrimary, *pReplica, ppOutput);
            }
            else
            {
                URAT_ERROR_JSON(ppOutput, "The immediate target of the service %s is not a server.",
                                pService->name());
            }
        }
        else
        {
            URAT_ERROR_JSON(ppOutput, "The service %s has more reachable servers than 1.",
                            pService->name());
        }
    }
    else
    {
        URAT_ERROR_JSON(ppOutput, "The service %s has more targets than 1.",
                        pService->name());
    }

    return rv;
}

}

void urat_register_commands()
{
    modulecmd_register_command(MXB_MODULE_NAME,
                               "prepare",
                               MODULECMD_TYPE_ACTIVE,
                               command_prepare,
                               MXS_ARRAY_NELEMS(command_prepare_argv),
                               command_prepare_argv,
                               "Prepare Urat for Service");
}
