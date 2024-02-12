/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffcommands.hh"
#include <set>
#include <vector>
#include <maxbase/format.hh>
#include <maxbase/string.hh>
#include <maxsql/mariadb_connector.hh>
#include <maxscale/cn_strings.hh>
#include <maxscale/config.hh>
#include <maxscale/json_api.hh>
#include <maxscale/modulecmd.hh>
#include <maxscale/utils.hh>
#include <maxscale/protocol/mariadb/maxscale.hh>
#include "../../../core/internal/config.hh"
#include "../../../core/internal/config_runtime.hh"
#include "../../../core/internal/monitormanager.hh"
#include "../../../core/internal/service.hh"
#include "diffrouter.hh"

using namespace mxq;
using namespace std;

typedef std::set<std::string> StringSet;

namespace
{
void register_prepare_command();
void register_start_command();
void register_status_command();
void register_stop_command();
void register_summary_command();
void register_unprepare_command();
}

void comparator_register_commands()
{
    register_prepare_command();
    register_start_command();
    register_status_command();
    register_stop_command();
    register_summary_command();
    register_unprepare_command();
}

/*
 * call command prepare
 */
namespace
{

static modulecmd_arg_type_t command_prepare_argv[] =
{
    {MODULECMD_ARG_SERVICE, "Service name"},
    {MODULECMD_ARG_SERVER,  "Main server name"},
    {MODULECMD_ARG_SERVER,  "Other server name"},
    {MODULECMD_ARG_STRING,  "'read_only' or 'read_write'"}
};

static int command_prepare_argc = MXS_ARRAY_NELEMS(command_prepare_argv);

bool check_prepare_prerequisites(const SERVICE& service,
                                 const SERVER& main,
                                 const SERVER& other)
{
    bool rv = false;

    MariaDB mdb;

    MariaDB::ConnectionSettings& settings = mdb.connection_settings();

    const auto& sConfig = service.config();
    settings.user = sConfig->user;
    settings.password = sConfig->password;

    if (mdb.open(other.address(), other.port()))
    {
        auto sResult = mdb.query("SHOW SLAVE STATUS");

        if (sResult)
        {
            if (sResult->get_col_count() != 0 && sResult->next_row())
            {
                auto master_host = sResult->get_string("Master_Host");
                int master_port = sResult->get_int("Master_Port");

                // TODO: One may be expressed using an IP and the other using a hostname.
                if (master_host == main.address() && master_port == main.port())
                {
                    auto slave_io_state = sResult->get_string("Slave_IO_State");

                    if (!slave_io_state.empty())
                    {
                        // The server to test replicates from the server used, so all things green.
                        rv = true;
                    }
                    else
                    {
                        MXB_ERROR("Server '%s' is configured to replicate from %s:%d, "
                                  "but is currently not replicating.",
                                  other.name(), master_host.c_str(), master_port);
                    }
                }
                else
                {
                    MXB_ERROR("Server '%s' replicates from %s:%d and not from '%s' (%s:%d).",
                              other.name(),
                              master_host.c_str(), master_port,
                              main.name(), main.address(), main.port());

                }
            }
            else
            {
                MXB_ERROR("Server %s does not replicate from any server.", other.name());
            }
        }
    }
    else
    {
        MXB_ERROR("Could not connect to server at %s:%d: %s",
                  other.address(), other.port(), mdb.error());
    }

    return rv;
}

Service* create_comparator_service(const string& name,
                                   const SERVICE& service,
                                   const SERVER& main,
                                   const SERVER& other,
                                   const char* zComparison_kind)
{
    auto& sValues = service.config();

    json_t* pParameters = json_object();
    json_object_set_new(pParameters, CN_USER, json_string(sValues->user.c_str()));
    json_object_set_new(pParameters, CN_PASSWORD, json_string(sValues->password.c_str()));
    json_object_set_new(pParameters, CN_SERVICE, json_string(service.name()));
    json_object_set_new(pParameters, "main", json_string(main.name()));
    json_object_set_new(pParameters, "comparison_kind", json_string(zComparison_kind));

    json_t* pAttributes = json_object();
    json_object_set_new(pAttributes, CN_ROUTER, json_string(MXB_MODULE_NAME));
    json_object_set_new(pAttributes, CN_PARAMETERS, pParameters);

    json_t* pServers_data = json_array();
    for (const char* zName : { main.name(), other.name() })
    {
        json_t* pServer_data = json_object();
        json_object_set_new(pServer_data, CN_ID, json_string(zName));
        json_object_set_new(pServer_data, CN_TYPE, json_string(CN_SERVERS));

        json_array_append_new(pServers_data, pServer_data);
    }
    json_t* pServers = json_object();
    json_object_set_new(pServers, CN_DATA, pServers_data);

    json_t* pRelationships = json_object();
    json_object_set_new(pRelationships, CN_SERVERS, pServers);

    json_t* pData = json_object();
    json_object_set_new(pData, CN_ID, json_string(name.c_str()));
    json_object_set_new(pData, CN_TYPE, json_string(CN_SERVICES));
    json_object_set_new(pData, CN_ATTRIBUTES, pAttributes);
    json_object_set_new(pData, CN_RELATIONSHIPS, pRelationships);

    json_t* pJson = json_object();
    json_object_set_new(pJson, CN_DATA, pData);

    Service* pC_service = nullptr;

    if (runtime_create_service_from_json(pJson))
    {
        pC_service = Service::find(name);

        if (!pC_service)
        {
            MXB_ERROR("Could create Comparator service '%s', but it could not subsequently "
                      "be looked up.", name.c_str());
        }
    }
    else
    {
        MXB_ERROR("Could not create Comparator service '%s', please check earlier errors.", name.c_str());
    }

    json_decref(pJson);

    return pC_service;
}

Service* create_comparator_service(const SERVICE& service,
                                   const SERVER& main,
                                   const SERVER& other,
                                   const char* zComparison_kind)
{
    Service* pC_service = nullptr;

    string name { "Comparator" };
    name += service.name();

    if (const char* zType = mxs::Config::get_object_type(name))
    {
        MXB_ERROR("Cannot create Comparator service for the service '%s', a %s "
                  "with the name '%s' exists already.",
                  service.name(), zType, name.c_str());
    }
    else
    {
        UnmaskPasswords unmasker;
        pC_service = create_comparator_service(name, service, main, other, zComparison_kind);
    }

    return pC_service;
}

bool get_comparison_kind(const char* zComparison_kind, ComparisonKind* pComparison_kind)
{
    bool rv = true;

    if (strcmp(zComparison_kind, "read_only") == 0)
    {
        *pComparison_kind = ComparisonKind::READ_ONLY;
    }
    else if (strcmp(zComparison_kind, "read_write") == 0)
    {
        *pComparison_kind = ComparisonKind::READ_WRITE;
    }
    else
    {
        MXB_ERROR("'%s' is not a valid value. Valid values are: 'read_only', 'read_write'",
                  zComparison_kind);
        rv = false;
    }

    return rv;
}

bool command_prepare(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    Service* pService = static_cast<Service*>(pArgs->argv[0].value.service);
    SERVER* pMain = pArgs->argv[1].value.server;
    SERVER* pOther = pArgs->argv[2].value.server;
    const char* zComparison_kind = pArgs->argv[3].value.string;
    ComparisonKind comparison_kind;

    if (!get_comparison_kind(zComparison_kind, &comparison_kind))
    {
        return false;
    }

    bool rv = true;

    vector<mxs::Target*> targets = pService->get_children();
    auto end = targets.end();

    auto it = std::find(targets.begin(), end, pMain);

    if (it != end)
    {
        switch (comparison_kind)
        {
        case ComparisonKind::READ_WRITE:
            if (!status_is_master(pMain->status()))
            {
                MXB_ERROR("'read_write' comparison specified, but '%s' is not the primary.",
                          pMain->name());
                rv = false;
            }
            break;

        case ComparisonKind::READ_ONLY:
            if (!status_is_slave(pMain->status()))
            {
                MXB_ERROR("'read_only' comparison specified, but '%s' is not a replica.",
                          pMain->name());
                rv = false;
            }
            break;
        }

        if (rv)
        {
            rv = check_prepare_prerequisites(*pService, *pMain, *pOther);
        }

        if (rv)
        {
            Service* pC_service = create_comparator_service(*pService, *pMain, *pOther, zComparison_kind);

            if (pC_service)
            {
                json_t* pOutput = json_object();
                auto s = mxb::string_printf("Comparator service '%s' created. Server '%s' ready "
                                            "to be evaluated.",
                                            pC_service->name(),
                                            pOther->name());
                json_object_set_new(pOutput, "status", json_string(s.c_str()));
                *ppOutput = pOutput;

                rv = true;
            }
        }
    }
    else
    {
        MXB_ERROR("'%s' is not a server of service '%s'.", pMain->name(), pService->name());
        rv = false;
    }

    return rv;
}

void register_prepare_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "prepare",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_prepare,
                                                  MXS_ARRAY_NELEMS(command_prepare_argv),
                                                  command_prepare_argv,
                                                  "Prepare Comparator for Service");
    mxb_assert(rv);
}

}

/**
 * call command start
 */
namespace
{

static modulecmd_arg_type_t command_start_argv[] =
{
    {MODULECMD_ARG_SERVICE | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Service name"},
};

static int command_start_argc = MXS_ARRAY_NELEMS(command_start_argv);


bool command_start(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    auto* pService = pArgs->argv[0].value.service;
    auto* pRouter = static_cast<CRouter*>(pService->router());

    return pRouter->start(ppOutput);
};

void register_start_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "start",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_start,
                                                  MXS_ARRAY_NELEMS(command_start_argv),
                                                  command_start_argv,
                                                  "Start Comparator for Service");
    mxb_assert(rv);
}

}

/**
 * call command status
 */
namespace
{

static modulecmd_arg_type_t command_status_argv[] =
{
    {MODULECMD_ARG_SERVICE | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Service name"},
};

static int command_status_argc = MXS_ARRAY_NELEMS(command_status_argv);


bool command_status(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    auto* pService = pArgs->argv[0].value.service;
    auto* pRouter = static_cast<CRouter*>(pService->router());

    return pRouter->status(ppOutput);
};

void register_status_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "status",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_status,
                                                  MXS_ARRAY_NELEMS(command_status_argv),
                                                  command_status_argv,
                                                  "comparator service status");
    mxb_assert(rv);
}

}

/**
 * call command stop
 */
namespace
{

static modulecmd_arg_type_t command_stop_argv[] =
{
    {MODULECMD_ARG_SERVICE | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Service name"},
};

static int command_stop_argc = MXS_ARRAY_NELEMS(command_stop_argv);


bool command_stop(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    auto* pService = pArgs->argv[0].value.service;
    auto* pRouter = static_cast<CRouter*>(pService->router());

    return pRouter->stop(ppOutput);
};

void register_stop_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "stop",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_stop,
                                                  MXS_ARRAY_NELEMS(command_stop_argv),
                                                  command_stop_argv,
                                                  "comparator service stop");
    mxb_assert(rv);
}

}

/**
 * call command summary
 */
namespace
{

static modulecmd_arg_type_t command_summary_argv[] =
{
    {MODULECMD_ARG_SERVICE | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Service name"},
    {MODULECMD_ARG_STRING | MODULECMD_ARG_OPTIONAL,
     "Enumeration - return|save|both - indicating whether the summary should be returned, "
     "saved, or both returned and saved. 'save' is the default."},
};

std::map<std::string, CRouter::Summary> summary_keywords =
{
    { "return", CRouter::Summary::RETURN },
    { "save", CRouter::Summary::SAVE },
    { "both", CRouter::Summary::BOTH },
};

static int command_summary_argc = MXS_ARRAY_NELEMS(command_summary_argv);


bool command_summary(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    bool rv = true;

    auto* pService = pArgs->argv[0].value.service;
    auto* pRouter = static_cast<CRouter*>(pService->router());

    CRouter::Summary summary = CRouter::Summary::SAVE;

    if (pArgs->argc == 2)
    {
        const char* zKeyword = pArgs->argv[1].value.string;

        auto it = summary_keywords.find(zKeyword);
        auto end = summary_keywords.end();

        if (it != end)
        {
            summary = it->second;
        }
        else
        {
            auto begin = summary_keywords.begin();
            std::vector<std::string> values;
            auto to = std::back_inserter(values);

            std::transform(begin, end, to, [](const auto& kv) {
                    return kv.first;
                });

            MXB_ERROR("'%s' is not a valid value. Valid values are: %s",
                      zKeyword, mxb::join(values, ",", "'").c_str());
            rv = false;
        }
    }

    if (rv)
    {
        rv = pRouter->summary(summary, ppOutput);
    }

    return rv;
};

void register_summary_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "summary",
                                                  MODULECMD_TYPE_PASSIVE,
                                                  command_summary,
                                                  MXS_ARRAY_NELEMS(command_summary_argv),
                                                  command_summary_argv,
                                                  "comparator service summary");
    mxb_assert(rv);
}

}

/*
 * call command unprepare
 */
namespace
{

static modulecmd_arg_type_t command_unprepare_argv[] =
{
    {MODULECMD_ARG_SERVICE | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Service name"},
};

static int command_unprepare_argc = MXS_ARRAY_NELEMS(command_unprepare_argv);

bool command_unprepare(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    bool rv = false;

    Service* pService = static_cast<Service*>(pArgs->argv[0].value.service);

    std::vector<mxs::Target*> targets = pService->get_children();
    std::set<std::string> target_names;

    std::transform(targets.begin(), targets.end(), std::inserter(target_names, target_names.begin()),
                   [](const mxs::Target* pTarget) {
                       return pTarget->name();
                   });

    rv = runtime_unlink_service(pService, target_names);

    if (rv)
    {
        bool use_force = false;
        rv = runtime_destroy_service(pService, use_force);

        if (!rv)
        {
            MXB_ERROR("Could not unprepare/destroy service '%s'.", pService->name());
        }
    }
    else
    {
        MXB_ERROR("Could not remove targets %s from service '%s' in order to "
                  "unprepare/destroy the latter.",
                  mxb::join(target_names, ",", "'").c_str(), pService->name());
    }

    return rv;
}

void register_unprepare_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "unprepare",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_unprepare,
                                                  MXS_ARRAY_NELEMS(command_unprepare_argv),
                                                  command_unprepare_argv,
                                                  "Unprepare/destroy comparator service");
    mxb_assert(rv);
}

}

