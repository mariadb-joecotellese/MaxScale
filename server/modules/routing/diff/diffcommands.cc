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
#include "diffutils.hh"

using namespace std;

typedef std::set<std::string> StringSet;

namespace
{
void register_create_command();
void register_destroy_command();
void register_start_command();
void register_status_command();
void register_stop_command();
void register_summary_command();
}

void diff_register_commands()
{
    register_create_command();
    register_destroy_command();
    register_start_command();
    register_status_command();
    register_stop_command();
    register_summary_command();
}

/*
 * call command create
 */
namespace
{

static modulecmd_arg_type_t command_create_argv[] =
{
    {MODULECMD_ARG_STRING,  "Name of Diff service to be created"},
    {MODULECMD_ARG_SERVICE, "Name of existing service"},
    {MODULECMD_ARG_SERVER,  "Main server name"},
    {MODULECMD_ARG_SERVER,  "Other server name"}
};

static int command_create_argc = MXS_ARRAY_NELEMS(command_create_argv);

Service* create_diff_service(const string& name,
                             const SERVICE& service,
                             const SERVER& main,
                             const SERVER& other)
{
    auto& sValues = service.config();

    json_t* pParameters = json_object();
    json_object_set_new(pParameters, CN_USER, json_string(sValues->user.c_str()));
    json_object_set_new(pParameters, CN_PASSWORD, json_string(sValues->password.c_str()));
    json_object_set_new(pParameters, CN_SERVICE, json_string(service.name()));
    json_object_set_new(pParameters, "main", json_string(main.name()));

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
            MXB_ERROR("Could create Diff service '%s', but it could not subsequently "
                      "be looked up.", name.c_str());
        }
    }
    else
    {
        MXB_ERROR("Could not create Diff service '%s', please check earlier errors.", name.c_str());
    }

    json_decref(pJson);

    return pC_service;
}

bool command_create(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    bool rv = false;

    const char* zDiff_service_name = pArgs->argv[0].value.string;
    Service* pService = static_cast<Service*>(pArgs->argv[1].value.service);
    SERVER* pMain = pArgs->argv[2].value.server;
    SERVER* pOther = pArgs->argv[3].value.server;

    vector<mxs::Target*> targets = pService->get_children();
    auto end = targets.end();

    auto it = std::find(targets.begin(), end, pMain);

    if (it != end)
    {
        bool arguments_ok = true;
        switch (get_replication_status(*pService, *pMain, *pOther))
        {
        case ReplicationStatus::OTHER_REPLICATES_FROM_MAIN:
            if (!status_is_master(pMain->status()))
            {
                MXB_ERROR("Read-write comparison implied as '%s' replicates from '%s', "
                          "but '%s' is not the primary.",
                          pOther->name(), pMain->name(), pMain->name());
                arguments_ok = false;
            }
            break;

        case ReplicationStatus::BOTH_REPLICATES_FROM_THIRD:
            if (!status_is_slave(pMain->status()))
            {
                MXB_ERROR("Read-only comparison implied as '%s' and '%s' replicates "
                          "from the same server, but '%s' is not a replica.",
                          pOther->name(), pMain->name(), pMain->name());
                arguments_ok = false;
            }
            break;

        case ReplicationStatus::MAIN_REPLICATES_FROM_OTHER:
            MXB_ERROR("Main '%s' replicates from other '%s', cannot continue.",
                      pMain->name(), pOther->name());
            arguments_ok = false;
            break;

        case ReplicationStatus::NO_RELATION:
            // TODO: This might make sense if you intend to use a read-only workload.
            MXB_ERROR("There is no replication relation between main '%s' and other '%s'.",
                      pMain->name(), pOther->name());
            arguments_ok = false;
            break;

        case ReplicationStatus::ERROR:
            arguments_ok = false;
        }

        if (arguments_ok)
        {
            if (const char* zType = mxs::Config::get_object_type(zDiff_service_name))
            {
                MXB_ERROR("Cannot create Diff service '%s' for the service '%s', a %s "
                          "with the name '%s' exists already.",
                          zDiff_service_name, pService->name(), zType, zDiff_service_name);
            }
            else
            {
                UnmaskPasswords unmasker;
                Service* pC_service = create_diff_service(zDiff_service_name, *pService, *pMain, *pOther);

                if (pC_service)
                {
                    json_t* pOutput = json_object();
                    auto s = mxb::string_printf("Diff service '%s' created. Server '%s' ready "
                                                "to be evaluated.",
                                                pC_service->name(),
                                                pOther->name());
                    json_object_set_new(pOutput, "status", json_string(s.c_str()));
                    *ppOutput = pOutput;
                    rv = true;
                }
            }
        }
    }
    else
    {
        MXB_ERROR("'%s' is not a server of service '%s'.", pMain->name(), pService->name());
    }

    return rv;
}

void register_create_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "create",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_create,
                                                  MXS_ARRAY_NELEMS(command_create_argv),
                                                  command_create_argv,
                                                  "Create Diff for Service");
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
    auto* pRouter = static_cast<DiffRouter*>(pService->router());

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
                                                  "Start Diff for Service");
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
    auto* pRouter = static_cast<DiffRouter*>(pService->router());

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
                                                  "diff service status");
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
    auto* pRouter = static_cast<DiffRouter*>(pService->router());

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
                                                  "diff service stop");
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

std::map<std::string, DiffRouter::Summary> summary_keywords =
{
    { "return", DiffRouter::Summary::RETURN },
    { "save", DiffRouter::Summary::SAVE },
    { "both", DiffRouter::Summary::BOTH },
};

static int command_summary_argc = MXS_ARRAY_NELEMS(command_summary_argv);


bool command_summary(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    bool rv = true;

    auto* pService = pArgs->argv[0].value.service;
    auto* pRouter = static_cast<DiffRouter*>(pService->router());

    DiffRouter::Summary summary = DiffRouter::Summary::SAVE;

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
                                                  "diff service summary");
    mxb_assert(rv);
}

}

/*
 * call command destroy
 */
namespace
{

static modulecmd_arg_type_t command_destroy_argv[] =
{
    {MODULECMD_ARG_SERVICE | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Service name"},
};

static int command_destroy_argc = MXS_ARRAY_NELEMS(command_destroy_argv);

bool command_destroy(const MODULECMD_ARG* pArgs, json_t** ppOutput)
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
            MXB_ERROR("Could not destroy service '%s'.", pService->name());
        }
    }
    else
    {
        MXB_ERROR("Could not remove targets %s from service '%s' in order to "
                  "destroy the latter.",
                  mxb::join(target_names, ",", "'").c_str(), pService->name());
    }

    return rv;
}

void register_destroy_command()
{
    MXB_AT_DEBUG(bool rv);

    MXB_AT_DEBUG(rv =) modulecmd_register_command(MXB_MODULE_NAME,
                                                  "destroy",
                                                  MODULECMD_TYPE_ACTIVE,
                                                  command_destroy,
                                                  MXS_ARRAY_NELEMS(command_destroy_argv),
                                                  command_destroy_argv,
                                                  "Destroy diff service");
    mxb_assert(rv);
}

}

