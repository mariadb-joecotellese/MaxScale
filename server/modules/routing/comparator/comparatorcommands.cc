/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorcommands.hh"
#include <set>
#include <vector>
#include <maxbase/format.hh>
#include <maxbase/string.hh>
#include <maxsql/mariadb_connector.hh>
#include <maxscale/config.hh>
#include <maxscale/json_api.hh>
#include <maxscale/modulecmd.hh>
#include <maxscale/utils.hh>
#include <maxscale/protocol/mariadb/maxscale.hh>
#include "../../../core/internal/config_runtime.hh"
#include "../../../core/internal/monitormanager.hh"
#include "../../../core/internal/service.hh"
#include "comparatorrouter.hh"

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
    {MODULECMD_ARG_SERVER,  "Other server name"}
};

static int command_prepare_argc = MXS_ARRAY_NELEMS(command_prepare_argv);

bool check_prepare_prerequisites(const SERVICE& service,
                                 const SERVER& primary,
                                 const SERVER& replica)
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
                                  replica.name(), master_host.c_str(), master_port);
                    }
                }
                else
                {
                    MXB_ERROR("Server '%s' replicates from %s:%d and not from '%s' (%s:%d).",
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

mxs::Monitor* create_comparator_monitor(const string& name,
                                        const SERVER& primary,
                                        const SERVER& replica)
{
    mxs::Monitor* pComparator_monitor = nullptr;
    mxs::Monitor* pPrimary_monitor = MonitorManager::server_is_monitored(&primary);

    if (!pPrimary_monitor)
    {
        MXB_ERROR("Cannot create Comparator monitor '%s', the primary server '%s' is not "
                  "monitored and thus there is no monitor to copy settings from.",
                  name.c_str(), primary.name());
    }
    else
    {
        string module = "mariadbmon";
        mxs::ConfigParameters params;

        const auto& settings = pPrimary_monitor->conn_settings();

        params.set("module", module);
        params.set("user", settings.username);
        params.set("password", settings.password);
        params.set("servers", replica.name());

        pComparator_monitor = MonitorManager::create_monitor(name, module, &params);

        if (!pComparator_monitor)
        {
            MXB_ERROR("Could not create Comparator monitor '%s', please check earlier errors.", name.c_str());
        }
    }

    return pComparator_monitor;
}

mxs::Monitor* create_comparator_monitor(const SERVICE& service,
                                        const SERVER& primary,
                                        const SERVER& replica)
{
    mxs::Monitor* pComparator_monitor = nullptr;

    string name { "Monitor_for_Comparator" };
    name += service.name();

    if (const char* zType = mxs::Config::get_object_type(name))
    {
        MXB_ERROR("Cannot create Comparator monitor '%s', a %s with that name already exists.",
                  name.c_str(), zType);
    }
    else
    {
        pComparator_monitor = create_comparator_monitor(name, primary, replica);
    }

    return pComparator_monitor;
}

Service* create_comparator_service(const string& name,
                                   const SERVICE& service,
                                   const SERVER& primary,
                                   const SERVER& replica)
{
    mxs::ConfigParameters params;
    mxs::ConfigParameters unknown;

    auto& sValues = service.config();

    vector<string> servers { primary.name(), replica.name() };

    params.set("user", sValues->user);
    params.set("password", sValues->password);
    params.set("router", "comparator");
    params.set("main", primary.name());
    params.set("servers", mxb::join(servers, ","));
    params.set("service", service.name());

    Service* pComparator_service = Service::create(name.c_str(), params);

    if (!pComparator_service)
    {
        MXB_ERROR("Could not create Comparator service '%s', please check earlier errors.", name.c_str());
    }

    return pComparator_service;
}

Service* create_comparator_service(const SERVICE& service,
                                   const SERVER& primary,
                                   const SERVER& replica)
{
    Service* pComparator_service = nullptr;

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
        pComparator_service = create_comparator_service(name, service, primary, replica);
    }

    return pComparator_service;
}

bool command_prepare(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    bool rv = false;

    Service* pService = static_cast<Service*>(pArgs->argv[0].value.service);
    SERVER* pPrimary = pArgs->argv[1].value.server;
    SERVER* pReplica = pArgs->argv[2].value.server;

    vector<mxs::Target*> targets = pService->get_children();
    auto end = targets.end();

    auto it = std::find(targets.begin(), end, pPrimary);

    if (it != end)
    {
        if (check_prepare_prerequisites(*pService, *pPrimary, *pReplica))
        {
            mxs::Monitor* pComparator_monitor = create_comparator_monitor(*pService, *pPrimary, *pReplica);

            if (pComparator_monitor)
            {
                MonitorManager::start_monitor(pComparator_monitor);

                Service* pComparator_service = create_comparator_service(*pService, *pPrimary, *pReplica);

                if (pComparator_service)
                {
                    json_t* pOutput = json_object();
                    auto s = mxb::string_printf("Comparator service '%s' and associated "
                                                "monitor '%s' created. Server '%s' ready "
                                                "to be evaluated.",
                                                pComparator_service->name(),
                                                pComparator_monitor->name(),
                                                pReplica->name());
                    json_object_set_new(pOutput, "status", json_string(s.c_str()));
                    *ppOutput = pOutput;

                    rv = true;
                }
            }
        }
    }
    else
    {
        MXB_ERROR("'%s' is not a server of service '%s'.", pPrimary->name(), pService->name());
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
    auto* pRouter = static_cast<ComparatorRouter*>(pService->router());

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
    auto* pRouter = static_cast<ComparatorRouter*>(pService->router());

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
    auto* pRouter = static_cast<ComparatorRouter*>(pService->router());

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

std::map<std::string, ComparatorRouter::Summary> summary_keywords =
{
    { "return", ComparatorRouter::Summary::RETURN },
    { "save", ComparatorRouter::Summary::SAVE },
    { "both", ComparatorRouter::Summary::BOTH },
};

static int command_summary_argc = MXS_ARRAY_NELEMS(command_summary_argv);


bool command_summary(const MODULECMD_ARG* pArgs, json_t** ppOutput)
{
    bool rv = true;

    auto* pService = pArgs->argv[0].value.service;
    auto* pRouter = static_cast<ComparatorRouter*>(pService->router());

    ComparatorRouter::Summary summary = ComparatorRouter::Summary::SAVE;

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

