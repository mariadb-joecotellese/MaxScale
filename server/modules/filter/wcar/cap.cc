/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "capdefs.hh"
#include "capconfig.hh"
#include "capfilter.hh"
#include <maxscale/modulecmd.hh>

namespace
{
modulecmd_arg_type_t start_cmd_args[] =
{
    {MODULECMD_ARG_FILTER | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Capture filter name"},
    {MODULECMD_ARG_STRING | MODULECMD_ARG_OPTIONAL,            "prefix"             },
    {MODULECMD_ARG_STRING | MODULECMD_ARG_OPTIONAL,            "size"               },
    {MODULECMD_ARG_STRING | MODULECMD_ARG_OPTIONAL,            "duration"           },
};

modulecmd_arg_type_t stop_cmd_args[] =
{
    {MODULECMD_ARG_FILTER | MODULECMD_ARG_NAME_MATCHES_DOMAIN, "Capture filter name"},
};

bool start_cmd(const MODULECMD_ARG* argv, json_t** output)
{
    mxb_assert(argv->argc > 0);
    mxb_assert(argv->argv[0].type.type == MODULECMD_ARG_FILTER);

    MXS_FILTER_DEF* filter = argv[0].argv->value.filter;
    std::string options;
    for (int i = 1; i < argv->argc; ++i)
    {
        options += std::string(argv->argv[i].value.string) + ' ';
    }

    CapFilter* instance = reinterpret_cast<CapFilter*>(filter_def_get_instance(filter));

    try
    {
        return instance->start_capture(options);
    }
    catch (const std::exception& ex)
    {
        MXB_ERROR("Failed to start capture: %s", ex.what());
        return false;
    }
}

bool stop_cmd(const MODULECMD_ARG* argv, json_t** output)
{
    mxb_assert(argv->argc > 0);
    mxb_assert(argv->argv[0].type.type == MODULECMD_ARG_FILTER);

    MXS_FILTER_DEF* filter = argv[0].argv->value.filter;
    CapFilter* instance = reinterpret_cast<CapFilter*>(filter_def_get_instance(filter));

    try
    {
        return instance->stop_capture();
    }
    catch (const std::exception& ex)
    {
        MXB_ERROR("Failed to stop capture: %s", ex.what());
        return false;
    }
}
}

extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    modulecmd_register_command(MXB_MODULE_NAME, "start", MODULECMD_TYPE_ACTIVE,
                               start_cmd, 4, start_cmd_args,
                               "Start capture");
    modulecmd_register_command(MXB_MODULE_NAME, "stop", MODULECMD_TYPE_ACTIVE,
                               stop_cmd, 1, stop_cmd_args,
                               "Stop capture");


    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::FILTER,
        mxs::ModuleStatus::IN_DEVELOPMENT,
        MXS_FILTER_VERSION,
        "Workload Capture and Replay.",
        WCAR_VERSION_STRING,
        CapFilter::CAPABILITIES,
        &mxs::FilterApi<CapFilter>::s_api,
        nullptr,    /* Process init. */
        nullptr,    /* Process finish. */
        nullptr,    /* Thread init. */
        nullptr,    /* Thread finish. */
        CapConfig::specification()
    };

    return &info;
}
