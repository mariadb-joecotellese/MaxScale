/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffrouter.hh"
#include "diffcommands.hh"


extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    diff_register_commands();

    const char* zDesc = "Compare different servers";

    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        "diff",
        mxs::ModuleType::ROUTER,
        mxs::ModuleStatus::ALPHA,
        MXS_ROUTER_VERSION,
        zDesc,
        "V1.0.0",
        DIFF_CAPABILITIES,
        &mxs::RouterApi<DiffRouter>::s_api,
        NULL,
        NULL,
        NULL,
        NULL,
        DiffConfig::specification()
    };

    return &info;
}
