/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "crouter.hh"
#include "ccommands.hh"


extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    comparator_register_commands();

    const char* zDesc = "Compare different servers";

    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        "comparator",
        mxs::ModuleType::ROUTER,
        mxs::ModuleStatus::ALPHA,
        MXS_ROUTER_VERSION,
        zDesc,
        "V1.0.0",
        COMPARATOR_CAPABILITIES,
        &mxs::RouterApi<CRouter>::s_api,
        NULL,
        NULL,
        NULL,
        NULL,
        CConfig::specification()
    };

    return &info;
}
