/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcardefs.hh"
#include "wcarconfig.hh"
#include "wcarfilter.hh"

extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::FILTER,
        mxs::ModuleStatus::IN_DEVELOPMENT,
        MXS_FILTER_VERSION,
        "Workload Capture and Replay.",
        WCAR_VERSION_STRING,
        WcarFilter::CAPABILITIES,
        &mxs::FilterApi<WcarFilter>::s_api,
        nullptr,    /* Process init. */
        nullptr,    /* Process finish. */
        nullptr,    /* Thread init. */
        nullptr,    /* Thread finish. */
        WcarConfig::specification()
    };

    return &info;
}
