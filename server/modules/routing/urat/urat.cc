/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratrouter.hh"
#include "uratcommands.hh"

namespace urat
{

const char* to_string(State state)
{
    switch (state)
    {
    case State::PREPARED:
        return "prepared";

    case State::SYNCHRONIZING:
        return "synchronizing";

    case State::CAPTURING:
        return "capturing";
    }

    mxb_assert(!true);
    return "unknown";
}
}

extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    urat_register_commands();

    const char* zDesc = "Upgrade Risk Assessment Tool";

    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        "urat",
        mxs::ModuleType::ROUTER,
        mxs::ModuleStatus::ALPHA,
        MXS_ROUTER_VERSION,
        zDesc,
        "V1.0.0",
        urat::CAPABILITIES,
        &mxs::RouterApi<UratRouter>::s_api,
        NULL,
        NULL,
        NULL,
        NULL,
        UratConfig::specification()
    };

    return &info;
}
