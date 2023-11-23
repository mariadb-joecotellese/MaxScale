/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#define MXB_MODULE_NAME "urat"

#include <maxscale/ccdefs.hh>
#include <maxscale/log.hh>
#include <maxscale/router.hh>
#include <maxscale/routing.hh>

namespace urat
{

const uint64_t CAPABILITIES = RCAP_TYPE_REQUEST_TRACKING | RCAP_TYPE_RUNTIME_CONFIG;

enum class State
{
    PREPARED,      // Setup for action.
    SYNCHRONIZING, // Started, suspending sessions, stopping replication, etc.
    CAPTURING      // Sessions restarted, capturing in process.
};

const char* to_string(State state);

}
