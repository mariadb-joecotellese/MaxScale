/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#define MXB_MODULE_NAME "comparator"

#include <maxscale/ccdefs.hh>
#include <maxscale/log.hh>
#include <maxscale/router.hh>
#include <maxscale/routing.hh>

const uint64_t COMPARATOR_CAPABILITIES = RCAP_TYPE_REQUEST_TRACKING | RCAP_TYPE_RUNTIME_CONFIG;

using ComparatorHash = uint64_t;
