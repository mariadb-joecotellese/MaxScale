/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"

#include <maxbase/jansson.hh>
#include <maxscale/ccdefs.hh>

#include "uratconfig.hh"

struct UratExporter
{
    virtual ~UratExporter() = default;

    /**
     * Ship a JSON object outside of MaxScale
     *
     * @param obj JSON object to ship
     */
    virtual void ship(json_t* obj) = 0;
};

std::unique_ptr<UratExporter> build_exporter(const UratConfig& config);
