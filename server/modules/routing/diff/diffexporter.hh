/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <maxbase/jansson.hh>
#include <maxscale/target.hh>
#include "diffconfig.hh"

class DiffExporter
{
public:
    virtual ~DiffExporter() = default;

    /**
     * Ship a JSON object outside of MaxScale
     *
     * @param pJson JSON object to ship
     */
    virtual void ship(json_t* pJson) = 0;
};

std::unique_ptr<DiffExporter> build_exporter(const DiffConfig& config, const mxs::Target& target);
