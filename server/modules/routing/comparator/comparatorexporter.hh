/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"

#include <maxbase/jansson.hh>

#include "comparatorconfig.hh"

class UratExporter
{
public:
    virtual ~UratExporter() = default;

    /**
     * Ship a JSON object outside of MaxScale
     *
     * @param pJson JSON object to ship
     */
    virtual void ship(json_t* pJson) = 0;
};

std::unique_ptr<UratExporter> build_exporter(const UratConfig& config);
