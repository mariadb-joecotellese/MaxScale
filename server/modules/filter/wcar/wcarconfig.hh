/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcardefs.hh"
#include <maxscale/config2.hh>

namespace wcar
{

extern mxs::config::Specification specification;

}

class WcarConfig : public mxs::config::Configuration
{
public:
    WcarConfig(const std::string& name);
};

