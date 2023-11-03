/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "wcarconfig.hh"

namespace config = mxs::config;

namespace wcar
{

config::Specification specification(MXB_MODULE_NAME, config::Specification::FILTER);

}

WcarConfig::WcarConfig(const std::string& name)
    : config::Configuration(name, &wcar::specification)
{
}
