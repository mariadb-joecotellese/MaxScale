/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capdefs.hh"
#include <maxbase/exception.hh>
#include <maxscale/config2.hh>
#include <functional>

DEFINE_EXCEPTION(WcarError);

enum class StorageType
{
    BINARY
};

enum class StorageMethod
{
    DIRECT,
    BATCH
};

class CapConfig : public mxs::config::Configuration
{
public:
    static mxs::config::Specification* specification();
    CapConfig(const std::string& name, std::function<bool()> filter_post_configure);

    std::string   capture_dir;
    StorageType   storage_type;
    StorageMethod storage_method;
    bool          start_capture;

private:
    bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;
    std::function<bool()> m_filter_post_configure;
};
