/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "capconfig.hh"
#include <maxscale/config.hh>
#include <filesystem>

namespace cfg = mxs::config;
namespace fs = std::filesystem;

cfg::Specification s_spec("wcar", cfg::Specification::FILTER);

// static
mxs::config::Specification* CapConfig::specification()
{
    return &s_spec;
}

cfg::ParamPath s_capture_dir(
    &s_spec, "capture_dir", "Directory where capture files are stored",
    cfg::ParamPath::C | cfg::ParamPath::W | cfg::ParamPath::R | cfg::ParamPath::X,
    mxs::datadir() + std::string("/capture"));

cfg::ParamEnum<StorageType> s_storage_type(
    &s_spec, "storage_type", "Type of persistent storage",
{
    {StorageType::SQLITE, "sqlite"},
    {StorageType::BINARY, "binary"},
},
    StorageType::SQLITE);

cfg::ParamEnum<StorageMethod> s_storage_method(
    &s_spec, "storage_method", "Type of persistent storage",
{
    {StorageMethod::DIRECT, "direct"},
    {StorageMethod::BATCH, "batch"},
},
    StorageMethod::DIRECT);

CapConfig::CapConfig(const std::string& name, std::function<bool ()> filter_post_configure)
    : cfg::Configuration(name, specification())
    , m_filter_post_configure(filter_post_configure)
{
    add_native(&CapConfig::capture_dir, &s_capture_dir);
    add_native(&CapConfig::storage_type, &s_storage_type);
    add_native(&CapConfig::storage_method, &s_storage_method);
}

bool CapConfig::post_configure(const std::map<std::string, maxscale::ConfigParameters>& nested_params)
{
    // TODO fix ParamPath, it does not create the dir when the default value is used.
    std::filesystem::create_directories(capture_dir);

    return m_filter_post_configure();
}
