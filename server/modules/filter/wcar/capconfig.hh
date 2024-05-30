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

enum class StorageMethod
{
    DIRECT,
    BATCH
};

enum class CaptureStartMethod
{
    ABORT_ACTIVE_TRANSACTIONS,
    IGNORE_ACTIVE_TRANSACTIONS
};

class CapConfig : public mxs::config::Configuration
{
public:
    static mxs::config::Specification* specification();
    CapConfig(const std::string& name, std::function<bool()> filter_post_configure);

    std::string        capture_dir;
    StorageMethod      storage_method;
    bool               start_capture;
    CaptureStartMethod capture_start_method;
    mxb::Duration      capture_duration;
    int64_t            capture_size;

    // The capture directory with the filter name as the suffix
    std::string capture_directory() const;

private:
    bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;
    std::function<bool()> m_filter_post_configure;
};

/**
 *  Parse strings of the form "key1=value1 key2=value2".
 *  - No spaces around the equals signs.
 *  - The keys can be any combination of std::isalnum
 *  - The values can be any combination of std::isgraph
 *  In the end it is up to the code that uses a key-value pair to determine
 *  if the value is valid.
 */
std::map<std::string, std::string> parse_key_value_pairs(const std::string& str);
