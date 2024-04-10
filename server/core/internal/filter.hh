/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

/**
 * @file core/maxscale/filter.h - The private filter interface
 */

#include <maxscale/filter.hh>

#include <memory>
#include <mutex>
#include <maxscale/config_common.hh>
#include <maxscale/config_state.hh>

/**
 * The definition of a filter from the configuration file.
 * This is basically the link between a plugin to load and the
 * options to pass to that plugin.
 */
class FilterDef : public MXS_FILTER_DEF
                , public mxs::ConfigState
{
public:
    FilterDef(std::string name, std::string module, mxs::Filter* instance);
    ~FilterDef();

    const char* name() const
    {
        return m_name.c_str();
    }

    const char* module() const
    {
        return m_module.c_str();
    }

    mxs::Filter* instance() const
    {
        return m_filter.get();
    }

    uint64_t capabilities() const
    {
        return instance()->getCapabilities();
    }

    mxs::config::Configuration& configuration() const
    {
        mxb_assert(instance());
        return instance()->getConfiguration();
    }

    std::ostream& persist(std::ostream& os) const;

    json_t*        to_json(const char* host) const;
    static json_t* filter_list_to_json(const char* host);

    static mxs::config::Specification* specification();

    mxb::Json config_state() const override
    {
        return mxb::Json(parameters_to_json(), mxb::Json::RefType::STEAL);
    }

private:
    std::string                  m_name;    /**< The Filter name */
    std::string                  m_module;  /**< The module to load */
    std::unique_ptr<mxs::Filter> m_filter;  /**< The runtime filter */

    json_t* json_data(const char* host) const;
    json_t* parameters_to_json() const;
};

typedef std::shared_ptr<FilterDef> SFilterDef;

SFilterDef filter_alloc(const char* name, const mxs::ConfigParameters params);
SFilterDef filter_alloc(const char* name, json_t* params);

void filter_free(const SFilterDef& filter);

// Find the internal filter representation
SFilterDef filter_find(const std::string& name);

/**
 * Check if a filter uses a server or a service
 *
 * @param target The target to check
 *
 * @return The list of filters that depend on the given target
 */
std::vector<SFilterDef> filter_depends_on_target(const mxs::Target* target);

/**
 * Check if filter can be destroyed
 *
 * A filter can be destroyed if no service uses it.
 *
 * @param filter Filter to check
 *
 * @return True if filter can be destroyed
 */
bool filter_can_be_destroyed(const SFilterDef& filter);

/**
 * Destroy a filter
 *
 * @param filter Filter to destroy
 */
void filter_destroy(const SFilterDef& filter);

/**
 * Destroy all filters
 */
void filter_destroy_instances();
