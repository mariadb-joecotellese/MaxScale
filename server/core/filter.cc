/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

/**
 * @file filter.c  - A representation of a filter within MaxScale.
 */

#include "internal/filter.hh"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <maxscale/paths.hh>
#include <maxscale/session.hh>
#include <maxscale/service.hh>
#include <maxscale/filter.hh>
#include <maxscale/json_api.hh>

#include "internal/config.hh"
#include "internal/modules.hh"
#include "internal/service.hh"

using std::string;
using std::set;
using Guard = std::lock_guard<std::mutex>;

using namespace maxscale;

static struct
{
    std::mutex              lock;
    std::vector<SFilterDef> filters;
} this_unit;

namespace
{

namespace cfg = mxs::config;

cfg::Specification s_spec(CN_FILTERS, cfg::Specification::FILTER);

cfg::ParamString s_type(&s_spec, CN_TYPE, "The type of the object", CN_FILTER);

cfg::ParamModule s_module(&s_spec, CN_MODULE, "The filter module to use", mxs::ModuleType::FILTER);
}

// static
mxs::config::Specification* FilterDef::specification()
{
    return &s_spec;
}

template<class Params, class Unrecognized>
SFilterDef do_filter_alloc(const char* name, Params params, Unrecognized unrecognized)
{
    SFilterDef filter;

    if (s_spec.validate(params, unrecognized))
    {
        auto module = s_module.get(params);
        mxb_assert(module);
        mxb_assert(module->specification);

        if (module->specification->validate(params))
        {
            auto func = (mxs::FILTER_API*)module->module_object;

            if (auto instance = func->createInstance(name))
            {
                filter = std::make_shared<FilterDef>(name, module->name, instance);

                if (filter->configuration().configure(params))
                {
                    Guard guard(this_unit.lock);
                    this_unit.filters.push_back(filter);
                }
                else
                {
                    filter.reset();
                }
            }
            else
            {
                MXB_ERROR("Failed to create filter '%s'.", name);
            }
        }
    }

    return filter;
}

SFilterDef filter_alloc(const char* name, const mxs::ConfigParameters params)
{
    mxs::ConfigParameters unrecognized;
    return do_filter_alloc(name, params, &unrecognized);
}

SFilterDef filter_alloc(const char* name, json_t* params)
{
    std::set<std::string> unrecognized;
    return do_filter_alloc(name, params, &unrecognized);
}

FilterDef::FilterDef(std::string name, std::string module, Filter* instance)
    : m_name(std::move(name))
    , m_module(std::move(module))
    , m_filter(instance)
{
}

FilterDef::~FilterDef()
{
    MXB_INFO("Destroying '%s'", name());
}

/**
 * Free the specified filter
 *
 * @param filter        The filter to free
 */
void filter_free(const SFilterDef& filter)
{
    mxb_assert(filter);
    // Removing the filter from the list will trigger deletion once it's no longer in use
    Guard guard(this_unit.lock);
    auto it = std::remove(this_unit.filters.begin(), this_unit.filters.end(), filter);
    mxb_assert(it != this_unit.filters.end());
    this_unit.filters.erase(it);
}

SFilterDef filter_find(const std::string& name)
{
    Guard guard(this_unit.lock);

    for (const auto& filter : this_unit.filters)
    {
        if (filter->name() == name)
        {
            return filter;
        }
    }

    return SFilterDef();
}

std::vector<SFilterDef> filter_depends_on_target(const mxs::Target* target)
{
    std::vector<SFilterDef> rval;
    Guard guard(this_unit.lock);

    for (const auto& filter : this_unit.filters)
    {
        for (const auto& kv : filter->configuration())
        {
            auto t = kv.second->parameter().type();

            if (t == "service" || t == "server" || t == "target")
            {
                if (kv.second->to_string() == target->name())
                {
                    rval.push_back(filter);
                }
            }
        }
    }

    return rval;
}

bool filter_can_be_destroyed(const SFilterDef& filter)
{
    mxb_assert(filter);
    return service_filter_in_use(filter).empty();
}

void filter_destroy(const SFilterDef& filter)
{
    mxb_assert(filter);
    mxb_assert(filter_can_be_destroyed(filter));
    filter_free(filter);
}

void filter_destroy_instances()
{
    Guard guard(this_unit.lock);
    this_unit.filters.clear();
}

Filter* filter_def_get_instance(const MXS_FILTER_DEF* filter_def)
{
    const FilterDef* filter = static_cast<const FilterDef*>(filter_def);
    mxb_assert(filter);
    return filter->instance();
}

json_t* FilterDef::parameters_to_json() const
{
    json_t* params = configuration().to_json();
    json_object_set_new(params, CN_MODULE, json_string(m_module.c_str()));
    return params;
}

json_t* FilterDef::json_data(const char* host) const
{
    const char CN_FILTER_DIAGNOSTICS[] = "filter_diagnostics";
    json_t* rval = json_object();

    json_object_set_new(rval, CN_ID, json_string(name()));
    json_object_set_new(rval, CN_TYPE, json_string(CN_FILTERS));

    json_t* attr = json_object();

    json_object_set_new(attr, CN_MODULE, json_string(module()));
    json_object_set_new(attr, CN_PARAMETERS, parameters_to_json());
    json_object_set_new(attr, CN_SOURCE, mxs::Config::object_source_to_json(name()));

    if (json_t* diag = instance()->diagnostics())
    {
        json_object_set_new(attr, CN_FILTER_DIAGNOSTICS, diag);
    }

    /** Store relationships to other objects */
    json_t* rel = json_object();

    std::string self = MXS_JSON_API_FILTERS + m_name + "/relationships/services";

    if (auto services = service_relations_to_filter(this, host, self))
    {
        json_object_set_new(rel, CN_SERVICES, services);
    }

    json_object_set_new(rval, CN_RELATIONSHIPS, rel);
    json_object_set_new(rval, CN_ATTRIBUTES, attr);
    json_object_set_new(rval, CN_LINKS, mxs_json_self_link(host, CN_FILTERS, name()));

    return rval;
}

json_t* FilterDef::to_json(const char* host) const
{
    string self = MXS_JSON_API_FILTERS + m_name;
    return mxs_json_resource(host, self.c_str(), json_data(host));
}

// static
json_t* FilterDef::filter_list_to_json(const char* host)
{
    json_t* rval = json_array();

    Guard guard(this_unit.lock);

    for (const auto& f : this_unit.filters)
    {
        if (json_t* json = f->json_data(host))
        {
            json_array_append_new(rval, json);
        }
    }

    return mxs_json_resource(host, MXS_JSON_API_FILTERS, rval);
}

namespace maxscale
{

//
// FilterSession
//

FilterSession::FilterSession(MXS_SESSION* pSession, SERVICE* pService)
    : m_pSession(pSession)
    , m_pService(pService)
    , m_pParser(m_pSession->client_connection()->parser())
{
}

FilterSession::~FilterSession()
{
}

void FilterSession::setDownstream(mxs::Routable* down)
{
    m_down = down;
}

void FilterSession::setUpstream(mxs::Routable* up)
{
    m_up = up;
}

bool FilterSession::routeQuery(GWBUF&& packet)
{
    return m_down->routeQuery(std::move(packet));
}

bool FilterSession::clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    return m_up->clientReply(std::move(packet), down, reply);
}

json_t* FilterSession::diagnostics() const
{
    return NULL;
}

void FilterSession::set_response(GWBUF&& response) const
{
    session_set_response(m_pSession, m_up, std::move(response));
}

const mxs::ProtocolData& FilterSession::protocol_data() const
{
    mxb_assert(m_pSession->protocol_data());
    return *m_pSession->protocol_data();
}

const mxs::ProtocolModule& FilterSession::protocol() const
{
    mxb_assert(m_pSession->protocol());
    return *m_pSession->protocol();
}

void FilterSession::lcall(std::function<bool()>&& fn)
{
    m_pSession->delay_routing(this, GWBUF {}, 0ms, [this, func = std::move(fn)](GWBUF&&){
        return func();
    });
}
}

std::ostream& FilterDef::persist(std::ostream& os) const
{
    configuration().persist(os);
    os << "type=filter\n";
    os << "module=" << module() << "\n";

    return os;
}
