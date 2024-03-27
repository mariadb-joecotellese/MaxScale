/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include <maxscale/config2.hh>

#include <sys/stat.h>
#include <unistd.h>

#include <maxbase/format.hh>
#include <maxscale/listener.hh>
#include <maxscale/mainworker.hh>
#include <maxscale/monitor.hh>
#include <maxscale/paths.hh>
#include <maxscale/secrets.hh>
#include <maxscale/utils.hh>
#include "internal/config.hh"
#include "internal/modules.hh"
#include "internal/service.hh"

using namespace std;

namespace
{

struct ThisUnit
{
    // ThisUnit is used as a scope and there is no `this_unit` instance. The
    // reason is that as configuration objects are static and this_unit would
    // be static, there is a creation order problem.

    static void insert(mxs::config::Configuration* pConfiguration)
    {
        init();

        mxb_assert(ThisUnit::pConfigurations->find(pConfiguration) == ThisUnit::pConfigurations->end());
        ThisUnit::pConfigurations->insert(pConfiguration);
    }

    static void remove(mxs::config::Configuration* pConfiguration)
    {
        auto it = ThisUnit::pConfigurations->find(pConfiguration);
        mxb_assert(it != ThisUnit::pConfigurations->end());
        ThisUnit::pConfigurations->erase(it);

        finish();
    }

    static std::set<mxs::config::Configuration*>* pConfigurations;

private:
    static void init()
    {
        if (!ThisUnit::pConfigurations)
        {
            ThisUnit::pConfigurations = new std::set<mxs::config::Configuration*>;
        }
    }

    static void finish()
    {
        mxb_assert(ThisUnit::pConfigurations);

        if (ThisUnit::pConfigurations->empty())
        {
            delete ThisUnit::pConfigurations;
            ThisUnit::pConfigurations = nullptr;
        }
    }
};

std::set<mxs::config::Configuration*>* ThisUnit::pConfigurations { nullptr };


using namespace maxscale::config;

bool is_core_param(Specification::Kind kind, const std::string& param)
{
    bool rv = false;

    switch (kind)
    {
    case Specification::FILTER:
        return FilterDef::specification()->find_param(param);
        break;

    case Specification::MONITOR:
        return mxs::Monitor::specification()->find_param(param);
        break;

    case Specification::ROUTER:
        return ::Service::specification()->find_param(param);
        break;

    case Specification::GLOBAL:
        break;

    case Specification::LISTENER:
    case Specification::PROTOCOL:
        return mxs::Listener::specification()->find_param(param);

    case Specification::SERVER:
        break;

    default:
        mxb_assert(!true);
    }

    return rv;
}
}

namespace maxscale
{

namespace config
{

/**
 * class Specification
 */
Specification::Specification(const char* zModule, Kind kind, const char* zPrefix)
    : m_module(zModule)
    , m_kind(kind)
    , m_prefix(zPrefix)
{
}

Specification::~Specification()
{
}

const string& Specification::module() const
{
    return m_module;
}

const string& Specification::prefix() const
{
    return m_prefix;
}

const Param* Specification::find_param(const string& name) const
{
    auto it = m_params.find(name);

    return it != m_params.end() ? it->second : nullptr;
}

ostream& Specification::document(ostream& out) const
{
    for (const auto& entry : m_params)
    {
        out << entry.second->documentation() << endl;
    }

    return out;
}

bool Specification::mandatory_params_defined(const std::set<std::string>& provided) const
{
    bool valid = true;

    for (const auto& entry : m_params)
    {
        const Param* pParam = entry.second;

        if (pParam->is_mandatory() && (provided.find(pParam->name()) == provided.end()))
        {
            MXB_ERROR("%s: The mandatory parameter '%s' is not provided.",
                      m_module.c_str(), pParam->name().c_str());
            valid = false;
        }
    }

    return valid;
}

bool Specification::validate(const Configuration* pConfig,
                             const mxs::ConfigParameters& params,
                             mxs::ConfigParameters* pUnrecognized) const
{
    bool valid = true;

    map<string, mxs::ConfigParameters> nested_parameters;
    map<string, const Param*> parameters_with_params;
    set<string> provided;

    for (const auto& param : params)
    {
        const auto& name = param.first;
        const auto& value = param.second;

        auto i = name.find('.');

        if (i != string::npos)
        {
            string head = name.substr(0, i);
            string tail = name.substr(i + 1);
            mxb::lower_case(head);

            // TODO: This assumption is currently slightly broken in the way that parameter_prefix() is used
            // to determine which parameters have a prefix value but the actual nested parameters are always
            // assumed to be a module name.
            nested_parameters[module_get_effective_name(head)].set(tail, value);
        }
        else
        {
            const Param* pParam = find_param(name.c_str());

            if (pParam)
            {
                provided.insert(name);

                bool param_valid = true;
                string message;

                if (pParam->validate(value.c_str(), &message))
                {
                    if (pParam->takes_parameters())
                    {
                        std::string real_name = pParam->parameter_prefix(value);
                        parameters_with_params[mxb::lower_case_copy(real_name)] = pParam;
                    }
                }
                else
                {
                    param_valid = false;
                    valid = false;
                }

                if (!message.empty())
                {
                    MXB_LOG_MESSAGE(param_valid ? LOG_WARNING : LOG_ERR,
                                    "%s: %s", name.c_str(), message.c_str());
                }
            }
            else if (!is_core_param(m_kind, name))
            {
                if (pUnrecognized)
                {
                    pUnrecognized->set(name, value);
                }
                else
                {
                    MXB_ERROR("%s: The parameter '%s' is unrecognized.", m_module.c_str(), name.c_str());
                    valid = false;
                }
            }
        }
    }

    if (valid)
    {
        if (mandatory_params_defined(provided))
        {
            map<string, mxs::ConfigParameters> unrecognized_parameters = nested_parameters;

            for (const auto& kv : parameters_with_params)
            {
                const auto& my_params = nested_parameters[kv.first];
                mxs::ConfigParameters unrecognized;
                bool param_valid = kv.second->validate_parameters(kv.first, my_params, &unrecognized);

                if (!unrecognized.empty())
                {
                    for (const auto& unknown : unrecognized)
                    {
                        if (pUnrecognized)
                        {
                            // If reporting upwards, we use the qualified name.
                            string qname = kv.first + "." + unknown.first;
                            pUnrecognized->set(qname, unknown.second);
                        }
                        else
                        {
                            // Otherwise, we report in the context of the module.
                            MXB_ERROR("%s: The parameter '%s' is unrecognized.",
                                      kv.first.c_str(), unknown.first.c_str());
                            param_valid = false;
                        }
                    }
                }

                if (!param_valid || !unrecognized.empty())
                {
                    valid = false;
                }

                // Remove the parameter once we've processed it. This will leave only unrecognized nested
                // parameters inside unrecognized_parameters once we're done.
                unrecognized_parameters.erase(kv.first);
            }

            for (const auto& kv : unrecognized_parameters)
            {
                for (const auto& p : kv.second)
                {
                    auto key = kv.first + "." + p.first;

                    if (pUnrecognized)
                    {
                        pUnrecognized->set(key, p.second);
                    }
                    else
                    {
                        MXB_ERROR("The parameter '%s' is unrecognized.", key.c_str());
                        valid = false;
                    }
                }
            }

            if (valid)
            {
                valid = post_validate(pConfig, params, nested_parameters);
            }
        }
        else
        {
            valid = false;
        }
    }

    return valid;
}

bool Specification::validate(const Configuration* pConfig,
                             json_t* pParams,
                             std::set<std::string>* pUnrecognized) const
{
    bool valid = true;

    map<string, json_t*> nested_parameters;
    map<string, const Param*> parameters_with_params;
    set<string> provided;

    const char* zKey;
    json_t* pValue;
    json_object_foreach(pParams, zKey, pValue)
    {
        if (json_typeof(pValue) == JSON_OBJECT && find_param(zKey) == nullptr)
        {
            // If the value is an object and there is no parameter with the
            // specified key, we assume it is the configuration of a nested object.
            nested_parameters[module_get_effective_name(zKey)] = pValue;
        }
        else
        {
            if (const Param* pParam = find_param(zKey))
            {
                provided.insert(zKey);

                string message;
                bool param_valid = true;

                if (pParam->validate(pValue, &message))
                {
                    if (pParam->takes_parameters())
                    {
                        mxb_assert(json_typeof(pValue) == JSON_STRING);

                        if (json_typeof(pValue) == JSON_STRING)
                        {
                            std::string real_name = pParam->parameter_prefix(json_string_value(pValue));
                            parameters_with_params[mxb::lower_case_copy(real_name)] = pParam;
                        }
                    }
                }
                else
                {
                    param_valid = false;
                    valid = false;
                }

                if (!message.empty())
                {
                    MXB_LOG_MESSAGE(param_valid ? LOG_WARNING : LOG_ERR, "%s: %s", zKey, message.c_str());
                }
            }
            else if (!is_core_param(m_kind, zKey))
            {
                if (pUnrecognized)
                {
                    pUnrecognized->insert(zKey);
                }
                else
                {
                    MXB_ERROR("%s: The parameter '%s' is unrecognized.", m_module.c_str(), zKey);
                    valid = false;
                }
            }
        }
    }

    if (valid)
    {
        if (mandatory_params_defined(provided))
        {
            map<string, json_t*> unrecognized_parameters = nested_parameters;

            for (const auto& kv : parameters_with_params)
            {
                const auto& my_params = nested_parameters[kv.first];
                set<string> unrecognized;
                bool param_valid = kv.second->validate_parameters(kv.first, my_params, &unrecognized);

                if (!unrecognized.empty())
                {
                    for (const auto& s : unrecognized)
                    {
                        if (pUnrecognized)
                        {
                            // If reporting upwards, we use the qualified name.
                            string qname = kv.first + "." + s;
                            pUnrecognized->insert(qname);
                        }
                        else
                        {
                            // Otherwise, we report in the context of the module.
                            MXB_ERROR("%s: The parameter '%s' is unrecognized.",
                                      kv.first.c_str(), s.c_str());
                            param_valid = false;
                        }
                    }
                }

                if (!param_valid || !unrecognized.empty())
                {
                    valid = false;
                }

                // Remove the parameter once we've processed it. This will leave only unrecognized nested
                // parameters inside unrecognized_parameters once we're done.
                unrecognized_parameters.erase(kv.first);
            }


            for (const auto& kv : unrecognized_parameters)
            {
                const char* k;
                json_t* v;
                json_object_foreach(kv.second, k, v)
                {
                    std::string key = kv.first + "." + k;

                    if (pUnrecognized)
                    {
                        pUnrecognized->insert(key);
                    }
                    else
                    {
                        MXB_ERROR("The parameter '%s' is unrecognized.", key.c_str());
                        valid = false;
                    }
                }
            }

            if (valid)
            {
                valid = post_validate(pConfig, pParams, nested_parameters);
            }
        }
        else
        {
            valid = false;
        }
    }

    return valid;
}

size_t Specification::size() const
{
    return m_params.size();
}

void Specification::insert(Param* pParam)
{
    mxb_assert(m_params.find(pParam->name()) == m_params.end());

    m_params.insert(make_pair(pParam->name(), pParam));
}

void Specification::remove(Param* pParam)
{
    auto it = m_params.find(pParam->name());
    mxb_assert(it != m_params.end());

    m_params.erase(it);
}

void Specification::insert(server::Dependency* pDependency)
{
    mxb_assert(m_server_dependencies.find(pDependency) == m_server_dependencies.end());

    m_server_dependencies.insert(pDependency);
}

void Specification::remove(server::Dependency* pDependency)
{
    mxb_assert(m_server_dependencies.find(pDependency) != m_server_dependencies.end());

    m_server_dependencies.erase(pDependency);
}

json_t* Specification::to_json() const
{
    json_t* pSpecification = json_array();

    for (const auto& kv : m_params)
    {
        const Param* pParam = kv.second;
        json_array_append_new(pSpecification, pParam->to_json());
    }

    return pSpecification;
}

/**
 * class Param
 */
Param::Param(Specification* pSpecification,
             const char* zName,
             const char* zDescription,
             Modifiable modifiable,
             Kind kind)
    : m_specification(*pSpecification)
    , m_name(zName)
    , m_description(zDescription)
    , m_modifiable(modifiable)
    , m_kind(kind)
{
    m_specification.insert(this);
}

Param::~Param()
{
    m_specification.remove(this);
}

const string& Param::name() const
{
    return m_name;
}

const string& Param::description() const
{
    return m_description;
}

std::string Param::documentation() const
{
    std::stringstream ss;

    ss << m_name << " (" << type() << ", ";

    if (is_mandatory())
    {
        ss << "mandatory";
    }
    else
    {
        ss << "optional, default: " << default_to_string();
    }

    ss << "): " << m_description;

    return ss.str();
}

Param::Kind Param::kind() const
{
    return m_kind;
}

bool Param::is_mandatory() const
{
    return m_kind == MANDATORY;
}

bool Param::is_optional() const
{
    return m_kind == OPTIONAL;
}

bool Param::has_default_value() const
{
    return is_optional();
}

bool Param::takes_parameters() const
{
    return false;
}

std::string Param::parameter_prefix(const std::string& value) const
{
    return value;
}

bool Param::validate_parameters(const std::string& value,
                                const mxs::ConfigParameters& params,
                                mxs::ConfigParameters* pUnrecognized) const
{
    if (pUnrecognized)
    {
        *pUnrecognized = params;
    }

    // No nested parameters for a parameter not taking nested parameters is valid.
    return params.empty();
}

bool Param::validate_parameters(const std::string& value,
                                json_t* pParams,
                                std::set<std::string>* pUnrecognized) const
{
    if (pUnrecognized)
    {
        const char* zKey;
        json_t* pValue;
        json_object_foreach(pParams, zKey, pValue)
        {
            pUnrecognized->insert(zKey);
        }
    }

    // No nested parameters for a parameter not taking nested parameters is valid.
    return json_object_size(pParams) == 0;
}

Param::Modifiable Param::modifiable() const
{
    return m_modifiable;
}

json_t* Param::to_json() const
{
    const char CN_MANDATORY[] = "mandatory";
    const char CN_MODIFIABLE[] = "modifiable";

    json_t* pJson = json_object();

    json_object_set_new(pJson, CN_NAME, json_string(m_name.c_str()));
    json_object_set_new(pJson, CN_DESCRIPTION, json_string(m_description.c_str()));
    json_object_set_new(pJson, CN_TYPE, json_string(type().c_str()));
    json_object_set_new(pJson, CN_MANDATORY, json_boolean(is_mandatory()));
    json_object_set_new(pJson, CN_MODIFIABLE, json_boolean(is_modifiable_at_runtime()));

    if (is_deprecated())
    {
        json_object_set_new(pJson, CN_DEPRECATED, json_boolean(true));
    }

    return pJson;
}

std::vector<std::string> Param::get_dependencies(const std::string& value) const
{
    return {};
}

/**
 * class Configuration
 */
Configuration::Configuration(const std::string& name, const config::Specification* pSpecification)
    : m_name(name)
    , m_pSpecification(pSpecification)
{
    ThisUnit::insert(this);
}

Configuration::Configuration(Configuration&& rhs)
    : m_name(std::move(rhs.m_name))
    , m_pSpecification(std::move(rhs.m_pSpecification))
    , m_values(std::move(rhs.m_values))
    , m_natives(std::move(rhs.m_natives))
{
    ThisUnit::insert(this);

    for (auto& kv : m_values)
    {
        Type* pType = kv.second;

        pType->m_pConfiguration = this;
    }
}

Configuration::~Configuration()
{
    ThisUnit::remove(this);
}

const std::set<Configuration*>& Configuration::all()
{
    mxb_assert(MainWorker::is_current());
    mxb_assert(ThisUnit::pConfigurations);

    return *ThisUnit::pConfigurations;
}

Configuration& Configuration::operator=(Configuration&& rhs)
{
    if (this != &rhs)
    {
        m_name = std::move(rhs.m_name);
        m_pSpecification = std::move(rhs.m_pSpecification);
        m_values = std::move(rhs.m_values);
        m_natives = std::move(rhs.m_natives);

        for (auto& kv : m_values)
        {
            Type* pType = kv.second;

            pType->m_pConfiguration = this;
        }
    }

    return *this;
}

const std::string& Configuration::name() const
{
    return m_name;
}

const config::Specification& Configuration::specification() const
{
    return *m_pSpecification;
}

bool Configuration::configure(const mxs::ConfigParameters& params,
                              mxs::ConfigParameters* pUnrecognized)
{
    // Aliases need not be pruned here, because 'params' is coming from a config
    // file and issues can occur only if a user configures in the same section
    // using both the actual name and the alias.

    mxs::ConfigParameters unrecognized;
    mxb_assert(validate(params, &unrecognized));
    mxb_assert(m_pSpecification->size() >= size());

    bool configured = true;

    map<string, mxs::ConfigParameters> nested_parameters;

    for (const auto& param : params)
    {
        const auto& name = param.first;
        const auto& value = param.second;

        auto i = name.find('.');

        if (i != string::npos)
        {
            string head = name.substr(0, i);
            string tail = name.substr(i + 1);

            nested_parameters[module_get_effective_name(head)].set(tail, value);
        }
        else
        {
            if (config::Type* pValue = find_value(name.c_str()))
            {
                string message;
                if (!pValue->set_from_string(value, &message))
                {
                    MXB_ERROR("%s: %s", m_pSpecification->module().c_str(), message.c_str());
                    configured = false;
                }
            }
            else if (!is_core_param(m_pSpecification->kind(), name))
            {
                if (pUnrecognized)
                {
                    pUnrecognized->set(name, value);
                }
                else
                {
                    MXB_ERROR("%s: The parameter '%s' is unrecognized.",
                              m_pSpecification->module().c_str(), name.c_str());
                    configured = false;
                }
            }
        }
    }

    if (configured)
    {
        m_first_time = false;
        configured = post_configure(nested_parameters);
    }

    return configured;
}

namespace
{

void insert_value(mxs::ConfigParameters& params, const char* zName, json_t* pValue)
{
    switch (json_typeof(pValue))
    {
    case JSON_STRING:
        params.set(zName, json_string_value(pValue));
        break;

    case JSON_INTEGER:
        params.set(zName, std::to_string(json_integer_value(pValue)));
        break;

    case JSON_REAL:
        params.set(zName, std::to_string(json_real_value(pValue)));
        break;

    case JSON_TRUE:
        params.set(zName, "true");
        break;

    case JSON_FALSE:
        params.set(zName, "false");
        break;

    case JSON_OBJECT:
        MXB_WARNING("%s: Object value not supported, ignored.", zName);
        break;

    case JSON_ARRAY:
        MXB_WARNING("%s: Array value not supported, ignored.", zName);
        break;

    case JSON_NULL:
        MXB_WARNING("%s: NULL value not supported, ignored.", zName);
        break;
    }
}
}

namespace
{

void prune_aliases(json_t* pJson, const Specification& specification)
{
    const char* zKey;
    json_t* pValue;
    void* pTmp;
    json_object_foreach_safe(pJson, pTmp, zKey, pValue)
    {
        if (const Param* pParam = specification.find_param(zKey))
        {
            if (pParam->final_name() != pParam->name())
            {
                json_object_set(pJson, pParam->final_name().c_str(), pValue);
                json_object_del(pJson, pParam->name().c_str());
            }
        }
    }
}
}

bool Configuration::configure(json_t* json, std::set<std::string>* pUnrecognized)
{
    prune_aliases(json, specification());

    set<string> unrecognized;
    mxb_assert(validate(json, &unrecognized));
    mxb_assert(m_pSpecification->size() >= size());

    bool configured = true;
    bool changed = false;

    map<string, mxs::ConfigParameters> nested_parameters;

    const char* key;
    json_t* value;

    if (!m_first_time)
    {
        // Check that only parameters that support reconfiguration are being modified.
        json_object_foreach(json, key, value)
        {
            if (config::Type* pValue = find_value(key))
            {
                if (!pValue->is_equal(value) && !pValue->parameter().is_modifiable_at_runtime())
                {
                    MXB_ERROR("%s: The parameter '%s' cannot be modified at runtime.",
                              m_pSpecification->module().c_str(), key);
                    configured = false;
                }
            }
        }
    }

    if (!configured)
    {
        return configured;
    }

    json_object_foreach(json, key, value)
    {
        if (json_typeof(value) == JSON_OBJECT && find_value(key) == nullptr)
        {
            // If the value is an object and there is no parameter with the
            // specified key, we assume it is the configuration of a nested object.
            const char* zNested_key;
            json_t* pNested_value;
            json_object_foreach(value, zNested_key, pNested_value)
            {
                // TODO: We throw away information here, but no can do for the time being.
                insert_value(nested_parameters[module_get_effective_name(key)], zNested_key, pNested_value);
            }
        }
        else
        {
            if (config::Type* pValue = find_value(key))
            {
                if (!pValue->is_equal(value))
                {
                    changed = true;
                    string message;

                    if (!pValue->set_from_json(value, &message))
                    {
                        MXB_ERROR("%s: %s", m_pSpecification->module().c_str(), message.c_str());
                        configured = false;
                    }
                }
            }
            else if (!is_core_param(m_pSpecification->kind(), key))
            {
                if (pUnrecognized)
                {
                    pUnrecognized->insert(key);
                }
                else
                {
                    MXB_ERROR("%s: The parameter '%s' is unrecognized.",
                              m_pSpecification->module().c_str(), key);
                    configured = false;
                }
            }
        }
    }

    if (configured)
    {
        // Always call post_configure() even if no changes were done. This makes sure that any side-effects
        // that the post_configure call has (e.g. reading files from disk) are re-applied when an attempt to
        // modify the configuration is made.
        m_first_time = false;

        // Mark the configuration as changed on the first attempt even if it really didn't
        changed = true;

        configured = post_configure(nested_parameters);
    }

    m_was_modified = changed;

    return configured;
}

Type* Configuration::find_value(const string& name)
{
    Type* pType = nullptr;
    const Param* pParam = m_pSpecification->find_param(name);

    if (pParam)
    {
        auto it = m_values.find(pParam->final_name());

        if (it != m_values.end())
        {
            pType = it->second;
        }
    }

    return pType;
}

const Type* Configuration::find_value(const string& name) const
{
    return const_cast<Configuration*>(this)->find_value(name);
}

ostream& Configuration::persist(ostream& out, const std::set<std::string>& force_persist) const
{
    out << '[' << m_name << ']' << '\n';
    return persist_append(out, force_persist);
}

ostream& Configuration::persist_append(ostream& out, const std::set<std::string>& force_persist) const
{
    for (const auto& entry : m_values)
    {
        Type* pValue = entry.second;

        if (pValue->parameter().is_mandatory()
            || force_persist.count(pValue->parameter().name())
            || pValue->to_string() != pValue->parameter().default_to_string())
        {
            auto str = pValue->persist();

            if (!str.empty())
            {
                if (!m_pSpecification->prefix().empty())
                {
                    out << m_pSpecification->prefix() << '.';
                }

                out << str << '\n';
            }
        }
    }

    return out;
}

void Configuration::fill(json_t* pObj) const
{
    json_t* pJson;

    if (!m_pSpecification->prefix().empty())
    {
        pJson = json_object();
        json_object_set_new(pObj, m_pSpecification->prefix().c_str(), pJson);
    }
    else
    {
        pJson = pObj;
    }

    for (const auto& kv : m_values)
    {
        const Type* pType = kv.second;

        json_object_set_new(pJson, kv.first.c_str(), pType->to_json());
    }
}

void Configuration::insert(Type* pValue)
{
    mxb_assert(m_values.find(pValue->parameter().name()) == m_values.end());

    m_values.insert(make_pair(pValue->parameter().name(), pValue));
}

void Configuration::remove(Type* pValue, const std::string& name)
{
    auto it = m_values.find(name);

    mxb_assert(it != m_values.end());
    mxb_assert(it->second == pValue);
    m_values.erase(it);
}

bool Configuration::post_configure(const std::map<string, mxs::ConfigParameters>& nested_params)
{
    return nested_params.empty();
}

size_t Configuration::size() const
{
    return m_values.size();
}

json_t* Configuration::to_json() const
{
    json_t* pConfiguration = json_object();
    fill(pConfiguration);
    return pConfiguration;
}

mxs::ConfigParameters Configuration::to_params() const
{
    mxs::ConfigParameters params;

    for (const auto& kv : m_values)
    {
        params.set(kv.first, kv.second->to_string());
    }

    return params;
}

/**
 * class Type
 */
Type::Type(Configuration* pConfiguration, const config::Param* pParam)
    : m_pConfiguration(pConfiguration)
    , m_pParam(pParam)
    , m_name(pParam->name())
{
    // The name is copied, so that we have access to it in the destructor
    // also in the case that Param happens to be destructed first.
    m_pConfiguration->insert(this);
}

Type::Type(Type&& rhs)
    : m_pConfiguration(rhs.m_pConfiguration)
    , m_pParam(rhs.m_pParam)
    , m_name(std::move(rhs.m_name))
{
    m_pConfiguration->remove(&rhs, m_name);
    m_pConfiguration->insert(this);
    rhs.m_pConfiguration = nullptr;
}

Type& Type::operator=(Type&& rhs)
{
    if (this != &rhs)
    {
        m_pConfiguration = rhs.m_pConfiguration;
        m_pParam = rhs.m_pParam;
        m_name = std::move(rhs.m_name);
        rhs.m_pConfiguration = nullptr;

        m_pConfiguration->remove(&rhs, m_name);
        m_pConfiguration->insert(this);
    }

    return *this;
}

Type::~Type()
{
    if (m_pConfiguration)
    {
        m_pConfiguration->remove(this, m_name);
    }
}

const config::Param& Type::parameter() const
{
    return *m_pParam;
}

std::string Type::persist() const
{
    std::ostringstream out;
    auto strval = to_string();

    if (!strval.empty())
    {
        out << m_pParam->name() << '=' << strval;
    }

    return out.str();
}

/**
 * ParamBool
 */
std::string ParamBool::type() const
{
    return "bool";
}

string ParamBool::to_string(value_type value) const
{
    return value ? "true" : "false";
}

bool ParamBool::from_string(const string& value_as_string, value_type* pValue, string* pMessage) const
{
    int rv = config_truth_value(value_as_string.c_str());

    if (rv == 1)
    {
        *pValue = true;
    }
    else if (rv == 0)
    {
        *pValue = false;
    }
    else if (pMessage)
    {
        mxb_assert(rv == -1);

        *pMessage = "Invalid boolean: ";
        *pMessage += value_as_string;
    }

    return rv != -1;
}

json_t* ParamBool::to_json(value_type value) const
{
    return json_boolean(value);
}

bool ParamBool::from_json(const json_t* pJson, value_type* pValue, string* pMessage) const
{
    bool rv = false;

    if (json_is_boolean(pJson))
    {
        *pValue = json_boolean_value(pJson) ? true : false;
        rv = true;
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json boolean, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

/**
 * ParamNumber
 */
std::string ParamNumber::to_string(value_type value) const
{
    return std::to_string(value);
}

bool ParamNumber::from_string(const std::string& value_as_string,
                              value_type* pValue,
                              std::string* pMessage) const
{
    const char* zValue = value_as_string.c_str();
    char* zEnd;
    errno = 0;
    long l = strtol(zValue, &zEnd, 10);

    bool rv = errno == 0 && zEnd != zValue && *zEnd == 0;

    if (rv)
    {
        rv = from_value(l, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Invalid ";
        *pMessage += type();
        *pMessage += ": ";
        *pMessage += value_as_string;
    }

    return rv;
}

json_t* ParamNumber::to_json(value_type value) const
{
    return json_integer(value);
}

bool ParamNumber::from_json(const json_t* pJson, value_type* pValue,
                            std::string* pMessage) const
{
    bool rv = false;

    if (json_is_integer(pJson))
    {
        value_type value = json_integer_value(pJson);

        rv = from_value(value, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json integer, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

bool ParamNumber::from_value(value_type value,
                             value_type* pValue,
                             std::string* pMessage) const
{
    bool rv = value >= m_min_value && value <= m_max_value;

    if (rv)
    {
        *pValue = value;
    }
    else if (pMessage)
    {
        if (value < m_min_value)
        {
            *pMessage = "Too small a ";
        }
        else
        {
            mxb_assert(value >= m_max_value);
            *pMessage = "Too large a ";
        }

        *pMessage += type();
        *pMessage += ": ";
        *pMessage += std::to_string(value);
    }

    return rv;
}

/**
 * ParamCount
 */
std::string ParamCount::type() const
{
    return "count";
}

/**
 * ParamPercent
 */

std::string ParamPercent::type() const
{
    return "percent";
}

json_t* ParamPercent::to_json() const
{
    json_t* rv = ParamCount::to_json();

    json_object_set_new(rv, "unit", json_string("%"));

    return rv;
}

std::string ParamPercent::to_string(value_type value) const
{
    string rv = ParamCount::to_string(value);
    rv += "%";
    return rv;
}

bool ParamPercent::from_string(const std::string& value_as_string,
                               value_type* pValue,
                               std::string* pMessage) const
{
    bool rv = false;
    auto size = value_as_string.size();

    if (size == 0 || value_as_string.back() != '%')
    {
        if (pMessage)
        {
            *pMessage = "Invalid percent: ";
            *pMessage += value_as_string;
        }
    }
    else
    {
        rv = ParamCount::from_string(value_as_string.substr(0, size - 1), pValue, pMessage);
    }

    return rv;
}

json_t* ParamPercent::to_json(value_type value) const
{
    return json_string(to_string(value).c_str());
}

bool ParamPercent::from_json(const json_t* pJson, value_type* pValue, std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        rv = from_string(json_string_value(pJson), pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string with a percent, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

/**
 * ParamInteger
 */
std::string ParamInteger::type() const
{
    return "int";
}

/**
 * ParamHost
 */
std::string ParamHost::type() const
{
    return "host";
}

std::string ParamHost::to_string(const value_type& value) const
{
    return value.org_input();
}

bool ParamHost::from_string(const std::string& value_as_string,
                            value_type* pValue,
                            std::string* pMessage) const
{
    mxb::Host host = mxb::Host::from_string(value_as_string, m_default_port);

    if (host.is_valid())
    {
        *pValue = host;
    }
    else if (pMessage)
    {
        *pMessage = "'";
        *pMessage += value_as_string;
        *pMessage += "' is not a valid host port combination.";
    }

    return host.is_valid();
}

json_t* ParamHost::to_json(const value_type& value) const
{
    auto str = to_string(value);
    return str.empty() ? json_null() : json_string(str.c_str());
}

bool ParamHost::from_json(const json_t* pJson,
                          value_type* pValue,
                          std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

/**
 * ParamPath
 */
std::string ParamPath::type() const
{
    return "path";
}

std::string ParamPath::to_string(const value_type& value) const
{
    return value;
}

bool ParamPath::from_string(const std::string& value_as_string,
                            value_type* pValue,
                            std::string* pMessage) const
{
    bool valid = is_valid(value_as_string.c_str());

    if (valid)
    {
        *pValue = value_as_string;
    }
    else if (pMessage)
    {
        *pMessage = "Invalid path (does not exist, required permissions are not granted, ";
        *pMessage += "or cannot be created): ";
        *pMessage += value_as_string;
    }

    return valid;
}

json_t* ParamPath::to_json(const value_type& value) const
{
    return value.empty() ? json_null() : json_string(value.c_str());
}

bool ParamPath::from_json(const json_t* pJson, value_type* pValue,
                          std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

// static
bool ParamPath::is_valid_path(uint32_t options, const value_type& value)
{
    if (value.empty())
    {
        return true;
    }

    bool valid = false;

    if (options & (W | R | X | F))
    {
        std::string buf;

        if (value.front() != '/')
        {
            buf = clean_up_pathname(mxb::string_printf("/%s/%s", mxs::module_configdir(), value.c_str()));
        }
        else
        {
            buf = value;
        }

        int mode = F_OK;
        int mask = 0;

        if (options & W)
        {
            mask |= S_IWUSR | S_IWGRP;
            mode |= W_OK;
        }
        if (options & R)
        {
            mask |= S_IRUSR | S_IRGRP;
            mode |= R_OK;
        }
        if (options & X)
        {
            mask |= S_IXUSR | S_IXGRP;
            mode |= X_OK;
        }

        if (access(buf.c_str(), mode) == 0)
        {
            valid = true;
        }
        else
        {
            /** Save errno as we do a second call to `accept` */
            int er = errno;

            if (access(buf.c_str(), F_OK) == 0 || (options & C) == 0)
            {
                /**
                 * Path already exists and it doesn't have the requested access
                 * right or the module doesn't want the directory to be created
                 * if it doesn't exist.
                 */
                MXB_ERROR("Bad path parameter '%s' (absolute path '%s'): %d, %s",
                          value.c_str(), buf.c_str(), er, mxb_strerror(er));
            }
            else if (mxs_mkdir_all(buf.c_str(), mask))
            {
                /** Successfully created path */
                valid = true;
            }
            else
            {
                /** Failed to create the directory, errno is set in `mxs_mkdir_all` */
                MXB_ERROR("Can't create path '%s' (absolute path '%s'): %d, %s",
                          value.c_str(), buf.c_str(), errno, mxb_strerror(errno));
            }
        }
    }
    else
    {
        /** No checks for the path are required */
        valid = true;
    }

    return valid;
}

/**
 * ParamPathList
 */
std::string ParamPathList::type() const
{
    return "pathlist";
}

std::string ParamPathList::to_string(const value_type& value) const
{
    return mxb::join(value, ":");
}

bool ParamPathList::from_string(const std::string& value_as_string,
                                value_type* pValue,
                                std::string* pMessage) const
{
    bool valid = true;
    auto paths = mxb::strtok(value_as_string, ":");

    for (const auto& path : paths)
    {
        if (ParamPath::is_valid_path(m_options, path))
        {
            pValue->push_back(path);
        }
        else
        {
            if (pMessage)
            {
                *pMessage = "Invalid path: " + path;
            }

            valid = false;
            break;
        }
    }

    return valid;
}

json_t* ParamPathList::to_json(const value_type& value) const
{
    json_t* arr;

    if (value.empty())
    {
        arr = json_null();
    }
    else
    {
        arr = json_array();

        for (const auto& path : value)
        {
            json_array_append_new(arr, json_string(path.c_str()));
        }
    }

    return arr;
}

bool ParamPathList::from_json(const json_t* pJson, value_type* pValue,
                              std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        rv = from_string(json_string_value(pJson), pValue, pMessage);
    }
    else if (json_is_array(pJson))
    {
        rv = true;

        size_t i;
        json_t* val;

        json_array_foreach(pJson, i, val)
        {
            if (json_is_string(val))
            {
                std::string path = json_string_value(val);

                if (ParamPath::is_valid_path(m_options, path))
                {
                    pValue->push_back(path);
                }
                else
                {
                    if (pMessage)
                    {
                        *pMessage = "Invalid path: " + path;
                    }

                    rv = false;
                    break;
                }
            }
            else
            {
                rv = false;

                if (pMessage)
                {
                    *pMessage = "Expected a json string, but got a json "s + mxb::json_type_to_string(val);
                }
            }
        }
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json "s + mxb::json_type_to_string(pJson);
    }

    return rv;
}

/**
 * ParamRegex
 */
std::string ParamRegex::type() const
{
    return "regex";
}

std::string ParamRegex::to_string(const value_type& type) const
{
    return type.pattern();
}

namespace
{

bool regex_from_string(const std::string& value_as_string,
                       uint32_t options,
                       RegexValue* pValue,
                       std::string* pMessage = nullptr)
{
    bool rv = false;

    if (value_as_string.empty())
    {
        *pValue = RegexValue();
        rv = true;
    }
    else
    {
        bool slashes = false;

        if (value_as_string.length() >= 2)
        {
            slashes = value_as_string.front() == '/' && value_as_string.back() == '/';
        }

        if (!slashes)
        {
            if (pMessage)
            {
                *pMessage = "Missing slashes (/) around a regular expression is deprecated.";
            }
        }

        string text = value_as_string.substr(slashes ? 1 : 0, value_as_string.length() - (slashes ? 2 : 0));

        if (text.empty())
        {
            *pValue = RegexValue{};
            rv = true;
        }
        else
        {

            uint32_t jit_available = 0;
            pcre2_config(PCRE2_CONFIG_JIT, &jit_available);

            uint32_t ovec_size;
            std::unique_ptr<pcre2_code> sCode(compile_regex_string(text.c_str(),
                                                                   jit_available, options, &ovec_size));

            if (sCode)
            {
                RegexValue value(text, std::move(sCode), ovec_size, options);

                *pValue = value;
                rv = true;
            }
        }
    }

    return rv;
}
}

bool ParamRegex::from_string(const std::string& value_as_string,
                             value_type* pValue,
                             std::string* pMessage) const
{
    return regex_from_string(value_as_string, m_options, pValue, pMessage);
}

json_t* ParamRegex::to_json(const value_type& value) const
{
    return !value.empty() ? json_string(("/" + value.pattern() + "/").c_str()) : json_null();
}

bool ParamRegex::from_json(const json_t* pJson,
                           value_type* pValue,
                           std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

RegexValue ParamRegex::create_default(const char* zRegex)
{
    RegexValue value;

    MXB_AT_DEBUG(bool rv = ) regex_from_string(zRegex, 0, &value);
    mxb_assert(rv);

    return value;
}

RegexValue::RegexValue(const std::string& text, uint32_t options)
{
    MXB_AT_DEBUG(bool rv = ) regex_from_string(text.c_str(), options, this);
    mxb_assert(rv);
}

/**
 * ParamServer
 */
std::string ParamServer::type() const
{
    return "server";
}

std::string ParamServer::to_string(value_type value) const
{
    return value ? value->name() : "";
}

bool ParamServer::from_string(const std::string& value_as_string,
                              value_type* pValue,
                              std::string* pMessage) const
{
    bool rv = false;

    if (value_as_string.empty())
    {
        *pValue = nullptr;
        rv = true;
    }
    else
    {
        *pValue = SERVER::find_by_unique_name(value_as_string);

        if (*pValue)
        {
            rv = true;
        }
        else if (pMessage)
        {
            *pMessage = "Unknown server: ";
            *pMessage += value_as_string;
        }
    }

    return rv;
}

json_t* ParamServer::to_json(value_type value) const
{
    return value ? json_string(value->name()) : json_null();
}

bool ParamServer::from_json(const json_t* pJson, value_type* pValue,
                            std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

std::vector<std::string> ParamServer::get_dependencies(const std::string& value) const
{
    return {value};
}

/**
 * ParamServerList
 */

std::vector<std::string> ParamServerList::get_dependencies(const std::string& value) const
{
    return mxb::strtok(value, ",");
}

std::string ParamServerList::type() const
{
    return "serverlist";
}

std::string ParamServerList::to_string(value_type value) const
{
    std::vector<std::string> str(value.size());
    std::transform(value.begin(), value.end(), str.begin(), std::mem_fn(&SERVER::name));
    return mxb::join(str, ",");
}

bool ParamServerList::from_string(const std::string& value_as_string, value_type* pValue,
                                  std::string* pMessage) const
{
    bool rv = true;

    if (!value_as_string.empty())
    {
        std::vector<std::string> unknown;

        for (auto val : mxb::strtok(value_as_string, ","))
        {
            mxb::trim(val);
            if (SERVER* s = SERVER::find_by_unique_name(val))
            {
                pValue->push_back(s);
            }
            else
            {
                unknown.push_back(val);
                rv = false;
            }
        }

        if (pMessage && !rv)
        {
            *pMessage = "Unknown servers: ";
            *pMessage += mxb::join(unknown, ",");
        }
    }

    return rv;
}

json_t* ParamServerList::to_json(value_type value) const
{
    return !value.empty() ? json_string(to_string(value).c_str()) : json_null();
}

bool ParamServerList::from_json(const json_t* pJson, value_type* pValue,
                                std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

/**
 * ParamModule
 */
ParamModule::value_type ParamModule::default_value() const
{
    value_type pModule = m_default_value;

    if (!pModule)
    {
        if (!m_default_module.empty())
        {
            pModule = get_module(m_default_module, m_module_type);
        }
        else
        {
            pModule = nullptr;
        }

        const_cast<ParamModule*>(this)->m_default_value = pModule;
    }

    return pModule;
}

std::string ParamModule::type() const
{
    return "module";
}

bool ParamModule::takes_parameters() const
{
    return true;
}

std::string ParamModule::parameter_prefix(const std::string& value) const
{
    const MXS_MODULE* pModule = get_module(value, m_module_type);
    return pModule ? pModule->name : value;
}

bool ParamModule::validate_parameters(const std::string& value,
                                      const mxs::ConfigParameters& params,
                                      mxs::ConfigParameters* pUnrecognized) const
{
    const MXS_MODULE* pModule = get_module(value, m_module_type);
    const mxs::config::Specification* pSpecification = pModule ? pModule->specification : nullptr;

    bool valid;
    if (!pSpecification || pSpecification->prefix().empty())
    {
        // The module does not expect nested parameters.
        valid = Param::validate_parameters(value, params, pUnrecognized);
    }
    else
    {
        valid = pSpecification->validate(params, pUnrecognized);
    }

    return valid;
}

bool ParamModule::validate_parameters(const std::string& value,
                                      json_t* pParams,
                                      std::set<std::string>* pUnrecognized) const
{
    const MXS_MODULE* pModule = get_module(value, m_module_type);
    const mxs::config::Specification* pSpecification = pModule ? pModule->specification : nullptr;

    bool valid;
    if (!pSpecification || pSpecification->prefix().empty())
    {
        // The module does not expect nested parameters.
        valid = Param::validate_parameters(value, pParams, pUnrecognized);
    }
    else
    {
        valid = pSpecification->validate(pParams, pUnrecognized);
    }

    return valid;
}

std::string ParamModule::to_string(value_type value) const
{
    return value ? value->name : "";
}

bool ParamModule::from_string(const std::string& value_as_string,
                              value_type* pValue,
                              std::string* pMessage) const
{
    bool rv = false;

    if (value_as_string.empty())
    {
        *pValue = nullptr;
        // TODO: Also ok for modules? In other contexts an empty string
        // TODO: is ok, but in the case of modules?
        rv = true;
    }
    else
    {
        *pValue = get_module(value_as_string, m_module_type);

        if (*pValue)
        {
            rv = true;
        }
        else if (pMessage)
        {
            *pMessage = "'";
            *pMessage += value_as_string;
            *pMessage += "' does not refer to a module, or refers to module of the wrong type.";
        }
    }

    return rv;
}

json_t* ParamModule::to_json(value_type value) const
{
    return value ? json_string(value->name) : json_null();
}

bool ParamModule::from_json(const json_t* pJson, value_type* pValue,
                            std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

/**
 * ParamTarget
 */
std::string ParamTarget::type() const
{
    return "target";
}

std::string ParamTarget::to_string(value_type value) const
{
    return value ? value->name() : "";
}

bool ParamTarget::from_string(const std::string& value_as_string,
                              value_type* pValue,
                              std::string* pMessage) const
{
    *pValue = mxs::Target::find(value_as_string);

    if (!*pValue && pMessage)
    {
        *pMessage = "Unknown target: ";
        *pMessage += value_as_string;
    }

    return *pValue;
}

json_t* ParamTarget::to_json(value_type value) const
{
    return value ? json_string(value->name()) : json_null();
}

bool ParamTarget::from_json(const json_t* pJson, value_type* pValue,
                            std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

std::vector<std::string> ParamTarget::get_dependencies(const std::string& value) const
{
    return {value};
}

/**
 * ParamService
 */
std::string ParamService::type() const
{
    return "service";
}

std::string ParamService::to_string(value_type value) const
{
    return value ? value->name() : "";
}

bool ParamService::from_string(const std::string& value_as_string,
                               value_type* pValue,
                               std::string* pMessage) const
{
    *pValue = ::Service::find(value_as_string);

    if (!*pValue && pMessage)
    {
        *pMessage = "Unknown Service: " + value_as_string;
    }

    return *pValue;
}

json_t* ParamService::to_json(value_type value) const
{
    return value ? json_string(value->name()) : json_null();
}

bool ParamService::from_json(const json_t* pJson, value_type* pValue,
                             std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        const char* z = json_string_value(pJson);

        rv = from_string(z, pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

std::vector<std::string> ParamService::get_dependencies(const std::string& value) const
{
    return {value};
}

/**
 * ParamSize
 */
std::string ParamSize::type() const
{
    return "size";
}

std::string ParamSize::to_string(value_type value) const
{
    // TODO: Use largest possible unit.
    return std::to_string(value);
}

bool ParamSize::from_string(const std::string& value_as_string,
                            value_type* pValue,
                            std::string* pMessage) const
{
    uint64_t value;
    bool valid = get_suffixed_size(value_as_string.c_str(), &value);

    if (!valid && pMessage)
    {
        *pMessage = "Invalid size: ";
        *pMessage += value_as_string;
    }
    else
    {
        valid = from_value(value, pValue, pMessage);
    }

    return valid;
}

json_t* ParamSize::to_json(value_type value) const
{
    return json_integer(value);
}

bool ParamSize::from_json(const json_t* pJson,
                          value_type* pValue,
                          std::string* pMessage) const
{
    bool rv = false;

    if (json_is_integer(pJson))
    {
        rv = from_value(json_integer_value(pJson), pValue, pMessage);
    }
    else if (json_is_string(pJson))
    {
        rv = from_string(json_string_value(pJson), pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

/**
 * ParamString
 */
std::string ParamString::type() const
{
    return "string";
}

std::string ParamString::to_string(value_type value) const
{
    std::string rval;

    if (!value.empty())
    {
        if (m_quotes != Quotes::IGNORED || isspace(value.front()) || isspace(value.back()))
        {
            rval = '"' + value + '"';
        }
        else
        {
            rval = value;
        }
    }

    return rval;
}

bool ParamString::from_string(const std::string& value_as_string,
                              value_type* pValue,
                              std::string* pMessage) const
{
    bool valid = true;

    char b = value_as_string.empty() ? 0 : value_as_string.front();
    char e = value_as_string.empty() ? 0 : value_as_string.back();

    if (b != '"' && b != '\'')
    {
        static const char zDesired[] = "The string value should be enclosed in quotes: ";
        static const char zRequired[] = "The string value must be enclosed in quotes: ";

        const char* zMessage = nullptr;

        switch (m_quotes)
        {
        case REQUIRED:
            zMessage = zRequired;
            valid = false;
            break;

        case DESIRED:
            zMessage = zDesired;
            break;

        case IGNORED:
            break;
        }

        if (pMessage && zMessage)
        {
            *pMessage = zMessage;
            *pMessage += value_as_string;
        }
    }

    if (valid)
    {
        string s = value_as_string;

        if (b == '"' || b == '\'')
        {
            valid = (b == e);

            if (valid)
            {
                s = s.substr(1, s.length() - 2);
            }
            else if (pMessage)
            {
                *pMessage = "A quoted string must end with the same quote: ";
                *pMessage += value_as_string;
            }
        }

        if (valid)
        {
            *pValue = s;
        }
    }

    return valid;
}

json_t* ParamString::to_json(value_type value) const
{
    return value.empty() ? json_null() : json_string(value.c_str());
}

bool ParamString::from_json(const json_t* pJson,
                            value_type* pValue,
                            std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        *pValue = json_string_value(pJson);
        rv = true;
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

/**
 * ParamStringList
 */
std::string ParamStringList::type() const
{
    return "stringlist";
}

std::string ParamStringList::to_string(value_type value) const
{
    return mxb::join(value, m_delimiter);
}

bool ParamStringList::from_string(const std::string& value_as_string,
                                  value_type* pValue,
                                  std::string* pMessage) const
{
    auto values = mxb::strtok(value_as_string, m_delimiter);

    // TODO: Are there cases where we don't want to trim the values?
    for (auto& v : values)
    {
        mxb::trim(v);
    }

    *pValue = std::move(values);

    return true;
}

json_t* ParamStringList::to_json(value_type value) const
{
    json_t* arr = json_array();

    for (const auto& v : value)
    {
        json_array_append_new(arr, json_string(v.c_str()));
    }

    return arr;
}

bool ParamStringList::from_json(const json_t* pJson,
                                value_type* pValue,
                                std::string* pMessage) const
{
    bool ok = false;
    value_type values;

    if (json_is_array(pJson))
    {
        ok = true;
        values.reserve(json_array_size(pJson));
        size_t i;
        json_t* v;

        json_array_foreach(pJson, i, v)
        {
            if (json_is_string(v))
            {
                values.push_back(json_string_value(v));
            }
            else
            {
                ok = false;
                break;
            }
        }
    }
    else if (json_is_string(pJson))
    {
        ok = from_string(json_string_value(pJson), &values, pMessage);
    }

    if (ok)
    {
        *pValue = std::move(values);
    }

    return ok;
}

std::string ParamPassword::type() const
{
    return "password";
}

std::string ParamPassword::to_string(value_type value) const
{
    return !value.empty() && config_mask_passwords() ? "*****" :
           ParamString::to_string(mxs::encrypt_password(value));
}

bool ParamPassword::from_string(const std::string& value, value_type* pValue, std::string* pMessage) const
{
    bool ok = ParamString::from_string(value, pValue, pMessage);

    if (ok)
    {
        *pValue = mxs::decrypt_password(*pValue);
    }

    return ok;
}


json_t* ParamPassword::to_json(value_type value) const
{
    return !value.empty() && config_mask_passwords() ? json_string("*****") :
           ParamString::to_json(mxs::encrypt_password(value));
}

bool ParamPassword::from_json(const json_t* pJson, value_type* pValue, std::string* pMessage) const
{
    bool ok = ParamString::from_json(pJson, pValue, pMessage);

    if (ok)
    {
        *pValue = mxs::decrypt_password(*pValue);
    }

    return ok;
}

bool ParamReplOpts::from_string(const std::string& value, value_type* pValue, std::string* pMessage) const
{
    bool rval = ParamString::from_string(value, pValue, pMessage);
    if (rval)
    {
        string errmsg = check_value(value);
        if (!errmsg.empty())
        {
            if (pMessage)
            {
                *pMessage = errmsg;
            }
            rval = false;
        }
    }
    return rval;
}

bool ParamReplOpts::from_json(const json_t* pJson, value_type* pValue, std::string* pMessage) const
{
    bool rval = ParamString::from_json(pJson, pValue, pMessage);
    if (rval)
    {
        string errmsg = check_value(*pValue);
        if (!errmsg.empty())
        {
            if (pMessage)
            {
                *pMessage = errmsg;
            }
            rval = false;
        }
    }
    return rval;
}

std::string ParamReplOpts::check_value(const string& value) const
{

    string errmsg;
    if (!value.empty())
    {
        // For now, just check that the last letter is not ';' or ',', this is an easy error to make.
        for (auto it = value.rbegin(); it != value.rend(); ++it)
        {
            char c = *it;
            if (isspace(c))
            {
                continue;
            }

            if (c == ';' || c == ',')
            {
                errmsg = mxb::string_printf("The last character is '%c', which should be left out.", c);
            }
            break;
        }
    }
    return errmsg;
}

config::HostPatterns config::HostPatterns::default_value()
{
    HostPatterns rval;
    rval.string_value = "*";
    rval.subnets.resize(3);
    rval.subnets[0].family = AF_INET;
    rval.subnets[1].family = AF_INET6;
    rval.subnets[2].family = AF_UNIX;
    return rval;
}

bool config::HostPatterns::operator==(const HostPatterns& rhs) const
{
    return string_value == rhs.string_value;
}

std::string config::ParamHostsPatternList::type() const
{
    return "host pattern list";
}

std::string config::ParamHostsPatternList::to_string(const value_type& value) const
{
    return value.string_value;
}

bool config::ParamHostsPatternList::from_string(const string& value, value_type* pValue,
                                                std::string* pMessage) const
{
    return parse_host_list(value, pValue, pMessage);
}

json_t* config::ParamHostsPatternList::to_json(const value_type& value) const
{
    return json_string(value.string_value.c_str());
}

bool config::ParamHostsPatternList::from_json(const json_t* pJson, value_type* pValue,
                                              std::string* pMessage) const
{
    bool rval = false;
    if (json_is_string(pJson))
    {
        string value_str = json_string_value(pJson);
        rval = parse_host_list(value_str, pValue, pMessage);
    }
    else
    {
        *pMessage = mxb::string_printf("Expected a json string, but got a json %s.",
                                       mxb::json_type_to_string(pJson));
    }
    return rval;
}

bool config::ParamHostsPatternList::parse_host_list(const string& value_str, HostPatterns* pHosts,
                                                    std::string* pMessage)
{
    auto& hosts = *pHosts;
    hosts.subnets.clear();
    hosts.host_patterns.clear();
    // Adapted from mxb::proxy_protocol::parse_networks_from_string.

    // Handle some special cases.
    if (value_str.empty())
    {
        // Configuration error.
        *pMessage = mxb::string_printf("Empty host pattern list definition is not allowed.");
        return false;
    }
    else if (value_str == "%")
    {
        // Default value, allow from all ip:s.
        hosts = HostPatterns::default_value();
        return true;
    }

    bool rval = true;
    char token[256] {};
    auto tokens = mxb::strtok(value_str, ", ");

    for (const auto& token_str : tokens)
    {
        if (token_str.length() < sizeof(token))
        {
            memcpy(token, token_str.data(), token_str.length());
            *(token + token_str.length()) = 0;

            mxb::proxy_protocol::Subnet subnet;
            // First, try parsing the token as a cidr notation ip address.
            if (mxb::proxy_protocol::parse_subnet(token, &subnet))
            {
                hosts.subnets.push_back(subnet);
            }
            else
            {
                // Perhaps token is a hostname pattern. Only do basic checking, as regex allows characters
                // not typically allowed in hostnames.
                if (token_str == "%")
                {
                    // Plain % in a list of hostname patterns is nonsensical.
                    rval = false;
                    *pMessage = mxb::string_printf("Hostname pattern '%%' cannot be mixed with other "
                                                   "patterns.");
                    break;
                }
                else
                {
                    hosts.host_patterns.push_back(token_str);
                }
            }
        }
        else
        {
            rval = false;
            *pMessage = mxb::string_printf("Host definition starting with '%s' is too long.",
                                           token_str.c_str());
            break;
        }
    }

    if (rval)
    {
        hosts.string_value = value_str;
    }
    else
    {
        hosts = HostPatterns();
    }
    return rval;
}
}
}
