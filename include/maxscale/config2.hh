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

#include <maxscale/ccdefs.hh>

#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <numeric>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>
#include <functional>
#include <maxbase/assert.hh>
#include <maxbase/atomic.hh>
#include <maxbase/proxy_protocol.hh>
#include <maxbase/string.hh>
#include <maxbase/host.hh>
#include <maxbase/log.hh>
#include <maxscale/config_common.hh>
#include <maxscale/modinfo.hh>

namespace maxscale
{

namespace config
{

enum class Origin
{
    DEFAULT, // Default value.
    CONFIG,  // Value obtained from a configuration file.
    USER     // Value explicitly set.
};

class Configuration;
class Param;
class Type;

namespace server
{
class Dependency;
}

// An instance of Specification specifies what parameters a particular module expects
// and of what type they are.
class Specification
{
public:
    enum Kind
    {
        FILTER,
        MONITOR,
        ROUTER,
        GLOBAL,
        SERVER,
        LISTENER,
        PROTOCOL
    };

    using ServerDependencies = std::set<server::Dependency*>;
    using ParamsByName = std::map<std::string, Param*>;     // We want to have them ordered by name.
    using const_iterator = ParamsByName::const_iterator;
    using value_type = ParamsByName::value_type;

    /**
     * Construct a specification
     *
     * A specification with a prefix expects the parameters to be defined in the form  of "prefix.name" when
     * configured in the configuration file or inside of a nested object when configured from JSON.
     *
     * @param zModule The the name of the module, e.g. "cachefilter".
     * @param kind    The type of the module.
     * @param zPrefix The prefix to use. This is added to all parameters used by this specification.
     */
    Specification(const char* zModule, Kind kind, const char* zPrefix = "");

    ~Specification();

    /**
     * @return What kind of specification.
     */
    Kind kind() const
    {
        return m_kind;
    }

    /**
     * @return The module name of this specification.
     */
    const std::string& module() const;

    /**
     * The prefix of this module or an empty string if no prefix is specified.
     */
    const std::string& prefix() const;

    /**
     *  Validate parameters
     *
     * @param pConfig        Pointer to the configuration whose parameters are validated
     *                       or nullptr if the configuration is not known.
     * @param params         Parameters as found in the configuration file.
     * @param pUnrecognized  If non-null:
     *                       - Will contain on return parameters that were not used.
     *                       - An unrecognized parameter will not cause the configuring
     *                         to fail.
     *
     * @return True, if `params` represent valid parameters - all mandatory are
     *         present, all present ones are of correct type - for this specification.
     */
    virtual bool validate(const Configuration* pConfig,
                          const mxs::ConfigParameters& params,
                          mxs::ConfigParameters* pUnrecognized = nullptr) const;

    bool validate(const mxs::ConfigParameters& params,
                  mxs::ConfigParameters* pUnrecognized = nullptr) const
    {
        return validate(nullptr, params, pUnrecognized);
    }

    /**
     *  Validate JSON
     *
     * @param pConfig        Pointer to the configuration whose parameters are validated
     *                       or nullptr if the configuration is not known.
     * @param pJson          JSON parameter object to validate
     * @param pUnrecognized  If non-null:
     *                       - Will contain on return object keys that were not used.
     *                       - An unrecognized parameter will not cause the configuring
     *                         to fail.
     *
     * @return True, if `pJson` represent valid JSON parameters - all mandatory are
     *         present, all present ones are of correct type - for this specification.
     */
    virtual bool validate(const Configuration* pConfig,
                          json_t* pJson,
                          std::set<std::string>* pUnrecognized = nullptr) const;

    bool validate(json_t* pJson,
                  std::set<std::string>* pUnrecognized = nullptr) const
    {
        return validate(nullptr, pJson, pUnrecognized);
    }

    /**
     * Find given parameter of the specification.
     *
     * @param name  The name of the parameter.
     *
     * @return The corresponding parameter object or NULL if the name is not a
     *         parameter of the specification.
     */
    const Param* find_param(const std::string& name) const;

    /**
     * Document this specification.
     *
     * @param out  The stream the documentation should be written to.
     *
     * @return @c out
     */
    std::ostream& document(std::ostream& out) const;

    /**
     * @return The number of parameters in the specification.
     */
    size_t size() const;

    /**
     * @return Const iterator to first parameter.
     */
    const_iterator cbegin() const
    {
        return m_params.cbegin();
    }

    /**
     * @return Const iterator to one past last parameter.
     */
    const_iterator cend() const
    {
        return m_params.cend();
    }

    /**
     * @return Const iterator to first parameter.
     */
    const_iterator begin() const
    {
        return m_params.begin();
    }

    /**
     * @return Const iterator to one past last parameter.
     */
    const_iterator end() const
    {
        return m_params.end();
    }

    /**
     * @return Specification as a json array.
     */
    json_t* to_json() const;

    /**
     * @return Server dependencies of this specification.
     */
    const ServerDependencies& server_dependencies() const
    {
        return m_server_dependencies;
    }

protected:

    /**
     * Post validation step
     *
     * This can be overridden to check dependencies between parameters.
     *
     * @param pConfig         Pointer to the configuration whose parameters are validated
     *                        or nullptr if the configuration is not known.
     * @param  params         The individually validated parameters
     * @params nested_params  Extracted nested parameters.
     *
     * @return True, if the post validation check is successful.
     *
     * @note The default implementation always returns true
     */
    virtual bool post_validate(const Configuration* pConfig,
                               const mxs::ConfigParameters& params,
                               const std::map<std::string, mxs::ConfigParameters>& nested_params) const
    {
        return true;
    }

    /**
     * Post validation step
     *
     * This can be overridden to check dependencies between parameters.
     *
     * @param pConfig         Pointer to the configuration whose parameters are validated
     *                        or nullptr if the configuration is not known.
     * @param pParams         The individually validated parameters
     * @param nested_params   Extracted nested parameters
     *
     * @return True, if the post validation check is successful.
     *
     * @note The default implementation always returns true
     */
    virtual bool post_validate(const Configuration* pConfig,
                               json_t* pParams,
                               const std::map<std::string, json_t*>& nested_params) const
    {
        return true;
    }

private:
    friend Param;
    void insert(Param* pParam);
    void remove(Param* pParam);

    friend server::Dependency;
    void insert(server::Dependency* pDependency);
    void remove(server::Dependency* pDependency);

    bool mandatory_params_defined(const std::set<std::string>& provided) const;

private:
    std::string        m_module;
    Kind               m_kind;
    ParamsByName       m_params;
    std::string        m_prefix;
    ServerDependencies m_server_dependencies;
};


/**
 * A instance of Param specifies a parameter of a module, that is, its name,
 * type, default value and whether it is mandatory or optional.
 */
class Param
{
public:
    enum Kind
    {
        MANDATORY,
        OPTIONAL
    };

    enum Modifiable
    {
        AT_STARTUP,     // The parameter can be modified only at startup.
        AT_RUNTIME      // The parameter can be modified also at runtime.
    };

    Param(const Param&) = delete;
    Param& operator=(const Param&) = delete;
    Param(Param&&) = delete;
    Param& operator=(Param&&) = delete;

    ~Param();

    /**
     * @return The specification of this parameter.
     */
    Specification& specification() const
    {
        return m_specification;
    }

    /**
     * @return The name of the parameter.
     */
    const std::string& name() const;

    /**
     * The final name of the parameter. For all parameters but ParamAlias, the final name
     * is the same as the name. For ParamAliases, the final name is the final name of the
     * parameter the ParamAlias is an alias for.
     *
     * @return The final name of the parameter.
     */
    virtual const std::string& final_name() const
    {
        return name();
    }

    /**
     * @return The type of the parameter (human readable).
     */
    virtual std::string type() const = 0;

    /**
     * @return The description of the parameter.
     */
    const std::string& description() const;

    /**
     * Document the parameter.
     *
     * The documentation of a parameters consists of its name, its type,
     * whether it is mandatory or optional (default value documented in
     * that case), and its description.
     *
     * @return The documentation.
     */
    std::string documentation() const;

    /**
     * @return The kind - mandatory or optional - of the parameter.
     */
    Kind kind() const;

    /**
     * @return True, if the parameter is mandatory.
     */
    bool is_mandatory() const;

    /**
     * @return True, if the parameter is optional.
     */
    bool is_optional() const;

    /**
     * @return True, if the parameter is deprecated.
     */
    virtual bool is_deprecated() const
    {
        return false;
    }

    /**
     * Synonym for @c is_optional.
     *
     * @return True, if the parameter has a default value.
     */
    bool has_default_value() const;

    /**
     * Specifies whether the value specified by this parameter takes parameters.
     * Currently only ParamModule takes parameters.
     *
     * @return True, if the value takes parameters. The default implementation returns false.
     */
    virtual bool takes_parameters() const;

    /**
     * Get parameter prefix from value
     *
     * For parameters with sub-parameters (i.e. takes_parameters() returns true), this returns the "real"
     * prefix if there are aliases for the parameters. In practice this only converts module names to their
     * canonical name.
     *
     * @return The prefix value to translate
     */
    virtual std::string parameter_prefix(const std::string& value) const;

    /**
     * Validate parameters of a parameter. Only applicable to parameters that takes
     * parameters and whose @c takes_parameters() returns true.
     *
     * @param value          The value identifying the parameter itself.
     * @param [pP|p]arams    The (nested) parameters of the parameter identified by @c value.
     * @param pUnrecognized  If provided, will on output contain the parameters that were not
     *                       recognized.
     *
     * @return True, if the parameters could be validated, false otherwise. If @c pUnrecognized
     *         is provided, then an unrecognized parameter will not cause the validation to fail.
     */
    virtual bool validate_parameters(const std::string& value,
                                     const mxs::ConfigParameters& params,
                                     mxs::ConfigParameters* pUnrecognized = nullptr) const;

    virtual bool validate_parameters(const std::string& value,
                                     json_t* pParams,
                                     std::set<std::string>* pUnrecognized = nullptr) const;

    /**
     * @return Modifiable::AT_RUNTIME or Modifiable::AT_STARTUP.
     */
    Modifiable modifiable() const;

    /**
     * @return True, if the parameter can be modified at runtime.
     */
    bool is_modifiable_at_runtime() const
    {
        return m_modifiable == Modifiable::AT_RUNTIME;
    }

    /**
     * @return Default value as string.
     *
     * @note Meaningful only if @c has_default_value returns true.
     */
    virtual std::string default_to_string() const = 0;

    /**
     * Validate a string.
     *
     * @param value_as_string  The string to validate.
     *
     * @return True, if @c value_as_string can be converted into a value of this type.
     */
    virtual bool validate(const std::string& value_as_string, std::string* pMessage) const = 0;

    /**
     * Validate JSON.
     *
     * @param value_as_json  The JSON to validate.
     *
     * @return True, if @c value_as_json can be converted into a value of this type.
     */
    virtual bool validate(json_t* value_as_json, std::string* pMessage) const = 0;

    /**
     * @return Parameter as json object.
     */
    virtual json_t* to_json() const;

    /**
     * @brief Get the names of any objects that this parameter depends on
     *
     * By default parameters do not depend on any objects. This function is only used during startup when
     * objects are being created from the configuration files and the order in which they are constructed is
     * being resolved. Changes done at runtime do not need to know the dependencies as the objects either
     * exist or do not.
     *
     * @param value The parameter value as a string
     *
     * @return The list of names of objects (servers, services etc.) that must be constructed before this
     *         parameter can be configured
     */
    virtual std::vector<std::string> get_dependencies(const std::string& value) const;

protected:
    Param(Specification* pSpecification,
          const char* zName,
          const char* zDescription,
          Modifiable modifiable,
          Kind kind);

protected:
    Specification&    m_specification;
    const std::string m_name;
    const std::string m_description;
    const Modifiable  m_modifiable;
    const Kind        m_kind;
};

/**
 * Concrete Param, helper class to be derived from with the actual
 * concrete parameter class.
 */
template<class ParamType, class NativeType>
class ConcreteParam : public Param
{
public:
    using value_type = NativeType;

    virtual value_type default_value() const
    {
        return m_default_value;
    }

    std::string default_to_string() const override
    {
        return static_cast<const ParamType*>(this)->to_string(m_default_value);
    }

    bool validate(const std::string& value_as_string, std::string* pMessage) const override
    {
        value_type value;
        return static_cast<const ParamType*>(this)->from_string(value_as_string, &value, pMessage);
    }

    bool validate(json_t* value_as_json, std::string* pMessage) const override
    {
        value_type value;
        return static_cast<const ParamType*>(this)->from_json(value_as_json, &value, pMessage);
    }

    bool is_valid(const value_type&) const
    {
        return true;
    }

    /**
     * Returns the value of this parameter as specified in the provided
     * collection of parameters, or default value if none specified.
     *
     * @note Before calling this member function @params should have been
     *       validated by calling @c Specification::validate(params).
     *
     * @param params The provided configuration parameters.
     *
     * @return The value of this parameter.
     */
    value_type get(const mxs::ConfigParameters& params) const
    {
        value_type rv {m_default_value};

        bool contains = params.contains(name());
        mxb_assert(!is_mandatory() || contains);

        if (contains)
        {
            const ParamType* pThis = static_cast<const ParamType*>(this);

            MXB_AT_DEBUG(bool valid = ) pThis->from_string(params.get_string(name()), &rv);
            mxb_assert(valid);
        }

        return rv;
    }

    /**
     * Get the parameter value from JSON
     *
     * @note Before calling this member function the JSON should have been
     *       validated by calling `Specification::validate(json)`.
     *
     * @param json The JSON object that defines the parameters.
     *
     * @return The value of this parameter if `json` contains a key with the name of this parameter. The
     *         default value if no key was found or the key was a JSON null.
     */
    value_type get(json_t* json) const
    {
        value_type rv {m_default_value};

        json_t* value = json_object_get(json, name().c_str());
        bool contains = value && !json_is_null(value);
        mxb_assert(!is_mandatory() || contains);

        if (contains)
        {
            const ParamType* pThis = static_cast<const ParamType*>(this);

            MXB_AT_DEBUG(bool valid = ) pThis->from_json(value, &rv);
            mxb_assert_message(valid, "JSON value is not valid: %s", mxb::json_dump(value).c_str());
        }

        return rv;
    }

    json_t* to_json() const override
    {
        auto rv = Param::to_json();

        if (kind() == Kind::OPTIONAL)
        {
            auto self = static_cast<const ParamType*>(this);
            auto val = self->to_json(m_default_value);

            if (json_is_null(val))
            {
                // "empty" default values aren't added
                json_decref(val);
            }
            else
            {
                json_object_set_new(rv, "default_value", val);
            }
        }

        return rv;
    }

protected:
    ConcreteParam(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  Modifiable modifiable,
                  Kind kind,
                  value_type default_value)
        : Param(pSpecification, zName, zDescription, modifiable, kind)
        , m_default_value(default_value)
    {
    }

    value_type m_default_value;
};

/**
 * ParamAlias
 */
class ParamAlias : public Param
{
public:
    ParamAlias(Specification* pSpecification,
               const char* zName,
               Param* pTarget)
        : Param(pSpecification,
                zName,
                create_description(pTarget).c_str(),
                pTarget->modifiable(),
                pTarget->kind())
        , m_target(*pTarget)
    {
    }

    const std::string& final_name() const override final
    {
        return m_target.final_name();
    }

    std::string type() const override final
    {
        return m_target.type();
    }

    std::string default_to_string() const override final
    {
        return m_target.default_to_string();
    }

    bool validate(const std::string& value_as_string, std::string* pMessage) const override final
    {
        return m_target.validate(value_as_string, pMessage);
    }

    bool validate(json_t* value_as_json, std::string* pMessage) const override final
    {
        return m_target.validate(value_as_json, pMessage);
    }

private:
    static std::string create_description(Param* pTarget)
    {
        std::string rv("Alias for '");
        rv += pTarget->name();
        rv += "'";

        return rv;
    }

    Param& m_target;
};

/**
 * ParamDeprecated
 */
template<class T>
class ParamDeprecated : public T
{
public:
    using T::T;

    bool is_deprecated() const override
    {
        return true;
    }
};

/**
 * ParamBool
 */
class ParamBool : public ConcreteParam<ParamBool, bool>
{
public:
    ParamBool(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamBool(pSpecification, zName, zDescription, modifiable, Param::MANDATORY, value_type())
    {
    }

    ParamBool(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              value_type default_value,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamBool(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL, default_value)
    {
    }

    std::string type() const override;

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    ParamBool(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Modifiable modifiable,
              Kind kind,
              value_type default_value)
        : ConcreteParam<ParamBool, bool>(pSpecification, zName, zDescription,
                                         modifiable, kind, default_value)
    {
    }
};

class ParamNumber : public ConcreteParam<ParamNumber, int64_t>
{
public:
    using Param::to_json;

    virtual std::string to_string(value_type value) const;
    virtual bool from_string(const std::string& value, value_type* pValue,
                             std::string* pMessage = nullptr) const;

    virtual json_t* to_json(value_type value) const;
    virtual bool from_json(const json_t* pJson, value_type* pValue,
                           std::string* pMessage = nullptr) const;

    bool is_valid(value_type value) const
    {
        return value >= m_min_value && value <= m_max_value;
    }

    value_type min_value() const
    {
        return m_min_value;
    }

    value_type max_value() const
    {
        return m_max_value;
    }

protected:
    ParamNumber(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                Modifiable modifiable,
                Kind kind,
                value_type default_value,
                value_type min_value,
                value_type max_value)
        : ConcreteParam<ParamNumber, int64_t>(pSpecification, zName, zDescription,
                                              modifiable, kind, default_value)
        , m_min_value(min_value <= max_value ? min_value : max_value)
        , m_max_value(max_value)
    {
        mxb_assert(min_value <= max_value);
    }

    bool from_value(value_type value,
                    value_type* pValue,
                    std::string* pMessage) const;

protected:
    value_type m_min_value;
    value_type m_max_value;
};

/**
 * ParamCount
 */
class ParamCount : public ParamNumber
{
public:
    ParamCount(Specification* pSpecification,
               const char* zName,
               const char* zDescription,
               Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamCount(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                     value_type(), 0, std::numeric_limits<value_type>::max())
    {
    }

    ParamCount(Specification* pSpecification,
               const char* zName,
               const char* zDescription,
               value_type min_value,
               value_type max_value,
               Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamCount(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                     value_type(), min_value, max_value)
    {
    }

    ParamCount(Specification* pSpecification,
               const char* zName,
               const char* zDescription,
               value_type default_value,
               Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamCount(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                     default_value, 0, std::numeric_limits<value_type>::max())
    {
    }

    ParamCount(Specification* pSpecification,
               const char* zName,
               const char* zDescription,
               value_type default_value,
               value_type min_value,
               value_type max_value,
               Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamCount(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                     default_value, min_value, max_value)
    {
    }

    std::string type() const override;

private:
    ParamCount(Specification* pSpecification,
               const char* zName,
               const char* zDescription,
               Modifiable modifiable,
               Kind kind,
               value_type default_value,
               value_type min_value,
               value_type max_value)
        : ParamNumber(pSpecification, zName, zDescription, modifiable, kind,
                      default_value,
                      min_value >= 0 ? min_value : 0,
                      max_value <= std::numeric_limits<value_type>::max() ?
                      max_value : std::numeric_limits<value_type>::max())
    {
        mxb_assert(min_value >= 0);
        mxb_assert(max_value <= std::numeric_limits<value_type>::max());
    }
};

using ParamNatural = ParamCount;

/**
 * ParamPercent
 */
class ParamPercent : public ParamCount
{
public:
    using ParamCount::ParamCount;

    std::string type() const override;

    json_t* to_json() const override;

    std::string to_string(value_type value) const override;
    bool from_string(const std::string& value, value_type* pValue,
                     std::string* pMessage = nullptr) const override;

    json_t* to_json(value_type value) const override;
    bool from_json(const json_t* pJson, value_type* pValue,
                   std::string* pMessage = nullptr) const override;

};

/**
 * ParamInteger
 */
class ParamInteger : public ParamNumber
{
public:
    ParamInteger(Specification* pSpecification,
                 const char* zName,
                 const char* zDescription,
                 Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamInteger(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                       value_type(),
                       std::numeric_limits<value_type>::min(),
                       std::numeric_limits<value_type>::max())
    {
    }

    ParamInteger(Specification* pSpecification,
                 const char* zName,
                 const char* zDescription,
                 value_type min_value,
                 value_type max_value,
                 Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamInteger(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                       value_type(), min_value, max_value)
    {
    }

    ParamInteger(Specification* pSpecification,
                 const char* zName,
                 const char* zDescription,
                 value_type default_value,
                 Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamInteger(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                       default_value,
                       std::numeric_limits<value_type>::min(),
                       std::numeric_limits<value_type>::max())
    {
    }

    ParamInteger(Specification* pSpecification,
                 const char* zName,
                 const char* zDescription,
                 value_type default_value,
                 value_type min_value,
                 value_type max_value,
                 Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamInteger(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                       default_value, min_value, max_value)
    {
    }

    std::string type() const override;

private:
    ParamInteger(Specification* pSpecification,
                 const char* zName,
                 const char* zDescription,
                 Modifiable modifiable,
                 Kind kind,
                 value_type default_value,
                 value_type min_value,
                 value_type max_value)
        : ParamNumber(pSpecification, zName, zDescription, modifiable, kind,
                      default_value,
                      min_value >= std::numeric_limits<value_type>::min() ?
                      min_value : std::numeric_limits<value_type>::min(),
                      max_value <= std::numeric_limits<value_type>::max() ?
                      max_value : std::numeric_limits<value_type>::max())
    {
        mxb_assert(min_value >= std::numeric_limits<value_type>::min());
        mxb_assert(max_value <= std::numeric_limits<value_type>::max());
    }
};

/**
 * ParamDuration
 */
template<class T>
class ParamDuration : public ConcreteParam<ParamDuration<T>, T>
{
public:
    using value_type = T;

    enum class DurationType
    {
        UNSIGNED,   // Negative durations are not allowed
        SIGNED      // Negative durations are allowed
    };

    ParamDuration(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamDuration(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                        DurationType::UNSIGNED, value_type())
    {
    }

    ParamDuration(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  value_type default_value,
                  Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamDuration(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                        DurationType::UNSIGNED, default_value)
    {
    }

    ParamDuration(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  value_type default_value,
                  DurationType duration_type,
                  Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamDuration(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                        duration_type, default_value)
    {
    }

    std::string type() const override;

    std::string to_string(const value_type& value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(const value_type& value) const;
    json_t* to_json() const override;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    ParamDuration(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  Param::Modifiable modifiable,
                  Param::Kind kind,
                  DurationType duration_type,
                  value_type default_value)
        : ConcreteParam<ParamDuration<T>, T>(pSpecification, zName, zDescription,
                                             modifiable, kind, default_value)
        , m_duration_type(duration_type)
    {
    }

private:
    DurationType m_duration_type;
};

using ParamMilliseconds = ParamDuration<std::chrono::milliseconds>;
using ParamSeconds = ParamDuration<std::chrono::seconds>;

/**
 * ParamEnum
 */
template<class T>
class ParamEnum : public ConcreteParam<ParamEnum<T>, T>
{
public:
    using value_type = T;

    ParamEnum(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              const std::vector<std::pair<T, const char*>>& enumeration,
              Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamEnum(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                    enumeration, value_type())
    {
    }

    ParamEnum(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              const std::vector<std::pair<T, const char*>>& enumeration,
              value_type default_value,
              Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamEnum(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                    enumeration, default_value)
    {
    }

    std::string type() const override;
    const std::vector<std::pair<T, const char*>>& values() const
    {
        return m_enumeration;
    }

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

    json_t* to_json() const override;

private:
    ParamEnum(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Param::Modifiable modifiable,
              Param::Kind kind,
              const std::vector<std::pair<T, const char*>>& enumeration,
              value_type default_value);

private:
    std::vector<std::pair<T, const char*>> m_enumeration;
};

template<typename T>
class ParamEnumList : public ConcreteParam<ParamEnumList<T>, std::vector<T>>
{
public:
    using value_type = std::vector<T>;

    ParamEnumList(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  const std::vector<std::pair<T, const char*>>& enumeration,
                  Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamEnumList(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                        enumeration, value_type())
    {
    }

    ParamEnumList(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  const std::vector<std::pair<T, const char*>>& enumeration,
                  value_type default_value,
                  Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamEnumList(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                        enumeration, default_value)
    {
    }

    std::string type() const override;
    const std::vector<std::pair<T, const char*>>& values() const
    {
        return m_enumeration;
    }

    std::string to_string(value_type value_list) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value_list) const;
    json_t* to_json() const override;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    ParamEnumList(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  Param::Modifiable modifiable,
                  Param::Kind kind,
                  const std::vector<std::pair<T, const char*>>& enumeration,
                  value_type default_value)
        : ConcreteParam<ParamEnumList, std::vector<T>>(
            pSpecification, zName, zDescription, modifiable, kind, default_value)
        , m_enumeration(enumeration)

    {
    }

private:
    std::vector<std::pair<T, const char*>> m_enumeration;
};

template<typename T>
std::string ParamEnumList<T>::type() const
{
    return "enum list";
}

template<typename T>
std::string ParamEnumList<T>::to_string(value_type value_list) const
{
    std::string ret;
    bool first = true;
    for (const auto& value : value_list)
    {
        auto it = std::find_if(m_enumeration.begin(), m_enumeration.end(),
                               [value](const std::pair<T, const char*>& entry) {
            return entry.first == value;
        });

        mxb_assert(it != m_enumeration.end());

        if (!first)
        {
            ret += ',';
        }
        ret += it->second;
        first = false;
    }

    return ret;
}

template<typename T>
bool ParamEnumList<T>::from_string(const std::string& values_as_string,
                                   value_type* pValue,
                                   std::string* pMessage) const
{
    bool success = true;

    for (auto value_as_string : mxb::strtok(values_as_string, ","))
    {
        maxbase::trim(value_as_string);

        auto it = std::find_if(m_enumeration.begin(), m_enumeration.end(),
                               [value_as_string](const std::pair<T, const char*>& elem) {
            return value_as_string == elem.second;
        });

        if (it != m_enumeration.end())
        {
            pValue->push_back(it->first);
        }
        else if (pMessage)
        {
            std::string s;
            for (size_t i = 0; i < m_enumeration.size(); ++i)
            {
                s += "'";
                s += m_enumeration[i].second;
                s += "'";

                if (i == m_enumeration.size() - 2)
                {
                    s += " and ";
                }
                else if (i != m_enumeration.size() - 1)
                {
                    s += ", ";
                }
            }

            *pMessage = "Invalid enumeration value: ";
            *pMessage += value_as_string;
            *pMessage += ", valid values are: ";
            *pMessage += s;
            *pMessage += ".";

            success = false;
            break;
        }
    }

    return success;
}

template<typename T>
json_t* ParamEnumList<T>::to_json(value_type value_list) const
{
    json_t* arr = json_array();
    for (const auto& value : value_list)
    {
        auto it = std::find_if(m_enumeration.begin(), m_enumeration.end(),
                               [value](const std::pair<T, const char*>& entry) {
            return entry.first == value;
        });

        if (it != m_enumeration.end())
        {
            json_array_append_new(arr, json_string(it->second));
        }
        else
        {
            mxb_assert(!true);
            json_array_append_new(arr, json_string("Unknown"));
        }
    }

    return arr;
}

template<typename T>
bool ParamEnumList<T>::from_json(const json_t* pJson, value_type* pValue, std::string* pMessage) const
{
    bool rv = false;

    std::string values_as_string;

    if (json_is_array(pJson))
    {
        size_t index;
        json_t* elem;
        bool first = true;
        json_array_foreach(pJson, index, elem)
        {
            if (json_is_string(elem))
            {
                if (!first)
                {
                    values_as_string += ',';
                }
                
                values_as_string += json_string_value(elem);
                
                first = false;
            }
            else
            {
                *pMessage = "Expected a json array of strings, but array contained a json ";
                *pMessage += mxb::json_type_to_string(pJson);
                *pMessage += ".";
            }
        }
    }
    else if (json_is_string(pJson))
    {
        values_as_string = json_string_value(pJson);
    }

    rv = from_string(values_as_string.c_str(), pValue, pMessage);

    return rv;
}

template<typename T>
json_t* ParamEnumList<T>::to_json() const
{
    auto rv = ConcreteParam<ParamEnumList<T>, std::vector<T>>::to_json();
    auto arr = json_array();

    for (const auto& a : m_enumeration)
    {
        json_array_append_new(arr, json_string(a.second));
    }

    json_object_set_new(rv, "enum_values", arr);

    return rv;
}

/**
 * ParamEnumMask
 */
template<class T>
class ParamEnumMask : public ConcreteParam<ParamEnumMask<T>, uint32_t>
{
public:
    using value_type = uint32_t;

    ParamEnumMask(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  const std::vector<std::pair<T, const char*>>& enumeration,
                  Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamEnumMask(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                        enumeration, value_type())
    {
    }

    ParamEnumMask(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  const std::vector<std::pair<T, const char*>>& enumeration,
                  value_type default_value,
                  Param::Modifiable modifiable = Param::Modifiable::AT_STARTUP)
        : ParamEnumMask(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                        enumeration, default_value)
    {
    }

    std::string type() const override;
    const std::vector<std::pair<T, const char*>>& values() const
    {
        return m_enumeration;
    }

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

    json_t* to_json() const override;

private:
    ParamEnumMask(Specification* pSpecification,
                  const char* zName,
                  const char* zDescription,
                  Param::Modifiable modifiable,
                  Param::Kind kind,
                  const std::vector<std::pair<T, const char*>>& enumeration,
                  value_type default_value);

private:
    std::vector<std::pair<T, const char*>> m_enumeration;
};

/**
 * ParamHost
 */
class ParamHost : public ConcreteParam<ParamHost, maxbase::Host>
{
public:
    ParamHost(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamHost(pSpecification, zName, zDescription, modifiable, Param::MANDATORY, value_type())
    {
    }

    ParamHost(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              int default_port,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamHost(pSpecification, zName, zDescription, modifiable, Param::MANDATORY, value_type(),
                    default_port)
    {
    }

    ParamHost(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              value_type default_value,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamHost(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL, default_value)
    {
    }

    ParamHost(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              value_type default_value,
              int default_port,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamHost(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL, default_value,
                    default_port)
    {
    }

    std::string type() const override;

    std::string to_string(const value_type& value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(const value_type& value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    ParamHost(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Modifiable modifiable,
              Kind kind,
              const value_type& default_value,
              int default_port = mxb::Host::InvalidPort)
        : ConcreteParam<ParamHost, maxbase::Host>(pSpecification, zName, zDescription,
                                                  modifiable, kind, default_value)
        , m_default_port(default_port)
    {
    }

    int m_default_port;
};

/**
 * ParamPath
 */
class ParamPath : public ConcreteParam<ParamPath, std::string>
{
public:
    enum Options
    {
        X = 1 << 0,     // Execute permission required.
        R = 1 << 1,     // Read permission required.
        W = 1 << 2,     // Write permission required.
        F = 1 << 3,     // File existence required.
        C = 1 << 4      // Create path if does not exist.
    };

    const uint32_t MASK = X | R | W | F | C;


    ParamPath(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              uint32_t options,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamPath(pSpecification, zName, zDescription, modifiable, Param::MANDATORY, options, value_type())
    {
    }

    ParamPath(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              uint32_t options,
              value_type default_value,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamPath(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL, options, default_value)
    {
    }

    std::string type() const override;

    std::string to_string(const value_type& value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(const value_type& value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

    bool is_valid(const value_type& value) const
    {
        return is_valid_path(m_options, value);
    }

    static bool is_valid_path(uint32_t options, const value_type& value);

private:
    ParamPath(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Modifiable modifiable,
              Kind kind,
              uint32_t options,
              value_type default_value)
        : ConcreteParam<ParamPath, std::string>(pSpecification, zName, zDescription,
                                                modifiable, kind, default_value)
        , m_options(options)
    {
    }

private:
    uint32_t m_options;
};

/**
 * ParamPathList
 */
class ParamPathList : public ConcreteParam<ParamPathList, std::vector<std::string>>
{
public:

    using Options = ParamPath::Options;

    ParamPathList(Specification* pSpecification, const char* zName, const char* zDescription,
                  uint32_t options, Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamPathList(pSpecification, zName, zDescription, modifiable,
                        Param::MANDATORY, options, value_type())
    {
    }

    ParamPathList(Specification* pSpecification, const char* zName, const char* zDescription,
                  uint32_t options, value_type default_value, Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamPathList(pSpecification, zName, zDescription, modifiable,
                        Param::OPTIONAL, options, default_value)
    {
    }

    std::string type() const override;

    std::string to_string(const value_type& value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(const value_type& value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    ParamPathList(Specification* pSpecification, const char* zName, const char* zDescription,
                  Modifiable modifiable, Kind kind, uint32_t options, value_type default_value)
        : ConcreteParam<ParamPathList, std::vector<std::string>>(pSpecification, zName, zDescription,
                                                                 modifiable, kind, default_value)
        , m_options(options)
    {
    }

private:
    uint32_t m_options;
};

/**
 * ParamRegex
 */

class RegexValue : public mxb::Regex
{
public:
    RegexValue() = default;
    RegexValue(const RegexValue&) = default;
    RegexValue& operator=(const RegexValue&) = default;

    /**
     * Creates a new RegexValue from a text pattern
     */
    RegexValue(const std::string& text, uint32_t options);

    /**
     * Creates a RegexValue from an already compiled pattern
     */
    RegexValue(const std::string& text,
               std::unique_ptr<pcre2_code> sCode,
               uint32_t ovec_size,
               uint32_t options)
        : mxb::Regex(text, sCode.release(), options)
        , ovec_size(ovec_size)
    {
    }

    bool operator==(const RegexValue& rhs) const
    {
        return this->pattern() == rhs.pattern()
               && this->ovec_size == rhs.ovec_size
               && this->options() == rhs.options()
               && (!this->valid() == !rhs.valid());     // Both have the same validity.
    }

    bool operator!=(const RegexValue& rhs) const
    {
        return !(*this == rhs);
    }

    uint32_t ovec_size {0};
};

class ParamRegex : public ConcreteParam<ParamRegex, RegexValue>
{
public:
    ParamRegex(Specification* pSpecification,
               const char* zName,
               const char* zDescription,
               Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamRegex, RegexValue>(pSpecification, zName, zDescription,
                                                modifiable, Param::MANDATORY, value_type())
    {
    }

    ParamRegex(Specification* pSpecification,
               const char* zName,
               const char* zDescription,
               const char* zRegex,
               Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamRegex, RegexValue>(pSpecification, zName, zDescription,
                                                modifiable, Param::OPTIONAL, create_default(zRegex))
    {
    }

    uint32_t options() const
    {
        return m_options;
    }

    std::string type() const override;

    std::string to_string(const value_type& value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(const value_type& value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    static RegexValue create_default(const char* zRegex);

    uint32_t m_options = 0;
};

/**
 * ParamServer
 */
class ParamServer : public ConcreteParam<ParamServer, SERVER*>
{
public:
    ParamServer(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamServer, SERVER*>(pSpecification, zName, zDescription,
                                              modifiable, Param::MANDATORY, nullptr)
    {
    }

    ParamServer(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                Param::Kind kind,
                Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamServer, SERVER*>(pSpecification, zName, zDescription,
                                              modifiable, kind, nullptr)
    {
    }

    std::string type() const override;

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

    std::vector<std::string> get_dependencies(const std::string& value) const override;
};

/**
 * ParamServerList
 */
class ParamServerList : public ConcreteParam<ParamServerList, std::vector<SERVER*>>
{
public:
    ParamServerList(Specification* pSpecification,
                    const char* zName,
                    const char* zDescription,
                    Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamServerList, std::vector<SERVER*>>(
            pSpecification, zName, zDescription,
            modifiable, Param::MANDATORY, {})
    {
    }

    ParamServerList(Specification* pSpecification,
                    const char* zName,
                    const char* zDescription,
                    Param::Kind kind,
                    Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamServerList, std::vector<SERVER*>>(
            pSpecification, zName, zDescription,
            modifiable, kind, {})
    {
    }

    std::string type() const override;

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

    std::vector<std::string> get_dependencies(const std::string& value) const override;
};

/**
 * ParamTarget
 */
class ParamTarget : public ConcreteParam<ParamTarget, mxs::Target*>
{
public:
    ParamTarget(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                Param::Kind kind = Param::MANDATORY,
                Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamTarget, mxs::Target*>(pSpecification, zName, zDescription,
                                                   modifiable, kind, nullptr)
    {
    }

    std::string type() const override;

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

    std::vector<std::string> get_dependencies(const std::string& value) const override;
};

/**
 * ParamService
 */
class ParamService : public ConcreteParam<ParamService, SERVICE*>
{
public:
    ParamService(Specification* pSpecification,
                 const char* zName,
                 const char* zDescription,
                 Param::Kind kind = Param::MANDATORY,
                 Modifiable modifiable = Modifiable::AT_STARTUP)
        : ConcreteParam<ParamService, SERVICE*>(pSpecification, zName, zDescription,
                                                modifiable, kind, nullptr)
    {
    }

    std::string type() const override;

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

    std::vector<std::string> get_dependencies(const std::string& value) const override;
};

/**
 * ParamSize
 */
class ParamSize : public ParamNumber
{
public:
    ParamSize(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamSize(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                    value_type(),
                    0,
                    std::numeric_limits<value_type>::max())
    {
    }

    ParamSize(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              value_type min_value,
              value_type max_value,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamSize(pSpecification, zName, zDescription, modifiable, Param::MANDATORY,
                    value_type(),
                    min_value, max_value)
    {
    }

    ParamSize(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              value_type default_value,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamSize(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL,
                    default_value,
                    0,
                    std::numeric_limits<value_type>::max())
    {
    }

    ParamSize(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              value_type default_value,
              value_type min_value,
              value_type max_value,
              Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamSize(pSpecification, zName, zDescription, modifiable, Param::OPTIONAL, default_value,
                    min_value, max_value)
    {
    }

    std::string type() const override;

    std::string to_string(value_type value) const override;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const override;

    json_t* to_json(value_type value) const override;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const override;

private:
    ParamSize(Specification* pSpecification,
              const char* zName,
              const char* zDescription,
              Modifiable modifiable,
              Kind kind,
              value_type default_value,
              value_type min_value,
              value_type max_value)
        : ParamNumber(pSpecification, zName, zDescription, modifiable, kind,
                      default_value, min_value, max_value)
    {
    }
};

/**
 * ParamString
 */
class ParamString : public ConcreteParam<ParamString, std::string>
{
public:
    using Param::to_json;

    enum Quotes
    {
        REQUIRED,   // The string *must* be surrounded by quotes.
        DESIRED,    // If there are no surrounding quotes, a warning is logged.
        IGNORED,    // The string may, but need not be surrounded by quotes. No warning.
    };

    ParamString(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamString(pSpecification, zName, zDescription, IGNORED, modifiable, Param::MANDATORY,
                      value_type())
    {
    }

    ParamString(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                Quotes quotes,
                Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamString(pSpecification, zName, zDescription, quotes, modifiable, Param::MANDATORY, value_type())
    {
    }

    ParamString(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                value_type default_value,
                Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamString(pSpecification, zName, zDescription, IGNORED, modifiable, Param::OPTIONAL,
                      default_value)
    {
    }

    ParamString(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                value_type default_value,
                Quotes quotes,
                Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamString(pSpecification, zName, zDescription, quotes, modifiable, Param::OPTIONAL, default_value)
    {
    }

    std::string type() const override;

    virtual std::string to_string(value_type value) const;
    virtual bool        from_string(const std::string& value, value_type* pValue,
                                    std::string* pMessage = nullptr) const;

    virtual json_t* to_json(value_type value) const;
    virtual bool    from_json(const json_t* pJson, value_type* pValue,
                              std::string* pMessage = nullptr) const;

private:
    ParamString(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                Quotes quotes,
                Modifiable modifiable,
                Kind kind,
                value_type default_value)
        : ConcreteParam<ParamString, std::string>(pSpecification, zName, zDescription,
                                                  modifiable, kind, default_value)
        , m_quotes(quotes)
    {
    }

    Quotes m_quotes;
};

/**
 * ParamStringList
 */
class ParamStringList : public ConcreteParam<ParamStringList, std::vector<std::string>>
{
public:
    ParamStringList(Specification* pSpecification,
                    const char* zName,
                    const char* zDescription,
                    const char* zDelimiter,
                    Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamStringList(pSpecification, zName, zDescription, zDelimiter,
                          modifiable, Param::MANDATORY, value_type())
    {
    }

    ParamStringList(Specification* pSpecification,
                    const char* zName,
                    const char* zDescription,
                    const char* zDelimiter,
                    value_type default_value,
                    Modifiable modifiable = Modifiable::AT_STARTUP)
        : ParamStringList(pSpecification, zName, zDescription, zDelimiter,
                          modifiable, Param::OPTIONAL, default_value)
    {
    }

    std::string type() const override;

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    ParamStringList(Specification* pSpecification,
                    const char* zName,
                    const char* zDescription,
                    const char* zDelimiter,
                    Modifiable modifiable,
                    Kind kind,
                    value_type default_value)
        : ConcreteParam<ParamStringList, std::vector<std::string>>(
            pSpecification, zName, zDescription, modifiable, kind, default_value)
        , m_delimiter(zDelimiter)
    {
    }

    const char* m_delimiter;
};

/**
 * ParamModule
 */
class ParamModule : public ConcreteParam<ParamModule, const MXS_MODULE*>
{
public:
    ParamModule(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                mxs::ModuleType module_type)
        : ConcreteParam<ParamModule, const MXS_MODULE*>(
            pSpecification, zName, zDescription, Param::AT_STARTUP, Param::MANDATORY, nullptr)
        , m_module_type(module_type)
    {
    }

    ParamModule(Specification* pSpecification,
                const char* zName,
                const char* zDescription,
                mxs::ModuleType module_type,
                const std::string& default_value)
        : ConcreteParam<ParamModule, const MXS_MODULE*>(
            pSpecification, zName, zDescription, Param::AT_STARTUP, Param::OPTIONAL, nullptr)
        , m_module_type(module_type)
        , m_default_module(default_value)
    {
    }

    value_type default_value() const override;

    std::string type() const override;

    bool takes_parameters() const override;

    std::string parameter_prefix(const std::string& value) const override;

    bool validate_parameters(const std::string& value,
                             const mxs::ConfigParameters& params,
                             mxs::ConfigParameters* pUnrecognized = nullptr) const override;

    bool validate_parameters(const std::string& value,
                             json_t* pParams,
                             std::set<std::string>* pUnrecognized = nullptr) const override;

    std::string to_string(value_type value) const;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(value_type value) const;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const;

private:
    mxs::ModuleType m_module_type;
    std::string     m_default_module;
};

/*
 * ParamPassword
 */
class ParamPassword : public ParamString
{
public:
    using ParamString::ParamString;

    std::string type() const override;

    std::string to_string(value_type value) const override;
    bool        from_string(const std::string& value, value_type* pValue,
                            std::string* pMessage = nullptr) const override;

    json_t* to_json(value_type value) const override;
    bool    from_json(const json_t* pJson, value_type* pValue,
                      std::string* pMessage = nullptr) const override;
};

class ParamReplOpts : public ParamString
{
public:
    ParamReplOpts(Specification* pSpecification, const char* zName, const char* zDescription,
                  Modifiable modifiable)
        : ParamReplOpts(pSpecification, zName, zDescription, modifiable, value_type())
    {
    }

    bool
    from_string(const std::string& value, value_type* pValue, std::string* pMessage = nullptr) const override;

    bool
    from_json(const json_t* pJson, value_type* pValue, std::string* pMessage = nullptr) const override;

private:
    ParamReplOpts(Specification* pSpecification, const char* zName, const char* zDescription,
                  Modifiable modifiable, value_type default_value)
        : ParamString(pSpecification, zName, zDescription, default_value, modifiable)
    {
    }

    std::string check_value(const std::string& value) const;
};

struct HostPatterns
{
    std::string                      string_value;  /**< Original setting string */
    mxb::proxy_protocol::SubnetArray subnets;       /**< Parsed binary subnets */
    std::vector<std::string>         host_patterns; /**< Parsed hostname patterns */

    static config::HostPatterns default_value();
    bool                        operator==(const HostPatterns& rhs) const;
};

class ParamHostsPatternList : public config::ConcreteParam<ParamHostsPatternList, HostPatterns>
{
public:
    ParamHostsPatternList(config::Specification* pSpecification, const char* zName, const char* zDescription,
                          value_type default_value)
        : config::ConcreteParam<ParamHostsPatternList, HostPatterns>(
            pSpecification, zName, zDescription, Modifiable::AT_STARTUP, Param::OPTIONAL,
            std::move(default_value))
    {
    }

    std::string type() const override final;

    std::string to_string(const value_type& value) const;
    bool        from_string(const std::string& value_str, value_type* pValue,
                            std::string* pMessage = nullptr) const;

    json_t* to_json(const value_type& value) const;
    bool    from_json(const json_t* pJson, value_type* pValue, std::string* pMessage = nullptr) const;

private:
    static bool parse_host_list(const std::string& value_str, HostPatterns* pHosts,
                                std::string* pMessage);
};


/**
 * ParamBitMask
 */
using ParamBitMask = ParamCount;

/**
 * server::Dependency
 *
 * An instance of Dependency describes the dependency between a parameter and
 * some server variable.
 */
class server::Dependency
{
public:
    // The approach to be used when collapsing the variable value of
    // several servers, to a single value to be used in MaxScale.
    enum Approach
    {
        AVG,    // The average of all values.
        MIN,    // The minium value.
        MAX     // The maximum value.
    };

    /**
     * Constructor
     *
     * @param zServer_variable  The global server variable that affects a MaxScale parameter.
     * @param pParameter        The MaxScale parameter affected by the server variable.
     */
    Dependency(const char* zServer_variable,
               const Param* pParameter)
        : m_server_variable(zServer_variable)
        , m_parameter(*pParameter)
    {
        m_parameter.specification().insert(this);
    }

    virtual ~Dependency()
    {
    }

    /**
     * @return The specification this dependency relates to.
     */
    Specification& specification() const
    {
        return m_parameter.specification();
    }

    /**
     * @return The global server variable of this dependency.
     */
    const std::string& server_variable() const
    {
        return m_server_variable;
    }

    /**
     * @return The parameter dependent on the server variable.
     */
    const Param& parameter() const
    {
        return m_parameter;
    }

    /**
     * Coalesces the values and applies the formatting.
     *
     * @param values  Vector of variable values.
     *
     * @return Formatted value that can be used with Type::set_from_string().
     */
    std::string apply(const std::vector<std::string>& values) const
    {
        return format(coalesce(values));
    }

    /**
     * Coalesces the values and applies the formatting.
     *
     * @param values  Vector of variable values.
     *
     * @return Formatted value that can be used with Type::set_from_json();
     */
    json_t* apply_json(const std::vector<std::string>& values) const
    {
        return format_json(coalesce(values));
    }

    /**
     * Format the value of a server variable for use with a MaxScale parameter.
     *
     * @param value  The variable value as returned by the server.
     *
     * @return The value formatted for @c Type::set_from_string(); right kind of
     *         content with right kind of suffix.
     */
    virtual std::string format(const std::string& value) const = 0;

    /**
     * Format the value of a server variable for use with a MaxScale parameter.
     *
     * @param value  The variable value as returned by the server.
     *
     * @return The value formatted for @c Type::set_from_json(); right kind of
     *         json_t* with the right kind of content.
     */
    virtual json_t* format_json(const std::string& value) const = 0;

    /**
     * Coalesce several values, obtained from different servers, to a single
     * value appropriate for the parameter in question. The way the values are
     * coalesced depends upon the server variable and the parameter.
     *
     * @param  values Vector of variable values.
     *
     * @return The coalesced value.
     */
    virtual std::string coalesce(const std::vector<std::string>& values) const = 0;

private:
    const std::string m_server_variable;
    const Param&      m_parameter;
};

namespace server
{

template<class value_type>
inline std::string format_server_value_to_parameter_string(const std::string& value);

template<class value_type>
inline json_t* format_server_value_to_parameter_json(const std::string& value);

template<>
inline std::string format_server_value_to_parameter_string<std::chrono::seconds>(const std::string& value)
{
    // When MaxScale duration parameters, regardless of unit, are converted to a string,
    // the unit will be milliseconds. To ensure that comparisons work, a server/ value in
    // seconds is formatted as milliseconds.
    long ms = 1000 * strtol(value.c_str(), nullptr, 10);

    return std::to_string(ms) + "ms";
}

template<>
inline json_t* format_server_value_to_parameter_json<std::chrono::seconds>(const std::string& value)
{
    return json_string(format_server_value_to_parameter_string<std::chrono::seconds>(value).c_str());
}

template<class ParamType>
class ConcreteDependency : public Dependency
{
public:
    ConcreteDependency(const char* zServer_variable,
                       const ParamType* pParameter)
        : Dependency(zServer_variable, pParameter)
    {
    }

    std::string format(const std::string& value) const override
    {
        return format_server_value_to_parameter_string<typename ParamType::value_type>(value);
    }

    json_t* format_json(const std::string& value) const override
    {
        return format_server_value_to_parameter_json<typename ParamType::value_type>(value);
    }
};

/**
 * NumberDependency
 *
 * A NumberDependency describes a dependency on a server variable whose value
 * can be treated as a number. When the values of different servers are coalesced,
 * the value returned can be the smallest or largest value, or the calculated average.
 * Further, the final value may be a certain percentage of the selected or calculated
 * value.
 */
template<class ParamType>
class NumberDependency : public ConcreteDependency<ParamType>
{
public:
    using Base = ConcreteDependency<ParamType>;

    NumberDependency(const char* zServer_variable,
                     const ParamType* pParameter,
                     Dependency::Approach approach,
                     int percent = 100)
        : Base(zServer_variable, pParameter)
        , m_approach(approach)
        , m_percent(percent)
    {
        mxb_assert(m_percent >= 0);
    }

    std::string coalesce(const std::vector<std::string>& values) const override
    {
        std::string rv;
        std::vector<int> numbers;
        std::transform(values.begin(), values.end(), std::back_inserter(numbers), [](const std::string& s) {
            return strtol(s.c_str(), nullptr, 0);
        });

        if (numbers.size() != 0)
        {
            int v = 0;

            switch (m_approach)
            {
            case Dependency::AVG:
                v = std::accumulate(numbers.begin(), numbers.end(), 0) / numbers.size();
                break;

            case Dependency::MIN:
                v = *std::min_element(numbers.begin(), numbers.end());
                break;

            case Dependency::MAX:
                v = *std::max_element(numbers.begin(), numbers.end());
                break;
            }

            if (m_percent != 100)
            {
                v *= m_percent;
                v /= 100;
            }

            rv = std::to_string(v);
        }

        return rv;
    }

private:
    Dependency::Approach m_approach;
    int                  m_percent;
};


/**
 * DurationDependency
 *
 * A DurationDependency describes a dependency on a server variable whose value
 * is a duration.
 */

template<class StdChronoDuration>
class DurationDependency : public NumberDependency<ParamDuration<StdChronoDuration>>
{
public:
    using Base = NumberDependency<ParamDuration<StdChronoDuration>>;
    using Base::Base;
};
}

/**
 * An instance of the class Configuration specifies the configuration of a particular
 * instance of a module.
 *
 * Walks hand in hand with Specification.
 */
template<class ParamType, class ConfigurationType>
class Native;

class Configuration
{
public:
    using ValuesByName = std::map<std::string, Type*>;      // We want to have them ordered by name.
    using const_iterator = ValuesByName::const_iterator;
    using value_type = ValuesByName::value_type;

    Configuration(Configuration&& rhs);
    Configuration& operator=(Configuration&& rhs);

    /**
     * Constructor
     *
     * @param name            The object (i.e. section name) of this configuration.
     * @param pSpecification  The specification this instance is a configuration of.
     */
    Configuration(const std::string& name, const Specification* pSpecification);

    ~Configuration();

    /**
     * Provide access to all configurations. To be called only from MainWorker.
     *
     * @return A set containing all current configurations.
     */
    static const std::set<Configuration*>& all();

    /**
     * @return The The object (i.e. section name) of this configuration.
     */
    const std::string& name() const;

    /**
     * @return The specification of this configuration.
     */
    const Specification& specification() const;

    /**
     * Validate parameters for this configuration.
     *
     * @see Specification::validate
     */
    bool validate(const mxs::ConfigParameters& params, mxs::ConfigParameters* pUnrecognized = nullptr) const
    {
        return specification().validate(this, params, pUnrecognized);
    }

    bool validate(json_t* pParams, std::set<std::string>* pUnrecognized = nullptr) const
    {
        return specification().validate(this, pParams, pUnrecognized);
    }

    /**
     * Configure this configuration
     *
     * @param params         The parameters that should be used, will be validated.
     * @param pUnrecognized  If non-null:
     *                       - Will contain on return parameters that were not used.
     *                       - An unrecognized parameter will not cause the configuring
     *                         to fail.
     *
     * @return True if could be configured.
     */
    virtual bool configure(const mxs::ConfigParameters& params,
                           mxs::ConfigParameters* pUnrecognized = nullptr);

    /**
     * Configure this configuration
     *
     * @param params         The JSON parameter object that should be used, will be validated.
     * @param pUnrecognized  If non-null:
     *                       - Will contain on return object keys that were not used.
     *                       - An unrecognized parameter will not cause the configuring
     *                         to fail.
     *
     * @return True if could be configured.
     *
     * @note If @c json contains aliases it will be modified during the call, as
     *       the value of the actual parameter will be set to that of an alias
     *       and the alias itself will be removed.
     */
    virtual bool configure(json_t* json, std::set<std::string>* pUnrecognized = nullptr);

    /**
     * @param name  The name of the parameter to look up.
     *
     * @return The corresponding @c Value or NULL if @c name is unknown.
     */
    Type*       find_value(const std::string& name);
    const Type* find_value(const std::string& name) const;

    /**
     * Persist the configuration to a stream.
     *
     * @param out           The stream to persist to.
     * @param force_persist Names of parameters that are always persisted
     */
    std::ostream& persist(std::ostream& out, const std::set<std::string>& force_persist = {}) const;

    /**
     * Append the configuration to an already persisted configuration.
     *
     * @param out           The stream to persist to.
     * @param force_persist Names of parameters that are always persisted
     */
    std::ostream& persist_append(std::ostream& out, const std::set<std::string>& force_persist = {}) const;

    /**
     * Fill the object with the param-name/param-value pairs of the configuration.
     *
     * @param pJson  The json object to be filled.
     */
    void fill(json_t* pJson) const;

    /**
     * @return The number of values in the configuration.
     */
    size_t size() const;

    /**
     * @return Const iterator to first parameter.
     */
    const_iterator begin() const
    {
        return m_values.cbegin();
    }

    /**
     * @return Const iterator to one past last parameter.
     */
    const_iterator end() const
    {
        return m_values.cend();
    }

    /**
     * @return Const iterator to first parameter.
     */
    const_iterator cbegin() const
    {
        return m_values.cbegin();
    }

    /**
     * @return Const iterator to one past last parameter.
     */
    const_iterator cend() const
    {
        return m_values.cend();
    }

    /**
     * @return Return the configuration as a json object.
     */
    json_t* to_json() const;

    /**
     * Whether the configuration was changed by the last call to configure()
     */
    bool was_modified() const
    {
        return m_was_modified;
    }

    /**
     * @return The configuration as mxs::ConfigParameters
     */
    mxs::ConfigParameters to_params() const;

    /**
     * At MaxScale startup, this function will be called after all configuration
     * objects have processed and all objects have been created. It allows an
     * object to check post-conditions that cannot be checked during normal
     * configuration processing. If any call returns @c false, the startup of
     * MaxScale is prevented.
     *
     * @return @c True, if the check passed, @c false otherwise.
     */
    virtual bool check_configuration()
    {
        return true;
    }

protected:
    /**
     * Called when configuration has initially been configured, to allow a Configuration to configure
     * nested parameters, check any interdependencies between values or to calculate derived ones.
     *
     * @param nested_params  Parameters intended for nested parameters.
     *
     * @return True, if everything is ok.
     *
     * @note The default implementation returns true if @c nested_params is empty, false otherwise.
     */
    virtual bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params);

    /**
     * Add a native parameter value:
     * - will be configured at startup
     * - assumed not to be modified at runtime via admin interface
     *
     * @param pValue  Pointer to the parameter value.
     * @param pParam  Pointer to paramter describing value.
     * @param onSet   Optional functor to be called when value is set (at startup).
     */
    template<class ParamType,
             class ConcreteConfiguration,
             class NativeParamType = Native<ParamType, ConcreteConfiguration>>
    void add_native(typename ParamType::value_type ConcreteConfiguration::* pValue,
                    ParamType* pParam,
                    std::function<void(typename ParamType::value_type)> on_set = nullptr);

    /**
     * Add a contained native parameter value:
     * - will be configured at startup
     * - assumed not to be modified at runtime via admin interface
     *
     * @param pContainer  Pointer to the container containing the parameter.
     * @param pValue      Memeber pointer to the parameter value.
     * @param pParam      Pointer to paramter describing value.
     * @param onSet       Optional functor to be called when value is set (at startup).
     */
    template<class ParamType, class ConcreteConfiguration, class Container>
    void add_native(Container ConcreteConfiguration::* pContainer,
                    typename ParamType::value_type Container::* pValue,
                    ParamType* pParam,
                    std::function<void(typename ParamType::value_type)> on_set = nullptr);

    /**
     * Add an indexed contained native parameter value:
     * - will be configured at startup
     * - assumed not to be modified at runtime via admin interface
     *
     * @param pArray   Member pointer to array of containers containing parameter.
     * @param index    Index of container in array.
     * @param pValue   Member pointer to the parameter value.
     * @param pParam   Pointer to paramter describing value.
     * @param onSet    Optional functor to be called when value is set (at startup).
     */
    template<class ParamType, class ConcreteConfiguration, class Container, int N>
    void add_native(Container (ConcreteConfiguration::* pArray)[N],
                    int index,
                    typename ParamType::value_type Container::* pValue,
                    ParamType * pParam,
                    std::function<void(typename ParamType::value_type)> on_set = nullptr);

private:
    friend Type;

    void insert(Type* pValue);
    void remove(Type* pValue, const std::string& name);

private:
    using Natives = std::vector<std::unique_ptr<Type>>;

    std::string          m_name;
    const Specification* m_pSpecification;
    ValuesByName         m_values;
    Natives              m_natives;
    bool                 m_first_time {true};
    bool                 m_was_modified {false};
};


/**
 * Base-class of all configuration value types.
 *
 * In the description of this class, "value" should be read as
 * "an instance of this type".
 */
class Type
{
public:
    Type(const Type& rhs) = delete;
    Type& operator=(const Type&) = delete;

    // Type is move-only
    Type(Type&& rhs);
    Type& operator=(Type&&);

    virtual ~Type();

    /**
     * Get parameter describing this value.
     *
     * @return Param of the value.
     */
    virtual const Param& parameter() const;

    /**
     * Persist this value as a string. It will be written as
     *
     *    name=value
     *
     * where @c value will be formatted in the correct way.
     *
     * @return @c The formatted value.
     */
    std::string persist() const;

    /**
     * Convert this value into its string representation.
     *
     * @return The value as it should appear in a configuration file.
     */
    virtual std::string to_string() const = 0;

    /**
     * Convert this value to a json object.
     *
     * @return The value as a json object.
     */
    virtual json_t* to_json() const = 0;

    /**
     * Set value.
     *
     * @param value_as_string  The new value expressed as a string.
     * @param pMessage         If non-null, on failure will contain
     *                         reason why.
     *
     * @return True, if the value could be set, false otherwise.
     */
    virtual bool set_from_string(const std::string& value_as_string,
                                 std::string* pMessage = nullptr) = 0;

    /**
     * Set value.
     *
     * @param json      The new value expressed as a json object.
     * @param pMessage  If non-null, on failure will contain reason why.
     *
     * @return True, if the value could be set, false otherwise.
     */
    virtual bool set_from_json(const json_t* pJson,
                               std::string* pMessage = nullptr) = 0;

    /**
     * Compare equality to a JSON value.
     *
     * This function is used to detect whether a parameter is being modified to a new value.
     *
     * @param json The JSON value to compare to
     *
     * @return True, if the value is equal to the given JSON value.
     */
    virtual bool is_equal(const json_t* pJson) const = 0;

protected:
    Type(Configuration* pConfiguration, const Param* pParam);

    friend Configuration;

    Configuration* m_pConfiguration;
    const Param*   m_pParam;
    std::string    m_name;
};

/**
 * Wrapper for native configuration value, not to be instantiated explicitly.
 */
template<class ParamType, class ConfigurationType>
class Native : public Type
{
public:
    using value_type = typename ParamType::value_type;

    Native(const Type& rhs) = delete;
    Native& operator=(const Native&) = delete;

    Native(ConfigurationType* pConfiguration,
           ParamType* pParam,
           typename ParamType::value_type ConfigurationType::* pValue,
           std::function<void(value_type)> on_set = nullptr)
        : Type(pConfiguration, pParam)
        , m_pValue(pValue)
        , m_on_set(on_set)
    {
    }

    // Native is move-only
    Native(Native&& rhs)
        : Type(rhs)
        , m_pValue(rhs.m_pValue)
        , m_on_set(rhs.m_on_set)
    {
        rhs.m_pValue = nullptr;
        rhs.m_on_set = nullptr;
    }

    Native& operator=(Native&& rhs)
    {
        if (this != &rhs)
        {
            Type::operator=(rhs);
            m_pValue = rhs.m_pValue;
            m_on_set = rhs.m_on_set;

            rhs.m_pValue = nullptr;
            rhs.m_on_set = nullptr;
        }

        return *this;
    }

    ~Native() = default;

    const ParamType& parameter() const override final
    {
        return static_cast<const ParamType&>(*m_pParam);
    }

    std::string to_string() const override
    {
        ConfigurationType* pConfiguration = static_cast<ConfigurationType*>(m_pConfiguration);

        return parameter().to_string(pConfiguration->*m_pValue);
    }

    json_t* to_json() const override final
    {
        ConfigurationType* pConfiguration = static_cast<ConfigurationType*>(m_pConfiguration);

        return parameter().to_json(pConfiguration->*m_pValue);
    }

    bool set_from_string(const std::string& value_as_string,
                         std::string* pMessage = nullptr) override
    {
        value_type value;
        bool rv = parameter().from_string(value_as_string, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool set_from_json(const json_t* pJson,
                       std::string* pMessage = nullptr) override final
    {
        value_type value;
        bool rv = parameter().from_json(pJson, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool is_equal(const json_t* pJson) const override final
    {
        value_type value;
        return parameter().from_json(pJson, &value) && get() == value;
    }

    value_type get() const
    {
        return static_cast<ConfigurationType*>(m_pConfiguration)->*m_pValue;
    }

    bool set(const value_type& value)
    {
        bool rv = parameter().is_valid(value);

        if (rv)
        {
            static_cast<ConfigurationType*>(m_pConfiguration)->*m_pValue = value;

            if (m_on_set)
            {
                m_on_set(value);
            }
        }

        return rv;
    }

protected:
    typename ParamType::value_type ConfigurationType::* m_pValue;
    std::function<void(value_type)>                     m_on_set;
};

/**
 * Wrapper for contained native configuration value, not to be instantiated explicitly.
 */
template<class ParamType, class ConfigurationType, class Container>
class ContainedNative : public Type
{
public:
    using value_type = typename ParamType::value_type;

    ContainedNative(const Type& rhs) = delete;
    ContainedNative& operator=(const ContainedNative&) = delete;

    ContainedNative(ConfigurationType* pConfiguration,
                    ParamType* pParam,
                    Container ConfigurationType::* pContainer,
                    typename ParamType::value_type Container::* pValue,
                    std::function<void(value_type)> on_set = nullptr)
        : Type(pConfiguration, pParam)
        , m_pContainer(pContainer)
        , m_pValue(pValue)
        , m_on_set(on_set)
    {
    }

    // Native is move-only
    ContainedNative(ContainedNative&& rhs)
        : Type(rhs)
        , m_pContainer(rhs.m_pContainer)
        , m_pValue(rhs.m_pValue)
        , m_on_set(rhs.m_on_set)
    {
        rhs.m_pContainer = nullptr;
        rhs.m_pValue = nullptr;
        rhs.m_on_set = nullptr;
    }

    ContainedNative& operator=(ContainedNative&& rhs)
    {
        if (this != &rhs)
        {
            Type::operator=(rhs);
            m_pContainer = rhs.m_pContainer;
            m_pValue = rhs.m_pValue;
            m_on_set = rhs.m_on_set;

            rhs.m_pContainer = nullptr;
            rhs.m_pValue = nullptr;
            rhs.m_on_set = nullptr;
        }

        return *this;
    }

    ~ContainedNative() = default;

    const ParamType& parameter() const override final
    {
        return static_cast<const ParamType&>(*m_pParam);
    }

    std::string to_string() const override
    {
        ConfigurationType* pConfiguration = static_cast<ConfigurationType*>(m_pConfiguration);

        return parameter().to_string((pConfiguration->*m_pContainer).*m_pValue);
    }

    json_t* to_json() const override final
    {
        ConfigurationType* pConfiguration = static_cast<ConfigurationType*>(m_pConfiguration);

        return parameter().to_json((pConfiguration->*m_pContainer).*m_pValue);
    }

    bool set_from_string(const std::string& value_as_string,
                         std::string* pMessage = nullptr) override final
    {
        value_type value;
        bool rv = parameter().from_string(value_as_string, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool set_from_json(const json_t* pJson,
                       std::string* pMessage = nullptr) override final
    {
        value_type value;
        bool rv = parameter().from_json(pJson, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool is_equal(const json_t* pJson) const override final
    {
        value_type value;
        return parameter().from_json(pJson, &value) && get() == value;
    }

    value_type get() const
    {
        return (static_cast<ConfigurationType*>(m_pConfiguration)->*m_pContainer).*m_pValue;
    }

    bool set(const value_type& value)
    {
        bool rv = parameter().is_valid(value);

        if (rv)
        {
            (static_cast<ConfigurationType*>(m_pConfiguration)->*m_pContainer).*m_pValue = value;

            if (m_on_set)
            {
                m_on_set(value);
            }
        }

        return rv;
    }

protected:
    Container ConfigurationType::*              m_pContainer;
    typename ParamType::value_type Container::* m_pValue;
    std::function<void(value_type)>             m_on_set;
};

/**
 * Wrapper for indexed native configuration value, not to be instantiated explicitly.
 */
template<class ParamType, class ConfigurationType, class Container, int N>
class IndexedContainedNative : public Type
{
public:
    using value_type = typename ParamType::value_type;

    IndexedContainedNative(const Type& rhs) = delete;
    IndexedContainedNative& operator=(const IndexedContainedNative&) = delete;

    IndexedContainedNative(ConfigurationType* pConfiguration,
                           ParamType* pParam,
                           Container(ConfigurationType::* pArray)[N],
                           int index,
                           typename ParamType::value_type Container::* pValue,
                           std::function<void(value_type)> on_set = nullptr)
        : Type(pConfiguration, pParam)
        , m_pArray(pArray)
        , m_index(index)
        , m_pValue(pValue)
        , m_on_set(on_set)
    {
    }

    // Native is move-only
    IndexedContainedNative(IndexedContainedNative&& rhs)
        : Type(rhs)
        , m_pArray(rhs.m_pArray)
        , m_index(rhs.m_index)
        , m_pValue(rhs.m_pValue)
        , m_on_set(rhs.m_on_set)
    {
        rhs.m_pArray = nullptr;
        rhs.m_index = 0;
        rhs.m_pValue = nullptr;
        rhs.m_on_set = nullptr;
    }

    IndexedContainedNative& operator=(IndexedContainedNative&& rhs)
    {
        if (this != &rhs)
        {
            Type::operator=(rhs);
            m_pArray = rhs.m_pArray;
            m_index = rhs.m_index;
            m_pValue = rhs.m_pValue;
            m_on_set = rhs.m_on_set;

            rhs.m_pArray = nullptr;
            rhs.m_index = 0;
            rhs.m_pValue = nullptr;
            rhs.m_on_set = nullptr;
        }

        return *this;
    }

    ~IndexedContainedNative() = default;

    const ParamType& parameter() const override final
    {
        return static_cast<const ParamType&>(*m_pParam);
    }

    std::string to_string() const override
    {
        ConfigurationType* pConfiguration = static_cast<ConfigurationType*>(m_pConfiguration);

        return parameter().to_string((pConfiguration->*m_pArray)[m_index].*m_pValue);
    }

    json_t* to_json() const override final
    {
        ConfigurationType* pConfiguration = static_cast<ConfigurationType*>(m_pConfiguration);

        return parameter().to_json((pConfiguration->*m_pArray)[m_index].*m_pValue);
    }

    bool set_from_string(const std::string& value_as_string,
                         std::string* pMessage = nullptr) override final
    {
        value_type value;
        bool rv = parameter().from_string(value_as_string, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool set_from_json(const json_t* pJson,
                       std::string* pMessage = nullptr) override final
    {
        value_type value;
        bool rv = parameter().from_json(pJson, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool is_equal(const json_t* pJson) const override final
    {
        value_type value;
        return parameter().from_json(pJson, &value) && get() == value;
    }

    value_type get() const
    {
        return (static_cast<ConfigurationType*>(m_pConfiguration)->*m_pArray)[m_index].*m_pValue;
    }

    bool set(const value_type& value)
    {
        bool rv = parameter().is_valid(value);

        if (rv)
        {
            (static_cast<ConfigurationType*>(m_pConfiguration)->*m_pArray)[m_index].*m_pValue = value;

            if (m_on_set)
            {
                m_on_set(value);
            }
        }

        return rv;
    }

protected:
    Container (ConfigurationType::* m_pArray)[N];
    int                                         m_index;
    typename ParamType::value_type Container::* m_pValue;
    std::function<void(value_type)>             m_on_set;
};

/**
 * A concrete Value. Instantiated with a derived class and the
 * corresponding param type.
 */
template<class ParamType>
class ConcreteTypeBase : public Type
{
public:
    using value_type = typename ParamType::value_type;

    ConcreteTypeBase(const ConcreteTypeBase&) = delete;
    ConcreteTypeBase& operator=(const ConcreteTypeBase& value) = delete;

    ConcreteTypeBase(ConcreteTypeBase&& rhs)
        : Type(std::forward<ConcreteTypeBase &&>(rhs))
        , m_value(std::move(rhs.m_value))
        , m_on_set(std::move(rhs.m_on_set))
    {
    }

    ConcreteTypeBase(Configuration* pConfiguration,
                     const ParamType* pParam,
                     std::function<void(value_type)> on_set = nullptr)
        : Type(pConfiguration, pParam)
        , m_value(pParam->default_value())
        , m_on_set(on_set)
    {
    }

    const ParamType& parameter() const override
    {
        return static_cast<const ParamType&>(*m_pParam);
    }

    bool set_from_string(const std::string& value_as_string,
                         std::string* pMessage = nullptr) override
    {
        value_type value;
        bool rv = parameter().from_string(value_as_string, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool set_from_json(const json_t* pJson,
                       std::string* pMessage = nullptr) override
    {
        value_type value;
        bool rv = parameter().from_json(pJson, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool is_equal(const json_t* pJson) const override final
    {
        value_type value;
        return parameter().from_json(pJson, &value) && get() == value;
    }

    value_type get() const
    {
        return parameter().is_modifiable_at_runtime() ? atomic_get() : non_atomic_get();
    }

    bool set(const value_type& value)
    {
        bool rv = parameter().is_valid(value);

        if (rv)
        {
            if (parameter().is_modifiable_at_runtime())
            {
                atomic_set(value);
            }
            else
            {
                non_atomic_set(value);
            }

            if (m_on_set)
            {
                m_on_set(value);
            }
        }

        return rv;
    }

    std::string to_string() const override
    {
        return parameter().to_string(m_value);
    }

    json_t* to_json() const override
    {
        return parameter().to_json(m_value);
    }

protected:
    void non_atomic_set(const value_type& value)
    {
        m_value = value;
    }

    value_type non_atomic_get() const
    {
        return m_value;
    }

protected:
    value_type                      m_value;
    std::function<void(value_type)> m_on_set;

    virtual value_type atomic_get() const = 0;
    virtual void       atomic_set(const value_type& value) = 0;
};

template<class ParamType, class EnableIf = void>
class ConcreteType : public ConcreteTypeBase<ParamType>
{
public:
    using ConcreteTypeBase<ParamType>::ConcreteTypeBase;
    using typename ConcreteTypeBase<ParamType>::value_type;

    value_type atomic_get() const override
    {
        std::lock_guard<std::mutex> guard(m_mutex);
        return this->non_atomic_get();
    }

    void atomic_set(const value_type& value) override
    {
        std::lock_guard<std::mutex> guard(m_mutex);
        this->non_atomic_set(value);
    }
private:
    mutable std::mutex m_mutex;
};

template<class ParamType>
class ConcreteType<ParamType,
                   typename std::enable_if<
                       std::is_enum<typename ParamType::value_type>::value
                       || std::is_arithmetic<typename ParamType::value_type>::value
                       >::type>
    : public ConcreteTypeBase<ParamType>
{
public:
    using ConcreteTypeBase<ParamType>::ConcreteTypeBase;
    using typename ConcreteTypeBase<ParamType>::value_type;

    value_type atomic_get() const override
    {
        return mxb::atomic::load(&this->m_value, mxb::atomic::RELAXED);
    }

    void atomic_set(const value_type& value) override
    {
        mxb::atomic::store(&this->m_value, value, mxb::atomic::RELAXED);
    }
};

/**
 * Count
 */
using Count = ConcreteType<ParamCount>;

/**
 * Integer
 */
using Integer = ConcreteType<ParamInteger>;

/**
 * BitMask
 */
using BitMask = Count;

/**
 * Bool
 */
using Bool = ConcreteType<ParamBool>;

/**
 * Duration
 */
template<class T>
class Duration : public Type
{
public:
    using value_type = T;
    using ParamType = ParamDuration<T>;

    Duration(Duration&& rhs)
        : Type(std::forward<Duration &&>(rhs))
        , m_on_set(std::move(rhs.m_on_set))
    {
        m_value.store(rhs.m_value.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }

    Duration(Configuration* pConfiguration,
             const ParamType* pParam,
             std::function<void(value_type)> on_set = nullptr)
        : Type(pConfiguration, pParam)
        , m_on_set(on_set)
    {
        m_value.store(pParam->default_value().count(), std::memory_order_relaxed);
    }

    const ParamType& parameter() const override
    {
        return static_cast<const ParamType&>(*m_pParam);
    }

    bool set_from_string(const std::string& value_as_string,
                         std::string* pMessage = nullptr) override
    {
        value_type value;
        bool rv = parameter().from_string(value_as_string, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool set_from_json(const json_t* pJson,
                       std::string* pMessage = nullptr) override
    {
        value_type value;
        bool rv = parameter().from_json(pJson, &value, pMessage);

        if (rv)
        {
            rv = set(value);
        }

        return rv;
    }

    bool is_equal(const json_t* pJson) const override final
    {
        value_type value;
        return parameter().from_json(pJson, &value) && get() == value;
    }

    value_type get() const
    {
        return value_type(m_value.load(std::memory_order_relaxed));
    }

    bool set(const value_type& value)
    {
        bool rv = parameter().is_valid(value);

        if (rv)
        {
            m_value.store(value.count(), std::memory_order_relaxed);

            if (m_on_set)
            {
                m_on_set(value);
            }
        }

        return rv;
    }

    std::string to_string() const override
    {
        return parameter().to_string(get());
    }

    json_t* to_json() const override
    {
        return parameter().to_json(get());
    }

protected:
    std::atomic<int64_t>            m_value;
    std::function<void(value_type)> m_on_set;
};

using Milliseconds = Duration<std::chrono::milliseconds>;
using Seconds = Duration<std::chrono::seconds>;

/**
 * Enum
 */
template<class T>
using Enum = ConcreteType<ParamEnum<T>>;

/**
 * EnumList
 */
template<class T>
using EnumList = ConcreteType<ParamEnumList<T>>;

/**
 * EnumMask
 */
template<class T>
using EnumMask = ConcreteType<ParamEnumMask<T>>;

/**
 * Host
 */
using Host = ConcreteType<ParamHost>;

/**
 * Module
 */
using Module = ConcreteType<ParamModule>;

/**
 * Path
 */
using Path = ConcreteType<ParamPath>;

/**
 * PathList
 */
using PathList = ConcreteType<ParamPathList>;

/**
 * Regex
 */
using Regex = ConcreteType<ParamRegex>;

/**
 * Size
 */
using Size = ConcreteType<ParamSize>;

/**
 * Server
 */
using Server = ConcreteType<ParamServer>;

/**
 * ServerList
 */
using ServerList = ConcreteType<ParamServerList>;

/**
 * Target
 */
using Target = ConcreteType<ParamTarget>;

/**
 * Service
 */
using Service = ConcreteType<ParamService>;

/**
 * String
 */
using String = ConcreteType<ParamString>;

/**
 * StringList
 */
using StringList = ConcreteType<ParamStringList>;

/**
 * IMPLEMENTATION DETAILS
 */
struct DurationSuffix
{
    static const char* of(const std::chrono::seconds&)
    {
        return "s";
    }

    static const char* of(const std::chrono::milliseconds&)
    {
        return "ms";
    }
};

template<class T>
std::string ParamDuration<T>::type() const
{
    return "duration";
}

template<class T>
std::string ParamDuration<T>::to_string(const value_type& value) const
{
    std::stringstream ss;
    ss << std::chrono::duration_cast<std::chrono::milliseconds>(value).count() << "ms";
    return ss.str();
}

template<class T>
bool ParamDuration<T>::from_string(const std::string& value_as_string,
                                   value_type* pValue,
                                   std::string* pMessage) const
{
    mxs::config::DurationUnit unit;

    std::chrono::milliseconds duration;
    const char* str = value_as_string.c_str();
    bool negate = false;

    if (*str == '-' && m_duration_type == DurationType::SIGNED)
    {
        negate = true;
        ++str;
    }

    bool valid = get_suffixed_duration(str, &duration, &unit);

    if (valid)
    {
        if constexpr (std::is_same_v<T, std::chrono::seconds> )
        {
            if (unit == mxs::config::DURATION_IN_MILLISECONDS)
            {
                if (duration < std::chrono::seconds(1) && duration > std::chrono::seconds(0))
                {
                    if (pMessage)
                    {
                        *pMessage = "Cannot set '" + this->name() + "' to " + value_as_string
                            + ": value must be defined in seconds.";
                    }

                    valid = false;
                }
                else if (duration.count() % 1000 && pMessage)
                {
                    std::chrono::seconds sec = std::chrono::duration_cast<std::chrono::seconds>(duration);
                    *pMessage = "Ignoring fractional part of '" + value_as_string + " for '" + this->name()
                        + "': value converted to " + std::to_string(sec.count()) + "s.";
                }
            }
        }

        if (negate)
        {
            duration = -duration;
        }

        *pValue = std::chrono::duration_cast<value_type>(duration);
    }
    else if (pMessage)
    {
        *pMessage = "Invalid duration: ";
        *pMessage += value_as_string;
    }

    return valid;
}

template<class T>
json_t* ParamDuration<T>::to_json(const value_type& value) const
{
    return json_string(to_string(value).c_str());
}

template<class T>
json_t* ParamDuration<T>::to_json() const
{
    auto rv = ConcreteParam<ParamDuration<T>, T>::to_json();

    json_object_set_new(rv, "unit", json_string("ms"));

    return rv;
}

template<class T>
bool ParamDuration<T>::from_json(const json_t* pJson,
                                 value_type* pValue,
                                 std::string* pMessage) const
{
    bool rv = false;

    if (json_is_string(pJson))
    {
        return from_string(json_string_value(pJson), pValue, pMessage);
    }
    else if (pMessage)
    {
        *pMessage = "Expected a json string with a duration, but got a json ";
        *pMessage += mxb::json_type_to_string(pJson);
        *pMessage += ".";
    }

    return rv;
}

template<class T>
ParamEnum<T>::ParamEnum(Specification* pSpecification,
                        const char* zName,
                        const char* zDescription,
                        Param::Modifiable modifiable,
                        Param::Kind kind,
                        const std::vector<std::pair<T, const char*>>& enumeration,
                        value_type default_value)
    : ConcreteParam<ParamEnum<T>, T>(pSpecification, zName, zDescription,
                                     modifiable, kind, default_value)
    , m_enumeration(enumeration)
{
}

template<class T>
std::string ParamEnum<T>::type() const
{
    return "enum";
}

template<class T>
json_t* ParamEnum<T>::to_json() const
{
    auto rv = ConcreteParam<ParamEnum<T>, T>::to_json();
    auto arr = json_array();

    for (const auto& a : m_enumeration)
    {
        json_array_append_new(arr, json_string(a.second));
    }

    json_object_set_new(rv, "enum_values", arr);

    return rv;
}

template<class T>
std::string ParamEnum<T>::to_string(value_type value) const
{
    auto it = std::find_if(m_enumeration.begin(), m_enumeration.end(),
                           [value](const std::pair<T, const char*>& entry) {
        return entry.first == value;
    });

    return it != m_enumeration.end() ? it->second : "unknown";
}

template<class T>
bool ParamEnum<T>::from_string(const std::string& value_as_string,
                               value_type* pValue,
                               std::string* pMessage) const
{
    auto it = std::find_if(m_enumeration.begin(), m_enumeration.end(),
                           [value_as_string](const std::pair<T, const char*>& elem) {
        return value_as_string == elem.second;
    });

    if (it != m_enumeration.end())
    {
        *pValue = it->first;
    }
    else if (pMessage)
    {
        std::string s;
        for (size_t i = 0; i < m_enumeration.size(); ++i)
        {
            s += "'";
            s += m_enumeration[i].second;
            s += "'";

            if (i == m_enumeration.size() - 2)
            {
                s += " and ";
            }
            else if (i != m_enumeration.size() - 1)
            {
                s += ", ";
            }
        }

        *pMessage = "Invalid enumeration value: ";
        *pMessage += value_as_string;
        *pMessage += ", valid values are: ";
        *pMessage += s;
        *pMessage += ".";
    }

    return it != m_enumeration.end();
}

template<class T>
json_t* ParamEnum<T>::to_json(value_type value) const
{
    auto it = std::find_if(m_enumeration.begin(), m_enumeration.end(),
                           [value](const std::pair<T, const char*>& entry) {
        return entry.first == value;
    });

    return it != m_enumeration.end() ? json_string(it->second) : nullptr;
}

template<class T>
bool ParamEnum<T>::from_json(const json_t* pJson, value_type* pValue,
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

template<class T>
ParamEnumMask<T>::ParamEnumMask(Specification* pSpecification,
                                const char* zName,
                                const char* zDescription,
                                Param::Modifiable modifiable,
                                Param::Kind kind,
                                const std::vector<std::pair<T, const char*>>& enumeration,
                                value_type default_value)
    : ConcreteParam<ParamEnumMask<T>, uint32_t>(pSpecification, zName, zDescription,
                                                modifiable, kind, default_value)
    , m_enumeration(enumeration)
{
}

template<class T>
std::string ParamEnumMask<T>::type() const
{
    return "enum_mask";
}

template<class T>
json_t* ParamEnumMask<T>::to_json() const
{
    auto rv = ConcreteParam<ParamEnumMask<T>, uint32_t>::to_json();
    auto arr = json_array();

    for (const auto& a : m_enumeration)
    {
        json_array_append_new(arr, json_string(a.second));
    }

    json_object_set_new(rv, "enum_values", arr);

    return rv;
}

template<class T>
std::string ParamEnumMask<T>::to_string(value_type value) const
{
    std::vector<std::string> values;

    for (const auto& entry : m_enumeration)
    {
        if (value & entry.first)
        {
            values.push_back(entry.second);
        }
    }

    return mxb::join(values, ",");
}

template<class T>
bool ParamEnumMask<T>::from_string(const std::string& value_as_string,
                                   value_type* pValue,
                                   std::string* pMessage) const
{
    bool rv = true;

    value_type value = 0;

    auto enum_values = mxb::strtok(value_as_string, ",");

    for (auto enum_value : enum_values)
    {
        mxb::trim(enum_value);

        auto it = std::find_if(m_enumeration.begin(), m_enumeration.end(),
                               [enum_value](const std::pair<T, const char*>& elem) {
            return enum_value == elem.second;
        });

        if (it != m_enumeration.end())
        {
            value |= it->first;
        }
        else
        {
            rv = false;
            break;
        }
    }

    if (rv)
    {
        *pValue = value;
    }
    else if (pMessage)
    {
        std::string s;
        for (size_t i = 0; i < m_enumeration.size(); ++i)
        {
            s += "'";
            s += m_enumeration[i].second;
            s += "'";

            if (i == m_enumeration.size() - 2)
            {
                s += " and ";
            }
            else if (i != m_enumeration.size() - 1)
            {
                s += ", ";
            }
        }

        *pMessage = "Invalid enumeration value: ";
        *pMessage += value_as_string;
        *pMessage += ", valid values are a combination of: ";
        *pMessage += s;
        *pMessage += ".";
    }

    return rv;
}

template<class T>
json_t* ParamEnumMask<T>::to_json(value_type value) const
{
    return json_string(to_string(value).c_str());
}

template<class T>
bool ParamEnumMask<T>::from_json(const json_t* pJson, value_type* pValue,
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

template<class ParamType, class ConcreteConfiguration, class NativeParamType>
void Configuration::add_native(typename ParamType::value_type ConcreteConfiguration::* pValue,
                               ParamType* pParam,
                               std::function<void(typename ParamType::value_type)> on_set)
{
    ConcreteConfiguration* pThis = static_cast<ConcreteConfiguration*>(this);
    pThis->*pValue = pParam->default_value();
    m_natives.push_back(std::unique_ptr<Type>(new NativeParamType(pThis, pParam, pValue, on_set)));
}

template<class ParamType, class ConcreteConfiguration, class Container>
void
Configuration::add_native(Container ConcreteConfiguration::* pContainer,
                          typename ParamType::value_type Container::* pValue,
                          ParamType* pParam,
                          std::function<void(typename ParamType::value_type)> on_set)
{
    ConcreteConfiguration* pThis = static_cast<ConcreteConfiguration*>(this);
    (pThis->*pContainer).*pValue = pParam->default_value();

    auto* pType = new ContainedNative<ParamType, ConcreteConfiguration, Container>(
        pThis, pParam, pContainer, pValue, on_set);
    m_natives.push_back(std::unique_ptr<Type>(pType));
}

template<class ParamType, class ConcreteConfiguration, class Container, int N>
void
Configuration::add_native(Container(ConcreteConfiguration::* pArray)[N],
                          int index,
                          typename ParamType::value_type Container::* pValue,
                          ParamType* pParam,
                          std::function<void(typename ParamType::value_type)> on_set)
{
    ConcreteConfiguration* pThis = static_cast<ConcreteConfiguration*>(this);
    (pThis->*pArray)[index].*pValue = pParam->default_value();

    auto* pType = new IndexedContainedNative<ParamType, ConcreteConfiguration, Container, N>(
        pThis, pParam, pArray, index, pValue, on_set);
    m_natives.push_back(std::unique_ptr<Type>(pType));
}
}
}

inline std::ostream& operator<<(std::ostream& out, const mxs::config::RegexValue& value)
{
    out << value.pattern();
    return out;
}
