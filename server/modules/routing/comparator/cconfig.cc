/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "cconfig.hh"
#include "../../../core/internal/service.hh"
#include "crouter.hh"

namespace
{

namespace config = maxscale::config;

class Specification : public config::Specification
{
public:
    using config::Specification::Specification;

protected:
    template<class Params>
    bool do_post_validate(Params& params) const;

    bool post_validate(const config::Configuration* config,
                       const mxs::ConfigParameters& params,
                       const std::map<std::string, mxs::ConfigParameters>& nested_params) const override
    {
        return do_post_validate(params);
    }

    bool post_validate(const config::Configuration* config,
                       json_t* json,
                       const std::map<std::string, json_t*>& nested_params) const override
    {
        return do_post_validate(json);
    }
};

namespace comparator
{

Specification specification(MXB_MODULE_NAME, config::Specification::ROUTER);

config::ParamEnum<ComparisonKind> comparison_kind(
    &specification,
    "comparison_kind",
    "Is the comparison read-write or read-only.",
    {
        {ComparisonKind::READ_ONLY, "read_only"},
        {ComparisonKind::READ_WRITE, "read_write"}
    },
    DEFAULT_COMPARISON_KIND,
    config::Param::AT_STARTUP);

config::ParamSize entries(
    &specification,
    "entries",
    "During the period specified by 'period', at most how many entries are logged.",
    DEFAULT_ENTRIES, // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max
    config::Param::AT_RUNTIME);

config::ParamEnum<Explain> explain(
    &specification,
    "explain",
    "What results should be EXPLAINed; 'none', 'other' or 'both'.",
    {
        {Explain::NONE, "none"},
        {Explain::OTHER, "other"},
        {Explain::BOTH, "both"}
    },
    DEFAULT_EXPLAIN,
    config::Param::AT_RUNTIME);

config::ParamTarget main(
    &specification,
    "main",
    "Server from which responses are returned",
    config::Param::Kind::MANDATORY,
    config::Param::AT_RUNTIME);

config::ParamPercent max_execution_time_difference(
    &specification,
    "max_execution_time_difference",
    "Maximum allowed execution time difference, specified in percent, "
    "between the main and an other server before the result is logged.",
    DEFAULT_MAX_EXECUTION_TIME_DIFFERENCE, // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max
    config::Param::AT_RUNTIME);

config::ParamSize max_request_lag(
    &specification,
    "max_request_lag",
    "How many requests an 'other' server may lag behind the 'main' server "
    "before SELECTs are not sent to 'other' in order to reduce the lag.",
    DEFAULT_MAX_REQUEST_LAG, // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max,
    config::Param::AT_RUNTIME);

config::ParamEnum<OnError> on_error(
    &specification,
    "on_error",
    "What to do when a non-main connection fails",
    {
        {OnError::IGNORE, "ignore"},
        {OnError::CLOSE, "close"}
    },
    DEFAULT_ON_ERROR,
    config::Param::AT_RUNTIME);

config::ParamEnum<Report> report(
    &specification,
    "report",
    "When to generate the report for an SQL command",
    {
        {Report::ALWAYS, "always"},
        {Report::ON_DISCREPANCY, "on_discrepancy"}
    },
    DEFAULT_REPORT,
    config::Param::AT_RUNTIME);

config::ParamCount retain_faster_statements(
    &specification,
    "retain_faster_statements",
    "How many of the faster statements should be retained so that they are available in the summary.",
    DEFAULT_RETAIN_FASTER_STATEMENTS,
    config::Param::AT_RUNTIME);

config::ParamCount retain_slower_statements(
    &specification,
    "retain_slower_statements",
    "How many of the slower statements should be retained so that they are available in the summary.",
    DEFAULT_RETAIN_SLOWER_STATEMENTS,
    config::Param::AT_RUNTIME);

config::ParamString service(
    &specification,
    "service",
    "The service the Comparator service is installed for");

config::ParamDuration<std::chrono::milliseconds> period(
    &specification,
    "period",
    "Specifies the period during which at most 'entries' number of entries are logged.",
    DEFAULT_PERIOD,
    config::Param::AT_RUNTIME);
}



template<class Params>
bool Specification::do_post_validate(Params& params) const
{
    return true;
}
}

// static
mxs::config::Specification* CConfig::specification()
{
    return &comparator::specification;
}

CConfig::CConfig(const char* zName, CRouter* pInstance)
    : mxs::config::Configuration(zName, &comparator::specification)
    , on_error(this, &comparator::on_error)
    , report(this, &comparator::report)
    , m_instance(*pInstance)
{
    add_native(&CConfig::pMain, &comparator::main);
    add_native(&CConfig::service_name, &comparator::service);

    add_native(&CConfig::comparison_kind, &comparator::comparison_kind);
    add_native(&CConfig::entries, &comparator::entries);
    add_native(&CConfig::explain, &comparator::explain);
    add_native(&CConfig::max_execution_time_difference, &comparator::max_execution_time_difference);
    add_native(&CConfig::max_request_lag, &comparator::max_request_lag);
    add_native(&CConfig::period, &comparator::period);
    add_native(&CConfig::retain_faster_statements, &comparator::retain_faster_statements);
    add_native(&CConfig::retain_slower_statements, &comparator::retain_slower_statements);
}

bool CConfig::post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params)
{
    // The service will be found only if the comparator service is
    // created at runtime, but not if the comparator service is
    // created from a configuration file at MaxScale startup.

    this->pService = Service::find(this->service_name);

    return m_instance.post_configure();
}

bool CConfig::check_configuration()
{
    // This function is only called at MaxScale startup and the
    // service should now be found.

    bool rv = false;

    this->pService = Service::find(this->service_name);

    if (this->pService)
    {
        rv = m_instance.check_configuration();
    }
    else
    {
        const auto& sn = this->service_name;
        MXB_ERROR("Could not find service '%.*s' that '%s' depends on.",
                  (int)sn.length(), sn.data(), name().c_str());
    }

    return rv;
}
