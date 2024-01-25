/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorconfig.hh"
#include "comparatorrouter.hh"

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
    &specification, "comparison_kind", "Is the comparison read-write or read-only",
    {
        {ComparisonKind::READ_ONLY, "read_only"},
        {ComparisonKind::READ_WRITE, "read_write"},
            },
    ComparisonKind::READ_WRITE, config::Param::AT_STARTUP);

config::ParamPercent explain_difference(
    &specification,
    "explain_difference",
    "Execution time difference, specified in percent, between the main and an other server "
    "that triggers an EXPLAIN on the statement on the other.",
    0, // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max
    config::Param::AT_RUNTIME);

config::ParamSize explain_iterations(
    &specification,
    "explain_iterations",
    "In case a statement is reported, on how many occurrences should EXPLAIN be executed.",
    DEFAULT_EXPLAIN_ITERATIONS, // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max
    config::Param::AT_RUNTIME);

config::ParamTarget main(
    &specification, "main", "Server from which responses are returned",
    config::Param::Kind::MANDATORY, config::Param::AT_RUNTIME);

config::ParamPercent max_execution_time_difference(
    &specification, "max_execution_time_difference", "Maximum allowed execution time difference, "
    "specified in percent, between the main and an other server before the result is logged.",
    10, // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max
    config::Param::AT_RUNTIME);

config::ParamSize max_request_lag(
    &specification,
    "max_request_lag",
    "How many requests an 'other' server may lag behind the 'main' server "
    "before SELECTs are not sent to 'other' in order to reduce the lag.",
    std::numeric_limits<config::ParamCount::value_type>::max(), // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max,
    config::Param::AT_RUNTIME);

config::ParamEnum<ErrorAction> on_error(
    &specification, "on_error", "What to do when a non-main connection fails",
    {
        {ErrorAction::ERRACT_IGNORE, "ignore"},
        {ErrorAction::ERRACT_CLOSE, "close"},
            },
    ErrorAction::ERRACT_IGNORE, config::Param::AT_RUNTIME);

config::ParamEnum<ReportAction> report(
    &specification, "report", "When to generate the report for an SQL command",
    {
        {ReportAction::REPORT_ALWAYS, "always"},
        {ReportAction::REPORT_ON_DISCREPANCY, "on_discrepancy"},
            },
    ReportAction::REPORT_ALWAYS, config::Param::AT_RUNTIME);

config::ParamService service(
    &specification, "service", "The service the Comparator service is installed for",
    config::Param::Kind::MANDATORY);
}


template<class Params>
bool Specification::do_post_validate(Params& params) const
{
    return true;
}
}

// static
mxs::config::Specification* ComparatorConfig::specification()
{
    return &comparator::specification;
}

ComparatorConfig::ComparatorConfig(const char* zName, ComparatorRouter* pInstance)
    : mxs::config::Configuration(zName, &comparator::specification)
    , on_error(this, &comparator::on_error)
    , report(this, &comparator::report)
    , m_instance(*pInstance)
{
    add_native(&ComparatorConfig::pMain, &comparator::main);
    add_native(&ComparatorConfig::pService, &comparator::service);

    add_native(&ComparatorConfig::max_execution_time_difference, &comparator::max_execution_time_difference);
    add_native(&ComparatorConfig::explain_difference, &comparator::explain_difference);
    add_native(&ComparatorConfig::explain_iterations, &comparator::explain_iterations);
    add_native(&ComparatorConfig::comparison_kind, &comparator::comparison_kind);
    add_native(&ComparatorConfig::max_request_lag, &comparator::max_request_lag);
}

bool ComparatorConfig::post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params)
{
    return m_instance.post_configure();
}
