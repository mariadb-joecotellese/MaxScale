/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffconfig.hh"
#include "../../../core/internal/service.hh"
#include "diffrouter.hh"

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

namespace diff
{

Specification specification(MXB_MODULE_NAME, config::Specification::ROUTER);

//
// Startup Parameters
//
config::ParamServer main(
    &specification,
    "main",
    "Server from which responses are returned",
    config::Param::Kind::MANDATORY);

config::ParamString service(
    &specification,
    "service",
    "The service the Diff service is installed for");

//
// Runtime Parameters
//
config::ParamEnum<Explain> explain(
    &specification,
    "explain",
    "What results should be EXPLAINed; 'none', 'other' or 'both'.",
    {
        {Explain::NONE, "none"},
        {Explain::OTHER, "other"},
        {Explain::BOTH, "both"}
    },
    Explain::BOTH, // Default
    config::Param::AT_RUNTIME);

config::ParamSize explain_entries(
    &specification,
    "explain_entries",
    "During the period specified by 'explain_period', at most how many instances of a "
    "particular query digest are EXPLAINed.",
    5, // Default
    0, // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max
    config::Param::AT_RUNTIME);

config::ParamDuration<std::chrono::milliseconds> explain_period(
    &specification,
    "explain_period",
    "Specifies the period during which at most 'explain_entries' number of instances of a "
    "particular query digest are EXPLAINED.",
    std::chrono::milliseconds { 15 * 60 * 1000 }, // Default, 15 minutes
    config::Param::AT_RUNTIME);

config::ParamSize max_request_lag(
    &specification,
    "max_request_lag",
    "How many requests an 'other' server may lag behind the 'main' server "
    "before SELECTs are not sent to 'other' in order to reduce the lag.",
    10, // Default
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
    OnError::IGNORE, // Default
    config::Param::AT_RUNTIME);

config::ParamPercent percentile(
    &specification,
    "percentile",
    "Specifies the percentile of sampels that will be considered when calculating the width "
    "and number of bins of the histogram.",
    99, // Default
    1, // Min
    100,  // Max
    config::Param::AT_RUNTIME);

config::ParamDuration<std::chrono::seconds> qps_window(
    &specification,
    "qps_window",
    "Specifies the size of the sliding window during which QPS is calculated.",
    std::chrono::seconds { 15 * 60 }, // Default, 15 minutes
    config::Param::AT_STARTUP);

config::ParamEnum<Report> report(
    &specification,
    "report",
    "When to generate the report for an SQL command",
    {
        {Report::ALWAYS, "always"},
        {Report::ON_DISCREPANCY, "on_discrepancy"}
    },
    Report::ON_DISCREPANCY, // Default
    config::Param::AT_RUNTIME);

config::ParamBool reset_replication(
    &specification,
    "reset_replication",
    "Whether the replication should be reset at the end, if it was stopped at the start.",
    true, // Default
    config::Param::AT_RUNTIME);

config::ParamCount retain_faster_statements(
    &specification,
    "retain_faster_statements",
    "How many of the faster statements should be retained so that they are available in the summary.",
    5, // Default
    config::Param::AT_RUNTIME);

config::ParamCount retain_slower_statements(
    &specification,
    "retain_slower_statements",
    "How many of the slower statements should be retained so that they are available in the summary.",
    5, // Default
    config::Param::AT_RUNTIME);

config::ParamCount samples(
    &specification,
    "samples",
    "How many samples per canonical statement should be collected before calculating the width "
    "and number of bins of the histogram.",
    1000, // Default
    100,  // Min
    std::numeric_limits<config::ParamCount::value_type>::max(), // Max
    config::Param::AT_RUNTIME);
}

template<class Params>
bool Specification::do_post_validate(Params& params) const
{
    return true;
}
}

DiffConfig::DiffConfig(const char* zName, DiffRouter* pInstance)
    : mxs::config::Configuration(zName, &diff::specification)
    , on_error(this, &diff::on_error)
    , report(this, &diff::report)
    , m_instance(*pInstance)
{
    add_native(&DiffConfig::pMain, &diff::main);
    add_native(&DiffConfig::service_name, &diff::service);

    add_native(&DiffConfig::explain, &diff::explain);
    add_native(&DiffConfig::explain_entries, &diff::explain_entries);
    add_native(&DiffConfig::explain_period, &diff::explain_period);
    add_native(&DiffConfig::max_request_lag, &diff::max_request_lag);
    add_native(&DiffConfig::percentile, &diff::percentile);
    add_native(&DiffConfig::qps_window, &diff::qps_window);
    add_native(&DiffConfig::reset_replication, &diff::reset_replication);
    add_native(&DiffConfig::retain_faster_statements, &diff::retain_faster_statements);
    add_native(&DiffConfig::retain_slower_statements, &diff::retain_slower_statements);
    add_native(&DiffConfig::samples, &diff::samples);
}

//static
mxs::config::ParamSize& DiffConfig::param_explain_entries()
{
    return diff::explain_entries;
}

//static
mxs::config::ParamDuration<std::chrono::milliseconds>& DiffConfig::param_explain_period()
{
    return diff::explain_period;
}

// static
mxs::config::Specification& DiffConfig::specification()
{
    return diff::specification;
}

bool DiffConfig::post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params)
{
    // The service will be found only if the diff service is
    // created at runtime, but not if the diff service is
    // created from a configuration file at MaxScale startup.

    this->pService = Service::find(this->service_name);

    return m_instance.post_configure();
}

bool DiffConfig::check_configuration()
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
