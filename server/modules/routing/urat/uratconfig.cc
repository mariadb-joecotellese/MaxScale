/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "uratconfig.hh"
#include "uratrouter.hh"

namespace
{

namespace config = maxscale::config;

class UratSpec : public config::Specification
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

UratSpec s_spec(MXB_MODULE_NAME, config::Specification::ROUTER);

config::ParamEnum<ExporterType> s_exporter(
    &s_spec, "exporter", "Exporter to use",
    {
        {ExporterType::EXPORT_FILE, "file"},
        {ExporterType::EXPORT_KAFKA, "kafka"},
        {ExporterType::EXPORT_LOG, "log"}
    }, config::Param::AT_RUNTIME);

config::ParamTarget s_main(
    &s_spec, "main", "Server from which responses are returned",
    config::Param::Kind::MANDATORY, config::Param::AT_RUNTIME);

config::ParamString s_file(
    &s_spec, "file", "File where data is exported", "", config::Param::AT_RUNTIME);

config::ParamString s_kafka_broker(
    &s_spec, "kafka_broker", "Kafka broker to use", "", config::Param::AT_RUNTIME);

config::ParamString s_kafka_topic(
    &s_spec, "kafka_topic", "Kafka topic where data is exported", "", config::Param::AT_RUNTIME);

config::ParamEnum<ErrorAction> s_on_error(
    &s_spec, "on_error", "What to do when a non-main connection fails",
    {
        {ErrorAction::ERRACT_IGNORE, "ignore"},
        {ErrorAction::ERRACT_CLOSE, "close"},
    },
    ErrorAction::ERRACT_IGNORE, config::Param::AT_RUNTIME);

config::ParamEnum<ReportAction> s_report(
    &s_spec, "report", "When to generate the report for an SQL command",
    {
        {ReportAction::REPORT_ALWAYS, "always"},
        {ReportAction::REPORT_ON_CONFLICT, "on_conflict"},
    },
    ReportAction::REPORT_ALWAYS, config::Param::AT_RUNTIME);

template<class Params>
bool UratSpec::do_post_validate(Params& params) const
{
    bool ok = true;

    switch (s_exporter.get(params))
    {
    case ExporterType::EXPORT_LOG:
        break;

    case ExporterType::EXPORT_FILE:
        if (s_file.get(params).empty())
        {
            MXB_ERROR("'%s' must be defined when exporter=file is used.", s_file.name().c_str());
            ok = false;
        }
        break;

    case ExporterType::EXPORT_KAFKA:
        if (s_kafka_broker.get(params).empty() || s_kafka_topic.get(params).empty())
        {
            MXB_ERROR("Both '%s' and '%s' must be defined when exporter=kafka is used.",
                      s_kafka_broker.name().c_str(), s_kafka_topic.name().c_str());
            ok = false;
        }
        break;
    }

    return ok;
}
}

// static
mxs::config::Specification* UratConfig::specification()
{
    return &s_spec;
}

UratConfig::UratConfig(const char* zName, UratRouter* pInstance)
    : mxs::config::Configuration(zName, &s_spec)
    , on_error(this, &s_on_error)
    , report(this, &s_report)
    , m_instance(*pInstance)
{
    add_native(&UratConfig::exporter, &s_exporter);
    add_native(&UratConfig::main, &s_main);
    add_native(&UratConfig::file, &s_file);
    add_native(&UratConfig::kafka_broker, &s_kafka_broker);
    add_native(&UratConfig::kafka_topic, &s_kafka_topic);
}

bool UratConfig::post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params)
{
    return m_instance.post_configure();
}
