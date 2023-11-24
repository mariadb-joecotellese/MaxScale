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

namespace urat
{

Specification specification(MXB_MODULE_NAME, config::Specification::ROUTER);

config::ParamEnum<ExporterType> exporter(
    &specification, "exporter", "Exporter to use",
    {
        {ExporterType::EXPORT_FILE, "file"},
        {ExporterType::EXPORT_KAFKA, "kafka"},
        {ExporterType::EXPORT_LOG, "log"}
    }, config::Param::AT_RUNTIME);

config::ParamTarget main(
    &specification, "main", "Server from which responses are returned",
    config::Param::Kind::MANDATORY, config::Param::AT_RUNTIME);

config::ParamString file(
    &specification, "file", "File where data is exported", "", config::Param::AT_RUNTIME);

config::ParamString kafka_broker(
    &specification, "kafka_broker", "Kafka broker to use", "", config::Param::AT_RUNTIME);

config::ParamString kafka_topic(
    &specification, "kafka_topic", "Kafka topic where data is exported", "", config::Param::AT_RUNTIME);

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
        {ReportAction::REPORT_ON_CONFLICT, "on_conflict"},
    },
    ReportAction::REPORT_ALWAYS, config::Param::AT_RUNTIME);

config::ParamService service(
    &specification, "service", "The service the Urat service is installed for",
    config::Param::Kind::MANDATORY);
}


template<class Params>
bool Specification::do_post_validate(Params& params) const
{
    bool ok = true;

    switch (urat::exporter.get(params))
    {
    case ExporterType::EXPORT_LOG:
        break;

    case ExporterType::EXPORT_FILE:
        if (urat::file.get(params).empty())
        {
            MXB_ERROR("'%s' must be defined when exporter=file is used.", urat::file.name().c_str());
            ok = false;
        }
        break;

    case ExporterType::EXPORT_KAFKA:
        if (urat::kafka_broker.get(params).empty() || urat::kafka_topic.get(params).empty())
        {
            MXB_ERROR("Both '%s' and '%s' must be defined when exporter=kafka is used.",
                      urat::kafka_broker.name().c_str(), urat::kafka_topic.name().c_str());
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
    return &urat::specification;
}

UratConfig::UratConfig(const char* zName, UratRouter* pInstance)
    : mxs::config::Configuration(zName, &urat::specification)
    , on_error(this, &urat::on_error)
    , report(this, &urat::report)
    , m_instance(*pInstance)
{
    add_native(&UratConfig::exporter, &urat::exporter);
    add_native(&UratConfig::main, &urat::main);
    add_native(&UratConfig::file, &urat::file);
    add_native(&UratConfig::kafka_broker, &urat::kafka_broker);
    add_native(&UratConfig::kafka_topic, &urat::kafka_topic);
    add_native(&UratConfig::service, &urat::service);
}

bool UratConfig::post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params)
{
    return m_instance.post_configure();
}
