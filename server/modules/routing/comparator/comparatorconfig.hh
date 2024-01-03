/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"

#include <maxscale/ccdefs.hh>
#include <maxscale/config2.hh>

enum ExporterType
{
    EXPORT_LOG,
    EXPORT_FILE,
    EXPORT_KAFKA,
};

enum ErrorAction
{
    ERRACT_IGNORE,
    ERRACT_CLOSE,
};

enum ReportAction
{
    REPORT_ALWAYS,
    REPORT_ON_DISCREPANCY,
};

class ComparatorRouter;

class ComparatorConfig : public mxs::config::Configuration
{
public:
    ComparatorConfig(const ComparatorConfig&) = delete;
    ComparatorConfig& operator=(const ComparatorConfig&) = delete;

    ComparatorConfig(const char* zName, ComparatorRouter* pInstance);

    mxs::Target* pMain;
    ExporterType exporter;
    std::string  file;
    std::string  kafka_broker;
    std::string  kafka_topic;

    mxs::config::Enum<ErrorAction>  on_error;
    mxs::config::Enum<ReportAction> report;

    int64_t explain_difference;
    int64_t max_execution_time_difference;

    SERVICE* pService;

    static mxs::config::Specification* specification();

protected:
    bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;

private:
    ComparatorRouter& m_instance;
};
