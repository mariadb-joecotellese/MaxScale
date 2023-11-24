/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"

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
    REPORT_ON_CONFLICT,
};

class UratRouter;

class UratConfig : public mxs::config::Configuration
{
public:
    UratConfig(const UratConfig&) = delete;
    UratConfig& operator=(const UratConfig&) = delete;

    UratConfig(const char* zName, UratRouter* pInstance);

    mxs::Target* main;
    ExporterType exporter;
    std::string  file;
    std::string  kafka_broker;
    std::string  kafka_topic;

    mxs::config::Enum<ErrorAction>  on_error;
    mxs::config::Enum<ReportAction> report;

    SERVICE*     service;

    static mxs::config::Specification* specification();

protected:
    bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;

private:
    UratRouter& m_instance;
};
