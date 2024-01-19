/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"

#include <maxscale/ccdefs.hh>
#include <maxscale/config2.hh>

enum class ErrorAction
{
    ERRACT_IGNORE,
    ERRACT_CLOSE,
};

enum class ReportAction
{
    REPORT_ALWAYS,
    REPORT_ON_DISCREPANCY,
};

enum class ComparisonKind
{
    READ_ONLY,
    READ_WRITE
};

class ComparatorRouter;

class ComparatorConfig : public mxs::config::Configuration
{
public:
    ComparatorConfig(const ComparatorConfig&) = delete;
    ComparatorConfig& operator=(const ComparatorConfig&) = delete;

    ComparatorConfig(const char* zName, ComparatorRouter* pInstance);

    mxs::Target* pMain;

    ComparisonKind                  comparison_kind;
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
