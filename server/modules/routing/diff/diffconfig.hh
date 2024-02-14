/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"

#include <maxscale/ccdefs.hh>
#include <maxscale/config2.hh>

enum class ComparisonKind
{
    READ_ONLY,
    READ_WRITE
};

enum class Explain
{
    NONE,
    OTHER,
    BOTH
};

enum class OnError
{
    IGNORE,
    CLOSE,
};

enum class Report
{
    ALWAYS,
    ON_DISCREPANCY,
};

class DiffRouter;

class DiffConfig : public mxs::config::Configuration
{
public:
    DiffConfig(const DiffConfig&) = delete;
    DiffConfig& operator=(const DiffConfig&) = delete;

    DiffConfig(const char* zName, DiffRouter* pInstance);

    SERVER* pMain;

    ComparisonKind             comparison_kind;
    Explain                    explain;
    mxs::config::Enum<OnError> on_error;
    mxs::config::Enum<Report>  report;
    int64_t                    max_execution_time_difference;
    int64_t                    entries;
    std::chrono::milliseconds  period;

    int64_t                    max_request_lag;

    int64_t                    retain_faster_statements;
    int64_t                    retain_slower_statements;

    SERVICE*    pService;
    std::string service_name;

    static mxs::config::Specification* specification();

protected:
    bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;

    bool check_configuration() override;

private:
    DiffRouter& m_instance;
};

/* *INDENT-OFF* */
constexpr ComparisonKind            DEFAULT_COMPARISON_KIND { ComparisonKind::READ_WRITE };
constexpr int64_t                   DEFAULT_ENTRIES { 2 };
constexpr Explain                   DEFAULT_EXPLAIN { Explain::BOTH };
constexpr int64_t                   DEFAULT_MAX_EXECUTION_TIME_DIFFERENCE { 10 };
constexpr int64_t                   DEFAULT_MAX_REQUEST_LAG { 10 };
constexpr OnError                   DEFAULT_ON_ERROR { OnError::IGNORE };
constexpr std::chrono::milliseconds DEFAULT_PERIOD { 60 * 60 * 1000 };
constexpr Report                    DEFAULT_REPORT { Report::ON_DISCREPANCY };
constexpr int64_t                   DEFAULT_RETAIN_FASTER_STATEMENTS { 5 };
constexpr int64_t                   DEFAULT_RETAIN_SLOWER_STATEMENTS { 5 };
/* *INDENT-ON* */
