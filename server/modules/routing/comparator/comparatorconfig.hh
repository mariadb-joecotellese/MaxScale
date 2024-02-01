/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"

#include <maxscale/ccdefs.hh>
#include <maxscale/config2.hh>

enum class ComparisonKind
{
    READ_ONLY,
    READ_WRITE
};

enum Explain
{
    OTHER = 1 << 0,
    MAIN  = 1 << 1
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

/* *INDENT-OFF* */
constexpr ComparisonKind            DEFAULT_COMPARISON_KIND { ComparisonKind::READ_WRITE };
constexpr int64_t                   DEFAULT_ENTRIES { 2 };
constexpr Explain                   DEFAULT_EXPLAIN { Explain::OTHER };
constexpr int64_t                   DEFAULT_MAX_EXECUTION_TIME_DIFFERENCE { 10 };
constexpr int64_t                   DEFAULT_MAX_REQUEST_LAG { 10 };
constexpr OnError                   DEFAULT_ON_ERROR { OnError::IGNORE };
constexpr std::chrono::milliseconds DEFAULT_PERIOD { 60 * 60 * 1000 };
constexpr Report                    DEFAULT_REPORT { Report::ON_DISCREPANCY };
/* *INDENT-ON* */

class ComparatorRouter;

class ComparatorConfig : public mxs::config::Configuration
{
public:
    ComparatorConfig(const ComparatorConfig&) = delete;
    ComparatorConfig& operator=(const ComparatorConfig&) = delete;

    ComparatorConfig(const char* zName, ComparatorRouter* pInstance);

    mxs::Target* pMain;

    ComparisonKind             comparison_kind;
    uint32_t                   explain;
    mxs::config::Enum<OnError> on_error;
    mxs::config::Enum<Report>  report;
    int64_t                    max_execution_time_difference;
    int64_t                    entries;
    std::chrono::milliseconds  period;

    int64_t                    max_request_lag;

    bool explain_main() const
    {
        return this->explain & Explain::MAIN;
    }

    bool explain_other() const
    {
        return this->explain & Explain::OTHER;
    }

    bool explain_all() const
    {
        return explain_main() && explain_other();
    }

    SERVICE* pService;

    static mxs::config::Specification* specification();

protected:
    bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;

private:
    ComparatorRouter& m_instance;
};
