/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"

#include <maxscale/ccdefs.hh>
#include <maxscale/config2.hh>

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

    static mxs::config::ParamSize& param_entries();
    static mxs::config::ParamDuration<std::chrono::milliseconds>& param_period();

    static mxs::config::Specification& specification();

    //
    // Mandatory, Startup
    //
    SERVER*     pMain;
    SERVICE*    pService;
    std::string service_name;

    //
    // Optional, Runtime
    //
    int64_t                    entries;
    Explain                    explain;
    int64_t                    max_execution_time_difference;
    int64_t                    max_request_lag;
    mxs::config::Enum<OnError> on_error;
    double                     percentile;
    std::chrono::milliseconds  period;
    mxs::config::Enum<Report>  report;
    bool                       reset_replication;
    int64_t                    retain_faster_statements;
    int64_t                    retain_slower_statements;
    int64_t                    samples;

protected:
    bool post_configure(const std::map<std::string, mxs::ConfigParameters>& nested_params) override;

    bool check_configuration() override;

private:
    DiffRouter& m_instance;
};
