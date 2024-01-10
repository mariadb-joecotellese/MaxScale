/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"

struct ComparatorStats
{
    std::chrono::nanoseconds total_duration { 0 };
    int64_t                  nRequest_packets { 0 };
    int64_t                  nRequests { 0 };
    int64_t                  nResponses { 0 };
};

struct ComparatorMainStats : ComparatorStats
{
    // TODO: Placeholder.
};

struct ComparatorOtherStats : ComparatorStats
{
    int64_t nFaster { 0 };
    int64_t nSlower { 0 };
};
