/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxscale/ccdefs.hh>

/**
 * The global clock
 *
 * This value is incremented roughly every 100 milliseconds and may be used for
 * very crude timing. The crudeness is due to the fact that the housekeeper
 * thread does the updating of this value.
 *
 * @return The current clock tick
 */
int64_t mxs_clock();

/**
 * Convert heartbeats to seconds
 */
static inline int64_t MXS_CLOCK_TO_SEC(int64_t a)
{
    return a / 10;
}

/**
 * Convert seconds to heartbeats
 */
static inline int64_t MXS_SEC_TO_CLOCK(int64_t a)
{
    return a * 10;
}

