/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxscale/ccdefs.hh>
#include <maxscale/event.hh>

namespace maxscale
{

namespace event
{

enum result_t
{
    IGNORED,    /**< The configuration was ignored, it does not affect events. */
    INVALID,    /**< The configuration was invalid. */
    ACCEPTED    /**< The configuration was accepted. */
};

/**
 * @brief Configure an event
 *
 * @param zName    A MaxScale event configuration item name,
 *                 such as "event.authentication_failure.facility"
 * @param zValue   The value it should be set to, e.g. "LOG_ERROR".
 *
 * @return IGNORED, if @c zName does not start with "event.",
 *         INVALID, if @c zName or @c zValue is invalid, and
 *         ACCEPTED, if @c zName and @c zValue are valid.
 */
result_t configure(const char* zName, const char* zValue);

inline result_t configure(const std::string& name, const std::string& value)
{
    return configure(name.c_str(), value.c_str());
}

/**
 * @brief Validate an event
 *
 * @param zName    A MaxScale event configuration item name,
 *                 such as "event.authentication_failure.facility"
 * @param zValue   The value it should be set to, e.g. "LOG_ERROR".
 *
 * @return IGNORED, if @c zName does not start with "event.",
 *         INVALID, if @c zName or @c zValue is invalid, and
 *         ACCEPTED, if @c zName and @c zValue are valid.
 */
result_t validate(const char* zName, const char* zValue);

inline result_t validate(const std::string& name, const std::string& value)
{
    return validate(name.c_str(), value.c_str());
}
}
}
