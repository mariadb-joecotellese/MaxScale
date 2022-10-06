/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2026-10-04
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

/**
 * @file ccdefs.hh
 *
 * This file is to be included first by all C++ headers.
 */

#if !defined (__cplusplus)
#error This file is only to be included by C++ code.
#endif

#include <maxscale/cdefs.h>
#include <maxbase/ccdefs.hh>
#include <maxscale/log.hh>

/**
 * All classes of MaxScale are defined in the namespace @c maxscale.
 *
 * Third party plugins should not place any definitions inside the namespace
 * to avoid any name clashes in the future. An exception are template
 * specializations.
 */
namespace maxscale
{
/**
 * Address used for initializing pointers to invalid values. Points to kernel space on
 * 64-bit systems so it's guaranteed to be an invalid userspace address.
 */
constexpr intptr_t BAD_ADDR = 0xDEADBEEFDEADBEEF;
}

/**
 * Shorthand for the @c maxscale namespace.
 */
namespace mxs = maxscale;

/**
 * If a statement is placed inside this macro, then no exceptions will
 * escape. Typical use-case is for preventing exceptions to escape across
 * a C-API.
 *
 * @code{.cpp}
 *
 *   void* cAPI()
 *   {
 *       void* rv = NULL;
 *       MXS_EXCEPTION_GUARD(rv = new Something);
 *       return rv;
 *   }
 * @endcode
 */
#if defined (SS_DEBUG)
#define MXS_EXCEPTION_GUARD(statement) do {statement;} while (false);
#else
#define MXS_EXCEPTION_GUARD(statement) \
    do {try {statement;} \
        catch (const std::bad_alloc&) {MXS_OOM();} \
        catch (const std::exception& x) {MXS_ERROR("Caught standard exception: %s", x.what());} \
        catch (...) {MXS_ERROR("Caught unknown exception.");}} while (false)
#endif
