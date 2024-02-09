/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxbase/ccdefs.hh>
#include <unistd.h>
#include <cstring>

namespace maxbase
{

static inline void default_stacktrace_handler(const char* line)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    write(STDOUT_FILENO, line, strlen(line));
    write(STDOUT_FILENO, "\n", 1);
#pragma GCC diagnostic pop
}

static inline void default_gdb_stacktrace_handler(const char* line)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    write(STDOUT_FILENO, line, strlen(line));
#pragma GCC diagnostic pop
}

/**
 * Dumps a stacktrace to stdout
 *
 * Useful when something is seriously broken and we can't rely on the logging subsystem to work.
 */
void emergency_stacktrace();

/**
 * Dump the stacktrace of the current thread
 *
 * @param handler A handler that is called once per stack frame with the function name and auxiliary
 *                information (file and line). By default the stacktrace is dumped to stdout.
 */
void dump_stacktrace(void (* handler)(const char* output) = default_stacktrace_handler);

/**
 * Dump a better stacktrace using GDB
 *
 * This version dumps stacktraces from all threads.
 *
 * @param handler A handler that is called to print output. By default the output is dumped to stdout.
 */
void dump_gdb_stacktrace(void (* handler)(const char* output) = default_gdb_stacktrace_handler);

/**
 * Check if GDB is installed and available
 *
 * @return True if GDB can be invoked with the system() function
 */
bool have_gdb();

/**
 * Get the demangled symbol name of the address
 *
 * @param addr Address to resolve
 *
 * @return The demangled symbol name
 */
std::string addr_to_symbol(void* addr);
}
