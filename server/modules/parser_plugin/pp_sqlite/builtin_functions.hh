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

#include <stdbool.h>
#include <stdint.h>

void init_builtin_functions();
void finish_builtin_functions();

bool is_builtin_readonly_function(const char* zToken,
                                  uint32_t major,
                                  uint32_t minor,
                                  uint32_t patch,
                                  bool check_oracle);
