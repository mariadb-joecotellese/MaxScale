/*
 * Copyright (c) 2019 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

/**
 * Some module names are used in multiple files so the names are defined here.
 * As this file may be included before defining MXS_MODULE_NAME, ccdefs.hh should
 * not be included here.
 */
#define MXS_CDC_PROTOCOL_NAME "CDC"
