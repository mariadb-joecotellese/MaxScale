/*
 * Copyright (c) 2020 MariaDB Corporation Ab
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

#include "nosqlprotocol.hh"
#include <bsoncxx/document/view.hpp>
#include <vector>

namespace nosql
{

namespace update_operator
{

bool is_supported(const std::string& name);

std::vector<std::string> supported_operators();

std::string convert(const bsoncxx::document::view& update_operators);

}

}

