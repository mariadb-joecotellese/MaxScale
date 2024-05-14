/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "nosqlprotocol.hh"
#include <bsoncxx/builder/stream/document.hpp>
#include "nosqlcommon.hh"
#include "nosqlconfig.hh"
#include "protocolmodule.hh"

namespace
{

struct ThisUnit
{
    bsoncxx::oid             oid;
    bsoncxx::document::value topology_version;

    ThisUnit()
        : topology_version(bsoncxx::builder::stream::document()
                           << "processId" << this->oid
                           << "counter" << (int64_t)0
                           << bsoncxx::builder::stream::finalize)
    {
    }
} this_unit;

}

bsoncxx::document::value& nosql::topology_version()
{
    return this_unit.topology_version;
}

/**
 * nosqlprotocol module entry point.
 *
 * @return The module object
 */

extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::PROTOCOL,
        mxs::ModuleStatus::GA,
        MXS_PROTOCOL_VERSION,
        "MaxScale NoSQL client protocol implementation",
        "V1.0.0",
        MXS_NO_MODULE_CAPABILITIES,
        &mxs::ProtocolApiGenerator<ProtocolModule>::s_api,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        &Configuration::specification()
    };

    return &info;
}
