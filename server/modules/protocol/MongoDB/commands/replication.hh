/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2024-08-24
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

//
// https://docs.mongodb.com/manual/reference/command/nav-replication/
//

#include "defs.hh"

namespace mxsmongo
{

namespace command
{

// https://docs.mongodb.com/manual/reference/command/applyOps/

// https://docs.mongodb.com/manual/reference/command/isMaster/
class IsMaster : public mxsmongo::Command
{
public:
    using mxsmongo::Command::Command;

    GWBUF* execute() override
    {
        DocumentBuilder doc;

        doc.append(kvp("isMaster", true));
        doc.append(kvp("topologyVersion", mxsmongo::topology_version()));
        doc.append(kvp("maxBsonObjectSize", 16 * 1024 * 1024));
        doc.append(kvp("maxMessageSizeBytes", 48000000));
        doc.append(kvp("maxWriteBatchSize", 100000));
        doc.append(kvp("localTime", bsoncxx::types::b_date(std::chrono::system_clock::now())));
        doc.append(kvp("logicalSessionTimeoutMinutes", 30));
        doc.append(kvp("connectionId", 4)); // TODO: Return proper.
        doc.append(kvp("minWireVersion", 0)); // TODO: Return proper minimum.
        doc.append(kvp("maxWireVersion", 9)); // TODO: Return proper maximum.
        doc.append(kvp("readOnly", false));
        doc.append(kvp("ok", 1));

        return create_response(doc.extract());
    }
};


// https://docs.mongodb.com/manual/reference/command/replSetAbortPrimaryCatchUp/

// https://docs.mongodb.com/manual/reference/command/replSetFreeze/

// https://docs.mongodb.com/manual/reference/command/replSetGetConfig/

// https://docs.mongodb.com/manual/reference/command/replSetGetStatus/
class ReplSetGetStatus : public mxsmongo::Command
{
public:
    using mxsmongo::Command::Command;

    GWBUF* execute() override
    {
        DocumentBuilder doc;

        doc.append(kvp("ok", 0));
        doc.append(kvp("errmsg", "not running with --replSet"));
        doc.append(kvp("code", (int32_t)error::Code::NO_REPLICATION_ENABLED));
        doc.append(kvp("codeName", "NoReplicationEnabled"));

        return create_response(doc.extract());
    }
};


// https://docs.mongodb.com/manual/reference/command/replSetInitiate/

// https://docs.mongodb.com/manual/reference/command/replSetMaintenance/

// https://docs.mongodb.com/manual/reference/command/replSetReconfig/

// https://docs.mongodb.com/manual/reference/command/replSetResizeOplog/

// https://docs.mongodb.com/manual/reference/command/replSetStepDown/

// https://docs.mongodb.com/manual/reference/command/replSetSyncFrom/


}

}
