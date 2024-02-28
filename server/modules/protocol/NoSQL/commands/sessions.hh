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

//
// https://docs.mongodb.com/v4.4/reference/command/nav-sessions/
//

#include "defs.hh"

namespace nosql
{

namespace command
{

// https://docs.mongodb.com/v4.4/reference/command/abortTransaction/

// https://docs.mongodb.com/v4.4/reference/command/commitTransaction/

// https://docs.mongodb.com/v4.4/reference/command/endSessions/
class EndSessions final : public ImmediateCommand
{
public:
    static constexpr const char* const KEY = "endSessions";
    static constexpr const char* const HELP = "";

    using ImmediateCommand::ImmediateCommand;

    Response::Status populate_response(DocumentBuilder& doc) override
    {
        doc.append(kvp(key::OK, 1));

        return Response::Status::NOT_CACHEABLE;
    }
};

// https://docs.mongodb.com/v4.4/reference/command/killAllSessions/

// https://docs.mongodb.com/v4.4/reference/command/killAllSessionsByPattern/

// https://docs.mongodb.com/v4.4/reference/command/killSessions/

// https://docs.mongodb.com/v4.4/reference/command/refreshSessions/

// https://docs.mongodb.com/v4.4/reference/command/startSession/


}

}
