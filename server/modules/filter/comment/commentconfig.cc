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
#define MXB_MODULE_NAME "commentfilter"

#include <maxscale/ccdefs.hh>
#include "commentconfig.hh"

namespace comment
{

namespace config = mxs::config;

config::Specification specification(MXB_MODULE_NAME, config::Specification::FILTER);

config::ParamString inject(
    &specification,
    "inject",
    "This string is injected as a comment before the statement. If the string "
    "contains $IP, it will be replaced with the IP of the client.",
    config::ParamString::Quotes::REQUIRED,
    mxs::config::Param::AT_RUNTIME);
}

CommentConfig::CommentConfig(const std::string& name)
    : mxs::config::Configuration(name, &comment::specification)
    , inject(this, &comment::inject)
{
}

// static
void CommentConfig::populate(MXS_MODULE& info)
{
    info.specification = &comment::specification;
}
