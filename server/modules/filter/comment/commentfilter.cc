/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

// All log messages from this module are prefixed with this
#define MXB_MODULE_NAME "commentfilter"

#include "commentfilter.hh"
#include <string>
#include <cstring>
#include "commentconfig.hh"

// This declares a module in MaxScale
extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::FILTER,
        mxs::ModuleStatus::GA,
        MXS_FILTER_VERSION,
        "A comment filter that can inject comments in sql queries",
        "V1.0.0",
        RCAP_TYPE_NONE,
        &mxs::FilterApi<CommentFilter>::s_api,
        NULL,                       /* Process init. */
        NULL,                       /* Process finish. */
        NULL,                       /* Thread init. */
        NULL,                       /* Thread finish. */
    };

    static bool populated = false;

    if (!populated)
    {
        CommentConfig::populate(info);
        populated = true;
    }

    return &info;
}

CommentFilter::CommentFilter(const std::string& name)
    : m_config(name)
{
}

// static
CommentFilter* CommentFilter::create(const char* zName)
{
    return new CommentFilter(zName);
}

std::shared_ptr<mxs::FilterSession> CommentFilter::newSession(MXS_SESSION* pSession, SERVICE* pService)
{
    return std::shared_ptr<mxs::FilterSession>(CommentFilterSession::create(pSession, pService, this));
}

json_t* CommentFilter::diagnostics() const
{
    return m_config.to_json();
}

uint64_t CommentFilter::getCapabilities() const
{
    return RCAP_TYPE_NONE;
}
