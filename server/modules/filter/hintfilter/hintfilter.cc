/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#define MXB_MODULE_NAME "hintfilter"

#include <stdio.h>
#include <maxscale/filter.hh>
#include <maxscale/modinfo.hh>
#include <maxscale/config2.hh>
#include "mysqlhint.hh"

/**
 * hintfilter.c - a filter to parse the MaxScale hint syntax and attach those
 * hints to the buffers that carry the requests.
 *
 */

mxs::config::Specification s_spec(MXB_MODULE_NAME, mxs::config::Specification::FILTER);

// static
HintInstance* HintInstance::create(const char* zName)
{
    return new HintInstance(zName);
}

HintInstance::HintInstance(const char* zName)
    : m_config(zName, &s_spec)
{
}

mxs::FilterSession* HintInstance::newSession(MXS_SESSION* pSession, SERVICE* pService)
{
    return new(std::nothrow) HintSession(pSession, pService);
}

json_t* HintInstance::diagnostics() const
{
    return nullptr;
}

uint64_t HintInstance::getCapabilities() const
{
    return RCAP_TYPE_STMT_INPUT;
}

mxs::config::Configuration& HintInstance::getConfiguration()
{
    return m_config;
}

HintSession::HintSession(MXS_SESSION* session, SERVICE* service)
    : mxs::FilterSession(session, service)
{
}

bool HintSession::routeQuery(GWBUF&& queue)
{
    for (auto hint : process_hints(queue))
    {
        queue.add_hint(std::move(hint));
    }

    return mxs::FilterSession::routeQuery(std::move(queue));
}

bool HintSession::clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    if (reply.is_complete() && m_current_id)
    {
        if (reply.error())
        {
            m_ps.erase(m_current_id);
            m_prev_id = 0;
        }

        m_current_id = 0;
    }

    return mxs::FilterSession::clientReply(std::move(packet), down, reply);
}

extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::FILTER,
        mxs::ModuleStatus::ALPHA,
        MXS_FILTER_VERSION,
        "A hint parsing filter",
        "V1.0.0",
        RCAP_TYPE_STMT_INPUT,
        &mxs::FilterApi<HintInstance>::s_api,
        NULL,       /* Process init. */
        NULL,       /* Process finish. */
        NULL,       /* Thread init. */
        NULL,       /* Thread finish. */
        &s_spec
    };

    return &info;
}
