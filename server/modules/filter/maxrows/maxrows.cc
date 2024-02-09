/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

#include "maxrows.hh"
#include <maxscale/protocol/mariadb/mysql.hh>

namespace
{

namespace maxrows
{

namespace config = mxs::config;

config::Specification specification(MXB_MODULE_NAME, config::Specification::FILTER);

config::ParamCount max_resultset_rows(
    &specification,
    "max_resultset_rows",
    "Specifies the maximum number of rows a resultset can have in order to be returned to the user.",
    std::numeric_limits<uint32_t>::max(),
    config::Param::AT_RUNTIME);

config::ParamSize max_resultset_size(
    &specification,
    "max_resultset_size",
    "Specifies the maximum size a resultset can have in order to be sent to the client.",
    65536,
    config::Param::AT_RUNTIME);

config::ParamInteger debug(
    &specification,
    "debug",
    "An integer value, using which the level of debug logging made by the Maxrows "
    "filter can be controlled.",
    0,
    0,
    3,
    config::Param::AT_RUNTIME);

config::ParamEnum<MaxRowsConfig::Mode> max_resultset_return(
    &specification,
    "max_resultset_return",
    "Specifies what the filter sends to the client when the rows or size limit "
    "is hit; an empty packet, an error packet or an ok packet.",
        {
            {MaxRowsConfig::Mode::EMPTY, "empty"},
            {MaxRowsConfig::Mode::ERR, "error"},
            {MaxRowsConfig::Mode::OK, "ok"}
        },
    MaxRowsConfig::Mode::EMPTY,
    config::Param::AT_RUNTIME);
}

void truncate_packets(GWBUF& buffer, uint64_t packets)
{
    // See: https://mariadb.com/kb/en/library/eof_packet/
    std::array<uint8_t, 9> eof {0x5, 0x0, 0x0, (uint8_t)(packets + 1), 0xfe, 0x0, 0x0, 0x0, 0x0};
    uint8_t* it = buffer.begin();

    while (it < buffer.end() && packets-- > 0)
    {
        it += mariadb::get_header(it).pl_length + MYSQL_HEADER_LEN;
    }

    buffer.rtrim(std::distance(it, buffer.end()));
    buffer.append(eof.data(), eof.size());
}
}

MaxRowsConfig::MaxRowsConfig(const char* zName)
    : mxs::config::Configuration(zName, &maxrows::specification)
    , max_rows(this, &maxrows::max_resultset_rows)
    , max_size(this, &maxrows::max_resultset_size)
    , debug(this, &maxrows::debug)
    , mode(this, &maxrows::max_resultset_return)
{
}

MaxRowsSession::MaxRowsSession(MXS_SESSION* pSession, SERVICE* pService, MaxRows* pFilter)
    : FilterSession(pSession, pService)
    , m_max_rows(pFilter->config().max_rows.get())
    , m_max_size(pFilter->config().max_size.get())
    , m_debug(pFilter->config().debug.get())
    , m_mode(pFilter->config().mode.get())
{
}

bool MaxRowsSession::routeQuery(GWBUF&& packet)
{
    return FilterSession::routeQuery(std::move(packet));
}

bool MaxRowsSession::clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    int rv = 1;

    if (m_collect)
    {
        // The resultset is stored in an internal buffer until we know whether to send it or to discard it
        m_buffer.append(buffer);

        if (reply.rows_read() > m_max_rows || reply.size() > m_max_size)
        {
            // A limit was exceeded, discard the result and replace it with a fake result
            switch (m_mode)
            {
            case MaxRowsConfig::Mode::EMPTY:
                if (reply.rows_read() > 0)
                {
                    // We have the start of the resultset with at least one row in it. Truncate the result
                    // to contain the start of the first resultset with no rows and inject an EOF packet into
                    // it.
                    uint64_t num_packets = reply.field_counts()[0] + 2;
                    truncate_packets(m_buffer, num_packets);
                    m_collect = false;
                }
                break;

            case MaxRowsConfig::Mode::ERR:
                m_buffer = mariadb::create_error_packet(1, 1226, "42000",
                                                        reply.rows_read() > m_max_rows ?
                                                        "Resultset row limit exceeded" :
                                                        "Resultset size limit exceeded");
                m_collect = false;
                break;

            case MaxRowsConfig::Mode::OK:
                m_buffer = mariadb::create_ok_packet();
                m_collect = false;
                break;

            default:
                mxb_assert(!true);
                rv = 0;
                break;
            }
        }
    }

    if (reply.is_complete())
    {
        rv = FilterSession::clientReply(std::move(m_buffer), down, reply);
        m_buffer.clear();
        m_collect = true;
    }

    return rv;
}

// static
MaxRows* MaxRows::create(const char* name)
{
    return new MaxRows(name);
}

extern "C"
{
MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::FILTER,
        mxs::ModuleStatus::IN_DEVELOPMENT,
        MXS_FILTER_VERSION,
        "A filter that limits resultsets.",
        "V1.0.0",
        MaxRows::CAPABILITIES,
        &mxs::FilterApi<MaxRows>::s_api,
        nullptr,        /* Process init. */
        nullptr,        /* Process finish. */
        nullptr,        /* Thread init. */
        nullptr,        /* Thread finish. */
        &maxrows::specification
    };

    return &info;
}
}
