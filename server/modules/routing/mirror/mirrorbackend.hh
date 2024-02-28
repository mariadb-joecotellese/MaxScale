/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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

#include "common.hh"

#include <vector>
#include <memory>

#include <maxscale/backend.hh>
#include <maxscale/router.hh>
#include <maxbase/checksum.hh>

class MyBackend;
using SMyBackends = std::vector<std::unique_ptr<MyBackend>>;

using Clock = std::chrono::steady_clock;

class MyBackend : public mxs::Backend
{
public:
    using mxs::Backend::Backend;

    static SMyBackends from_endpoints(const mxs::Endpoints& endpoints);

    bool write(GWBUF&& buffer, response_type type = EXPECT_RESPONSE) override;

    void process_result(const GWBUF& buffer, const mxs::Reply& reply);

    const mxb::CRC32& checksum() const
    {
        return m_checksum;
    }

    // Query duration in milliseconds
    uint64_t duration() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(m_end - m_start).count();
    }

    const mxs::Reply& reply() const
    {
        return m_reply;
    }

private:
    Clock::time_point m_start;
    Clock::time_point m_end;
    mxb::CRC32        m_checksum;
    mxs::Reply        m_reply;
};
