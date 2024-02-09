/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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
#pragma once

#include <maxscale/ccdefs.hh>
#include <maxscale/filter.hh>
#include <string>

class CommentFilter;

class CommentFilterSession : public maxscale::FilterSession
{
    // Prevent copy-constructor and assignment operator usage
    CommentFilterSession(const CommentFilterSession&);
    CommentFilterSession& operator=(const CommentFilterSession&);

public:
    ~CommentFilterSession();

    // Create a new filter session
    static CommentFilterSession* create(MXS_SESSION* pSession,
                                        SERVICE* pService,
                                        const CommentFilter* pFilter);

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

private:

    // Used in the create function
    CommentFilterSession(MXS_SESSION* pSession, SERVICE* pService, const CommentFilter* pFilter);
    std::string parseComment(std::string comment);

    std::string m_inject;
};
