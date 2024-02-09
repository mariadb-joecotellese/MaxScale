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

#include "hintrouterdefs.hh"

#include <deque>
#include <unordered_map>
#include <vector>
#include <string>

#include <maxscale/router.hh>

using std::string;

class HintRouter;

class HintRouterSession : public maxscale::RouterSession
{
public:
    using BackendMap = std::unordered_map<string, mxs::Endpoint*>;      // All backends, indexed by name
    using BackendArray = std::vector<mxs::Endpoint*>;
    using MapElement = BackendMap::value_type;
    using size_type = BackendArray::size_type;

    HintRouterSession(MXS_SESSION* pSession,
                      HintRouter* pRouter,
                      const BackendMap& backends);

    ~HintRouterSession();

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

private:
    HintRouterSession(const HintRouterSession&);            // denied
    HintRouterSession& operator=(const HintRouterSession&); // denied
private:
    bool route_by_hint(const GWBUF& packet, const Hint& current_hint, bool ignore_errors);
    bool route_to_slave(GWBUF&& packet, bool print_errors);
    void update_connections();

    HintRouter*    m_router;
    BackendMap     m_backends;          // all connections
    mxs::Endpoint* m_master;            // connection to master
    BackendArray   m_slaves;            // connections to slaves
    size_type      m_n_routed_to_slave; // packets routed to a single slave, used for rr
    size_type      m_surplus_replies;   // how many replies should be ignored
};
