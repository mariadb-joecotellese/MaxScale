/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <deque>
#include <maxscale/backend.hh>
#include <maxscale/buffer.hh>
#include "comparatorbackend.hh"
#include "comparatorround.hh"
#include "comparatorrouter.hh"
#include "comparatorresult.hh"

class ComparatorRouter;

class ComparatorSession : public mxs::RouterSession
{
public:
    ComparatorSession(const ComparatorSession&) = delete;
    ComparatorSession& operator=(const ComparatorSession&) = delete;

    ComparatorSession(MXS_SESSION* pSession,
                      ComparatorRouter* pRouter,
                      SComparatorMainBackend sMain,
                      SComparatorOtherBackends backends);

    bool routeQuery(GWBUF&& packet) override;

    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    bool handleError(mxs::ErrorType type, const std::string& message,
                     mxs::Endpoint* pProblem, const mxs::Reply& reply) override final;

private:
    SComparatorMainBackend      m_sMain;
    SComparatorOtherBackends    m_others;
    int                         m_responses = 0;
    ComparatorRouter&           m_router;
    uint64_t                    m_num_queries = 0;
    std::deque<ComparatorRound> m_rounds;

    void check_if_round_is_ready();
    bool should_report(const ComparatorRound& round) const;
    void generate_report(const ComparatorRound& round);
    json_t* generate_report(const ComparatorBackend* pBackend, const ComparatorResult& result);
};
