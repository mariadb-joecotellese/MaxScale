/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "uratsession.hh"
#include "uratresult.hh"
#include "uratrouter.hh"

#include <maxscale/protocol/mariadb/mysql.hh>

using namespace maxscale;

UratSession::UratSession(MXS_SESSION* pSession, UratRouter* pRouter, SUratBackends backends)
    : RouterSession(pSession)
    , m_backends(std::move(backends))
    , m_router(*pRouter)
{
    for (const auto& sBackend : m_backends)
    {
        if (sBackend->target() == m_router.get_main())
        {
            m_pMain = sBackend.get();
        }
    }
}

bool UratSession::routeQuery(GWBUF&& packet)
{
    int rc = 0;

    if (m_responses)
    {
        m_queue.push_back(std::move(packet));
        rc = 1;
    }
    else
    {
        m_query = get_sql_string(packet);
        m_command = mxs_mysql_get_command(packet);
        bool expecting_response = protocol_data().will_respond(packet);

        for (const auto& sBackend : m_backends)
        {
            auto type = mxs::Backend::NO_RESPONSE;

            if (expecting_response)
            {
                type = sBackend.get() == m_pMain
                    ? mxs::Backend::EXPECT_RESPONSE : mxs::Backend::IGNORE_RESPONSE;
            }

            if (sBackend->in_use() && sBackend->write(packet.shallow_clone(), type))
            {
                if (sBackend.get() == m_pMain)
                {
                    // Routing is successful as long as we can write to the main connection
                    rc = 1;
                }

                if (expecting_response)
                {
                    ++m_responses;
                }
            }
        }
    }

    return rc;
}

void UratSession::route_queued_queries()
{
    while (!m_queue.empty() && m_responses == 0)
    {
        MXB_INFO(">>> Routing queued queries");
        auto query = std::move(m_queue.front());
        m_queue.pop_front();

        MXB_AT_DEBUG(std::string query_sql = get_sql_string(query));

        if (!routeQuery(std::move(query)))
        {
            break;
        }

        MXB_INFO("<<< Queued queries routed");

        // Routing of queued queries should never cause the same query to be put back into the queue. The
        // check for m_responses should prevent it.
        mxb_assert(m_queue.empty() || get_sql(m_queue.back()) != query_sql);
    }
}

void UratSession::finalize_reply()
{
    // All replies have now arrived. Return the last chunk of the result to the client
    // that we've been storing in the session.
    MXB_INFO("All replies received, routing last chunk to the client.");

    RouterSession::clientReply(std::move(m_last_chunk), m_last_route, m_pMain->reply());
    m_last_chunk.clear();

    generate_report();
    m_results.clear();
    route_queued_queries();
}

bool UratSession::clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    auto* pBackend = static_cast<UratBackend*>(down.endpoint()->get_userdata());

    if (!reply.is_complete())
    {
        pBackend->process_result(packet, reply);
    }
    else
    {
        UratResult result = pBackend->finish_result(packet, reply);
        m_results.emplace_back(result);
        pBackend->ack_write();
        --m_responses;

        MXB_INFO("Reply from '%s' complete%s.", pBackend->name(), pBackend == m_pMain ?
                 ", delaying routing of last chunk until all replies have been received" : "");

        if (pBackend == m_pMain)
        {
            m_last_chunk = std::move(packet);
            m_last_route = down;
            packet.clear();
        }

        if (m_responses == 0)
        {
            mxb_assert(!m_last_chunk.empty());
            mxb_assert(!packet || pBackend != m_pMain);

            packet.clear();
            finalize_reply();
        }
    }

    bool rc = true;

    if (packet && pBackend == m_pMain)
    {
        rc = RouterSession::clientReply(std::move(packet), down, reply);
    }

    return rc;
}

bool UratSession::handleError(mxs::ErrorType type,
                              const std::string& message,
                              mxs::Endpoint* pProblem,
                              const mxs::Reply& reply)
{
    auto* pBackend = static_cast<Backend*>(pProblem->get_userdata());

    if (pBackend->is_waiting_result())
    {
        --m_responses;

        if (m_responses == 0 && pBackend != m_pMain)
        {
            finalize_reply();
        }
    }

    pBackend->close();

    // We can continue as long as the main connection isn't dead
    bool ok = m_router.config().on_error.get() == ErrorAction::ERRACT_IGNORE && pBackend != m_pMain;
    return ok || mxs::RouterSession::handleError(type, message, pProblem, reply);
}

bool UratSession::should_report() const
{
    bool rval = true;

    if (m_router.config().report.get() == ReportAction::REPORT_ON_CONFLICT)
    {
        rval = false;
        std::string checksum;

        for (const auto& sBackend : m_backends)
        {
            if (sBackend->in_use())
            {
                if (checksum.empty())
                {
                    checksum = sBackend->checksum().hex();
                }
                else if (checksum != sBackend->checksum().hex())
                {
                    rval = true;
                }
            }
        }
    }

    return rval;
}

void UratSession::generate_report()
{
    if (should_report())
    {
        json_t* pJson = json_object();
        json_object_set_new(pJson, "query", json_string(m_query.c_str()));
        json_object_set_new(pJson, "command", json_string(mariadb::cmd_to_string(m_command)));
        json_object_set_new(pJson, "session", json_integer(m_pSession->id()));
        json_object_set_new(pJson, "query_id", json_integer(++m_num_queries));

        json_t* pArr = json_array();

        for (const auto& sBackend : m_backends)
        {
            if (sBackend->in_use())
            {
                const char* type = sBackend->reply().error() ?
                    "error" : (sBackend->reply().is_resultset() ? "resultset" : "ok");

                json_t* pO = json_object();
                json_object_set_new(pO, "target", json_string(sBackend->name()));
                json_object_set_new(pO, "checksum", json_string(sBackend->checksum().hex().c_str()));
                json_object_set_new(pO, "rows", json_integer(sBackend->reply().rows_read()));
                json_object_set_new(pO, "warnings", json_integer(sBackend->reply().num_warnings()));
                json_object_set_new(pO, "duration", json_integer(sBackend->duration()));
                json_object_set_new(pO, "type", json_string(type));

                json_array_append_new(pArr, pO);
            }
        }

        json_object_set_new(pJson, "results", pArr);

        m_router.ship(pJson);
    }
}
