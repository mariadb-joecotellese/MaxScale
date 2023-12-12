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

UratSession::UratSession(MXS_SESSION* pSession,
                         UratRouter* pRouter,
                         SUratMainBackend sMain,
                         SUratOtherBackends others)
    : RouterSession(pSession)
    , m_sMain(std::move(sMain))
    , m_others(std::move(others))
    , m_router(*pRouter)
{
}

bool UratSession::routeQuery(GWBUF&& packet)
{
    bool expecting_response = protocol_data().will_respond(packet);
    mxs::Backend::response_type type = expecting_response
        ? mxs::Backend::EXPECT_RESPONSE : mxs::Backend::NO_RESPONSE;

    bool rv = false;

    if (m_sMain->in_use() && m_sMain->write(packet.shallow_clone(), type))
    {
        auto* pMain = m_sMain.get();
        m_rounds.emplace_back(get_sql_string(packet), mxs_mysql_get_command(packet), pMain);

        UratRound& round = m_rounds.back();

        if (type == mxs::Backend::EXPECT_RESPONSE)
        {
            type = mxs::Backend::IGNORE_RESPONSE;
        }

        for (const auto& sOther : m_others)
        {
            if (sOther->in_use() && sOther->write(packet.shallow_clone(), type))
            {
                round.add_backend(sOther.get());
            }
        }

        rv = true;
    }

    return rv;
}

bool UratSession::clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    auto* pBackend = static_cast<UratBackend*>(down.endpoint()->get_userdata());

    pBackend->process_result(packet);

    if (reply.is_complete())
    {
        UratResult result = pBackend->finish_result(reply);

        int32_t nBacklog = pBackend->nBacklog();
        mxb_assert(nBacklog < (int)m_rounds.size());

        int32_t index = m_rounds.size() - 1 - nBacklog;
        mxb_assert(index >= 0 && index < (int)m_rounds.size());

        UratRound& round = m_rounds[index];

        round.set_result(pBackend, result);
        pBackend->ack_write();

        MXB_INFO("Reply from '%s' complete.", pBackend->name());
    }

    bool rv = true;

    if (pBackend == m_sMain.get())
    {
        rv = RouterSession::clientReply(std::move(packet), down, reply);
    }

    if (reply.is_complete())
    {
        // Here, and not inside the first block so that we send the data
        // to client first and only then worry about statistics.
        check_if_round_is_ready();
    }

    return rv;
}

bool UratSession::handleError(mxs::ErrorType type,
                              const std::string& message,
                              mxs::Endpoint* pProblem,
                              const mxs::Reply& reply)
{
    auto* pBackend = static_cast<UratBackend*>(pProblem->get_userdata());

    for (UratRound& round : m_rounds)
    {
        round.remove_backend(pBackend);
    }

    pBackend->close();

    check_if_round_is_ready();

    // We can continue as long as the main connection isn't dead
    bool ok = m_router.config().on_error.get() == ErrorAction::ERRACT_IGNORE && pBackend != m_sMain.get();
    return ok || mxs::RouterSession::handleError(type, message, pProblem, reply);
}

void UratSession::check_if_round_is_ready()
{
    // Rounds will become ready from the front and in order. If the
    // first round is not ready, then any subsequent one cannot be.

    while (!m_rounds.empty() && m_rounds.front().ready())
    {
        const UratRound& round = m_rounds.front();

        if (should_report(round))
        {
            generate_report(round);
        }

        m_rounds.pop_front();
    }
}

bool UratSession::should_report(const UratRound& round) const
{
    auto report = m_router.config().report.get();

    bool rv = (report == ReportAction::REPORT_ALWAYS);

    if (!rv)
    {
        if (m_sMain->in_use())
        {
            std::string checksum;
            for (const auto& kv : round.results())
            {
                const UratResult& result = kv.second;

                if (checksum.empty())
                {
                    checksum = result.checksum().hex();
                }
                else if (checksum != result.checksum().hex())
                {
                    rv = true;
                    break;
                }
            }
        }
    }

    return rv;
}

void UratSession::generate_report(const UratRound& round)
{
    json_t* pJson = json_object();
    json_object_set_new(pJson, "query", json_string(round.query().c_str()));
    json_object_set_new(pJson, "command", json_string(mariadb::cmd_to_string(round.command())));
    json_object_set_new(pJson, "session", json_integer(m_pSession->id()));
    json_object_set_new(pJson, "query_id", json_integer(++m_num_queries));

    json_t* pArr = json_array();

    for (const auto& kv : round.results())
    {
        const UratBackend* pBackend = kv.first;
        const UratResult& result = kv.second;

        json_array_append_new(pArr, generate_report(pBackend, result));
    }

    json_object_set_new(pJson, "results", pArr);

    m_router.ship(pJson);
}

json_t* UratSession::generate_report(const UratBackend* pBackend, const UratResult& result)
{
    const char* type = result.reply().error() ?
        "error" : (result.reply().is_resultset() ? "resultset" : "ok");

    json_t* pO = json_object();
    json_object_set_new(pO, "target", json_string(pBackend->name()));
    json_object_set_new(pO, "checksum", json_string(result.checksum().hex().c_str()));
    json_object_set_new(pO, "rows", json_integer(result.reply().rows_read()));
    json_object_set_new(pO, "warnings", json_integer(result.reply().num_warnings()));
    json_object_set_new(pO, "duration", json_integer(result.duration().count()));
    json_object_set_new(pO, "type", json_string(type));

    return pO;
}
