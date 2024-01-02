/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "comparatorsession.hh"
#include "comparatorresult.hh"
#include "comparatorrouter.hh"

#include <maxscale/protocol/mariadb/mysql.hh>

using namespace maxscale;

namespace
{

bool is_checksum_discrepancy(const ComparatorResult& result, const std::string& main_checksum)
{
    return result.checksum().hex() != main_checksum;
}

// TODO: milliseconds is too coarse.
bool is_execution_time_discrepancy(const ComparatorResult& result,
                                   const std::chrono::milliseconds& min,
                                   const std::chrono::milliseconds& max)
{
    auto duration = result.duration();

    return duration < min || duration > max;
}

}

ComparatorSession::ComparatorSession(MXS_SESSION* pSession,
                                     ComparatorRouter* pRouter,
                                     SComparatorMainBackend sMain,
                                     SComparatorOtherBackends others)
    : RouterSession(pSession)
    , m_sMain(std::move(sMain))
    , m_others(std::move(others))
    , m_router(*pRouter)
{
    for (auto& sOther : m_others)
    {
        sOther->set_result_handler(this);
    }
}

bool ComparatorSession::routeQuery(GWBUF&& packet)
{
    bool rv = false;

    if (m_sMain->in_use())
    {
        bool expecting_response = m_large_payload ? false : protocol_data().will_respond(packet);
        mxs::Backend::response_type type = expecting_response
            ? mxs::Backend::EXPECT_RESPONSE : mxs::Backend::NO_RESPONSE;

        std::shared_ptr<ComparatorMainResult> sMain_result;

        if (type != mxs::Backend::NO_RESPONSE)
        {
            sMain_result = m_sMain->prepare(get_sql_string(packet), mxs_mysql_get_command(packet));
        }

        if (m_sMain->write(packet.shallow_clone(), type))
        {
            if (type == mxs::Backend::EXPECT_RESPONSE)
            {
                type = mxs::Backend::IGNORE_RESPONSE;
            }

            for (const auto& sOther : m_others)
            {
                if (sOther->in_use())
                {
                    if (type != mxs::Backend::NO_RESPONSE)
                    {
                        mxb_assert(sMain_result);
                        sOther->prepare(sMain_result);
                    }

                    sOther->write(packet.shallow_clone(), type);
                }
            }

            m_large_payload = (packet.length() == MYSQL_PACKET_LENGTH_MAX);

            rv = true;
        }
    }

    return rv;
}

bool ComparatorSession::clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    auto* pBackend = static_cast<ComparatorBackend*>(down.endpoint()->get_userdata());

    pBackend->process_result(packet);

    if (reply.is_complete())
    {
        pBackend->finish_result(reply);
        pBackend->ack_write();

        MXB_INFO("Reply from '%s' complete.", pBackend->name());
    }

    bool rv = true;

    if (pBackend == m_sMain.get())
    {
        rv = RouterSession::clientReply(std::move(packet), down, reply);
    }

    return rv;
}

bool ComparatorSession::handleError(mxs::ErrorType type,
                                    const std::string& message,
                                    mxs::Endpoint* pProblem,
                                    const mxs::Reply& reply)
{
    auto* pBackend = static_cast<ComparatorBackend*>(pProblem->get_userdata());

    pBackend->close();

    // We can continue as long as the main connection isn't dead
    bool ok = m_router.config().on_error.get() == ErrorAction::ERRACT_IGNORE && pBackend != m_sMain.get();
    return ok || mxs::RouterSession::handleError(type, message, pProblem, reply);
}

void ComparatorSession::ready(const ComparatorOtherResult& other_result)
{
    if (should_report(other_result))
    {
        generate_report(other_result);
    }
}

bool ComparatorSession::should_report(const ComparatorOtherResult& other_result) const
{
    auto& config = m_router.config();
    auto report = config.report.get();

    bool rv = (report == ReportAction::REPORT_ALWAYS);

    if (!rv)
    {
        const auto& main_result = other_result.main_result();
        std::string main_checksum = main_result.checksum().hex();

        std::chrono::milliseconds main_duration = main_result.duration();
        std::chrono::milliseconds delta = (main_duration * config.max_execution_time_difference) / 100;
        std::chrono::milliseconds min_duration = main_duration - delta;
        std::chrono::milliseconds max_duration = main_duration + delta;

        if (is_checksum_discrepancy(other_result, main_checksum))
        {
            rv = true;
        }
        else if (is_execution_time_discrepancy(other_result, min_duration, max_duration))
        {
            rv = true;
        }
    }

    return rv;
}

void ComparatorSession::generate_report(const ComparatorOtherResult& other_result)
{
    const auto& main_result = other_result.main_result();

    json_t* pJson = json_object();
    json_object_set_new(pJson, "query", json_string(main_result.sql().c_str()));
    json_object_set_new(pJson, "command", json_string(mariadb::cmd_to_string(main_result.command())));
    json_object_set_new(pJson, "session", json_integer(m_pSession->id()));
    json_object_set_new(pJson, "query_id", json_integer(++m_num_queries));

    json_t* pArr = json_array();

    json_array_append_new(pArr, generate_json(main_result));
    json_array_append_new(pArr, generate_json(other_result));

    json_object_set_new(pJson, "results", pArr);

    m_router.ship(pJson);
}

json_t* ComparatorSession::generate_json(const ComparatorResult& result)
{
    const char* type = result.reply().error() ?
        "error" : (result.reply().is_resultset() ? "resultset" : "ok");

    json_t* pO = json_object();
    json_object_set_new(pO, "target", json_string(result.backend().name()));
    json_object_set_new(pO, "checksum", json_string(result.checksum().hex().c_str()));
    json_object_set_new(pO, "rows", json_integer(result.reply().rows_read()));
    json_object_set_new(pO, "warnings", json_integer(result.reply().num_warnings()));
    json_object_set_new(pO, "duration", json_integer(result.duration().count()));
    json_object_set_new(pO, "type", json_string(type));

    return pO;
}
