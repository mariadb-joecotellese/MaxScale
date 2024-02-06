/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "croutersession.hh"
#include <maxscale/protocol/mariadb/mysql.hh>
#include "cresult.hh"
#include "crouter.hh"

using namespace maxscale;
using mariadb::QueryClassifier;
using std::unique_ptr;

namespace
{

inline bool is_checksum_discrepancy(const CResult& result, mxb::CRC32 main_checksum)
{
    return result.checksum() != main_checksum;
}

inline bool is_execution_time_discrepancy(const std::chrono::nanoseconds& duration,
                                          const std::chrono::nanoseconds& min,
                                          const std::chrono::nanoseconds& max)
{
    return duration < min || duration > max;
}

}

CRouterSession::CRouterSession(MXS_SESSION* pSession,
                               CRouter* pRouter,
                               SCMainBackend sMain,
                               SCOtherBackends others)
    : RouterSession(pSession)
    , m_sMain(std::move(sMain))
    , m_others(std::move(others))
    , m_router(*pRouter)
{
    m_sMain->set_router_session(this);

    for (auto& sOther : m_others)
    {
        sOther->set_router_session(this);
        sOther->set_result_handler(this);
    }
}

CRouterSession::~CRouterSession()
{
    Stats stats { m_sMain->backend()->target(), m_sMain->stats() };

    for (auto& sOther : m_others)
    {
        stats.other_stats.insert(std::make_pair(sOther->target(), sOther->stats()));
    }

    m_router.collect(stats);
}

bool CRouterSession::routeQuery(GWBUF&& packet)
{
    bool rv = false;

    if (m_sMain->in_use())
    {
        bool expecting_response = m_sMain->extraordinary_in_process()
            ? false : protocol_data().will_respond(packet);
        mxs::Backend::response_type type = expecting_response
            ? mxs::Backend::EXPECT_RESPONSE : mxs::Backend::NO_RESPONSE;

        std::shared_ptr<CMainResult> sMain_result;

        if (type != mxs::Backend::NO_RESPONSE)
        {
            sMain_result = m_sMain->prepare(packet);
        }

        auto nMain_backlog = m_sMain->nBacklog();

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
                    bool write_to_other = true;

                    if (!sOther->extraordinary_in_process())
                    {
                        // Nothing funky in process.

                        auto nOther_backlog = sOther->nBacklog();

                        if (nMain_backlog - nOther_backlog > m_router.config().max_request_lag)
                        {
                            auto qi = parser().helper().get_query_info(packet);

                            using P = mxs::Parser;
                            const auto W = mxs::sql::TYPE_WRITE;

                            if (qi.op == sql::OpCode::OP_SELECT            // A SELECT,
                                && qi.query                                // a regular one (not a PS),
                                && !P::type_mask_contains(qi.type_mask, W) // not FOR UPDATE, and
                                && !qi.multi_part_packet)                  // not multi part.
                            {
                                // Ok, so a vanilla SELECT. Let's skip due to the lag.
                                sOther->bump_requests_skipped();
                                write_to_other = false;
                            }
                        }
                    }

                    if (write_to_other)
                    {
                        if (type != mxs::Backend::NO_RESPONSE)
                        {
                            mxb_assert(sMain_result);
                            sOther->prepare(sMain_result);
                        }

                        sOther->write(packet.shallow_clone(), type);
                    }
                }
            }

            rv = true;
        }
    }

    return rv;
}

bool CRouterSession::clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    auto* pBackend = static_cast<CBackend*>(down.endpoint()->get_userdata());

    pBackend->process_result(packet, reply);

    CBackend::Routing routing = CBackend::Routing::CONTINUE;

    if (reply.is_complete())
    {
        routing = pBackend->finish_result(reply);
        pBackend->ack_write();
    }

    bool rv = true;

    if (pBackend == m_sMain.get() && routing == CBackend::Routing::CONTINUE)
    {
        rv = RouterSession::clientReply(std::move(packet), down, reply);
    }

    return rv;
}

bool CRouterSession::handleError(mxs::ErrorType type,
                                 const std::string& message,
                                 mxs::Endpoint* pProblem,
                                 const mxs::Reply& reply)
{
    auto* pBackend = static_cast<CBackend*>(pProblem->get_userdata());

    pBackend->close();

    // We can continue as long as the main connection isn't dead
    bool ok = m_router.config().on_error.get() == OnError::IGNORE && pBackend != m_sMain.get();
    return ok || mxs::RouterSession::handleError(type, message, pProblem, reply);
}

Explain CRouterSession::ready(COtherResult& other_result)
{
    Explain rv = Explain::NONE;

    if (should_report(other_result))
    {
        auto now = m_pSession->worker()->epoll_tick_now();
        auto hash = other_result.hash();
        auto id = other_result.id();
        CRegistry::Entries explainers;

        auto entries = m_router.config().entries;

        if (entries == 0 || !m_router.registry().is_explained(now, hash, id, &explainers))
        {
            auto explain = m_router.config().explain;

            if (other_result.is_explainable() && explain != Explain::NONE)
            {
                other_result.set_explainers(explainers);
                rv = explain;
            }
            else
            {
                generate_report(other_result);
            }
        }
    }

    return rv;
}

void CRouterSession::ready(const CExplainOtherResult& explain_result)
{
    const auto& error = explain_result.error();

    if (!error.empty())
    {
        auto& main_result = explain_result.other_result().main_result();

        auto sql = main_result.sql();
        MXB_WARNING("EXPLAIN of '%.*s' failed: %s", (int)sql.length(), sql.data(), error.c_str());

        generate_report(explain_result.other_result());
    }
    else
    {
        generate_report(explain_result);
    }
}

bool CRouterSession::should_report(const COtherResult& other_result) const
{
    const auto& config = m_router.config();

    bool rv = (config.report.get() == Report::ALWAYS);

    if (!rv)
    {
        const auto& main_result = other_result.main_result();
        std::chrono::nanoseconds main_duration = main_result.duration();
        std::chrono::nanoseconds delta = (main_duration * config.max_execution_time_difference) / 100;
        std::chrono::nanoseconds other_duration = other_result.duration();

        if (is_checksum_discrepancy(other_result, main_result.checksum()))
        {
            rv = true;
        }
        else
        {
            std::chrono::nanoseconds min_duration = main_duration - delta;
            std::chrono::nanoseconds max_duration = main_duration + delta;

            if (is_execution_time_discrepancy(other_duration, min_duration, max_duration))
            {
                rv = true;
            }
        }
    }

    return rv;
}

void CRouterSession::generate_report(const COtherResult& other_result)
{
    generate_report(other_result, nullptr, nullptr);
}

namespace
{

json_t* load_json(std::string_view json)
{
    json_error_t error;
    json_t* pJson = json_loadb(json.data(), json.length(), 0, &error);

    if (!pJson)
    {
        MXB_WARNING("Could not parse EXPLAIN result '%.*s' returned by server, storing as string: %s",
                    (int)json.length(), json.data(), error.text);

        pJson = json_stringn(json.data(), json.length());
    }

    return pJson;
}

}

void CRouterSession::generate_report(const CExplainOtherResult& result)
{
    std::string_view json;

    json_t* pExplain_other = nullptr;
    json = result.json();

    if (!json.empty())
    {
        pExplain_other = load_json(json);
    }

    json_t* pExplain_main = nullptr;
    const CExplainMainResult* pMain_result = result.explain_main_result();

    if (pMain_result)
    {
        json = pMain_result->json();

        if (!json.empty())
        {
            pExplain_main = load_json(json);
        }
    }

    generate_report(result.other_result(), pExplain_other, pExplain_main);
}

void CRouterSession::generate_report(const COtherResult& other_result,
                                     json_t* pExplain_other,
                                     json_t* pExplain_main)
{
    const auto& main_result = other_result.main_result();

    json_t* pJson = json_object();
    auto sql = main_result.sql();
    json_object_set_new(pJson, "id", json_integer(main_result.id()));
    json_object_set_new(pJson, "session", json_integer(m_pSession->id()));
    json_object_set_new(pJson, "command", json_string(mariadb::cmd_to_string(main_result.command())));
    json_object_set_new(pJson, "query", json_stringn(sql.data(), sql.length()));

    json_t* pOther = generate_json(other_result, pExplain_other);
    json_t* pMain = generate_json(main_result, pExplain_main);

    const CRegistry::Entries& explainers = other_result.explainers();

    if (!explainers.empty())
    {
        json_t* pExplained_by = json_array();

        for (const auto& explainer : explainers)
        {
            json_array_append_new(pExplained_by, json_integer(explainer.id));
        }

        json_object_set_new(pOther, "explained_by", pExplained_by);
    }

    json_t* pArr = json_array();
    json_array_append_new(pArr, pMain);
    json_array_append_new(pArr, pOther);

    json_object_set_new(pJson, "results", pArr);

    static_cast<COtherBackend&>(other_result.backend()).exporter().ship(pJson);
}

json_t* CRouterSession::generate_json(const CResult& result, json_t* pExplain)
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

    if (pExplain)
    {
        json_object_set_new(pO, "explain", pExplain);
    }

    return pO;
}
