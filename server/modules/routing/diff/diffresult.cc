/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffresult.hh"
#include <vector>
#include "diffbackend.hh"
#include "diffregistry.hh"

namespace
{

struct ThisUnit
{
    std::atomic<int64_t> id { 1 };
} this_unit;

}

/**
 * CResult
 */
CResult::~CResult()
{
}

void CResult::process(const GWBUF& buffer)
{
    mxb_assert(!closed());
    m_checksum.update(buffer);
}

std::chrono::nanoseconds CResult::close(const mxs::Reply& reply)
{
    mxb_assert(!closed());
    m_reply = reply;
    m_end = Clock::now();

    return duration();
}


/**
 * CMainResult
 */
CMainResult::CMainResult(CMainBackend* pBackend, const GWBUF& packet)
    : CResult(pBackend)
    , m_id(this_unit.id++)
    , m_packet(packet.shallow_clone())
{
}

CMainResult::~CMainResult()
{
}

std::string_view CMainResult::sql() const
{
    if (m_sql.empty())
    {
        m_sql = backend().phelper().get_sql(m_packet);
    }

    return m_sql;
}

uint8_t CMainResult::command() const
{
    if (m_command == 0)
    {
        m_command = backend().phelper().get_command(m_packet);
    }

    return m_command;
}

std::string_view CMainResult::canonical() const
{
    if (m_canonical.empty())
    {
        m_canonical = backend().parser().get_canonical(m_packet);
    }

    return m_canonical;
}

CResult::Hash CMainResult::hash() const
{
    if (m_hash == 0)
    {
        m_hash = CRegistry::hash_for(canonical());
    }

    return m_hash;
}

std::chrono::nanoseconds CMainResult::close(const mxs::Reply& reply)
{
    auto rv = CResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (std::shared_ptr<COtherResult> sDependent : dependents)
    {
        sDependent->main_was_closed();
    }

    return rv;
}

/**
 * COtherResult
 */

COtherResult::COtherResult(COtherBackend* pBackend,
                           Handler* pHandler,
                           std::shared_ptr<CMainResult> sMain_result)
    : CResult(pBackend)
    , m_handler(*pHandler)
    , m_sMain_result(sMain_result)
{
}

COtherResult::~COtherResult()
{
}

std::chrono::nanoseconds COtherResult::close(const mxs::Reply& reply)
{
    auto rv = CResult::close(reply);

    if (m_sMain_result->closed())
    {
        m_handler.ready(*this);
        m_sMain_result->remove_dependent(shared_from_this());
    }

    return rv;
}

void COtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
        m_sMain_result->remove_dependent(shared_from_this());
    }
}


/**
 * CExplainResult
 */

std::chrono::nanoseconds CExplainResult::close(const mxs::Reply& reply)
{
    CResult::close(reply);

    mxb_assert(reply.is_complete());

    if (!reply.row_data().empty())
    {
        mxb_assert(reply.row_data().size() == 1);
        mxb_assert(reply.row_data().front().size() == 1);

        m_json = reply.row_data().front().front();
    }

    // Return 0, so that the duration of the EXPLAIN request is not
    // included in the total duration.
    return std::chrono::milliseconds { 0 };
}

/**
 * CExplainMainResult
 */
CExplainMainResult::CExplainMainResult(CMainBackend* pBackend,
                                       std::shared_ptr<CMainResult> sMain_result)
    : CExplainResult(pBackend)
    , m_sMain_result(sMain_result)
{
    mxb_assert(m_sMain_result);
}

CExplainMainResult::~CExplainMainResult()
{
}

std::chrono::nanoseconds CExplainMainResult::close(const mxs::Reply& reply)
{
    auto rv = CExplainResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (std::shared_ptr<CExplainOtherResult> sDependent : dependents)
    {
        sDependent->main_was_closed();
    }

    static_cast<CMainBackend&>(backend()).ready(*this);

    return rv;
}

/**
 * CExplainOtherResult
 */
CExplainOtherResult::CExplainOtherResult(Handler* pHandler,
                                         std::shared_ptr<const COtherResult> sOther_result,
                                         std::shared_ptr<CExplainMainResult> sExplain_main_result)
    : CExplainResult(&sOther_result->backend())
    , m_handler(*pHandler)
    , m_sOther_result(sOther_result)
    , m_sExplain_main_result(sExplain_main_result)
{
    mxb_assert(m_sOther_result);
}

CExplainOtherResult::~CExplainOtherResult()
{
}

std::chrono::nanoseconds CExplainOtherResult::close(const mxs::Reply& reply)
{
    auto rv = CExplainResult::close(reply);

    if (!m_sExplain_main_result || m_sExplain_main_result->closed())
    {
        m_handler.ready(*this);

        if (m_sExplain_main_result)
        {
            m_sExplain_main_result->remove_dependent(shared_from_this());
        }
    }

    return rv;
}

void CExplainOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
        m_sExplain_main_result->remove_dependent(shared_from_this());
    }
}
