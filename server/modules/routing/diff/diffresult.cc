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
 * DiffResult
 */
DiffResult::DiffResult(DiffBackend* pBackend)
    : m_backend(*pBackend)
    , m_parser(pBackend->parser())
    , m_start(Clock::now())
    , m_end(Clock::time_point::max())
{
    mxb_assert(pBackend);
}

DiffResult::~DiffResult()
{
}

void DiffResult::process(const GWBUF& buffer)
{
    mxb_assert(!closed());
    m_checksum.update(buffer);
}

std::chrono::nanoseconds DiffResult::close(const mxs::Reply& reply)
{
    mxb_assert(!closed());
    m_reply = reply;
    m_end = Clock::now();

    return duration();
}


/**
 * DiffMainResult
 */
DiffMainResult::DiffMainResult(DiffMainBackend* pBackend)
    : DiffResult(pBackend)
{
}


/**
 * DiffOrdinaryMainResult
 */
DiffOrdinaryMainResult::DiffOrdinaryMainResult(DiffMainBackend* pBackend, const GWBUF& packet)
    : DiffMainResult(pBackend)
    , m_id(this_unit.id++)
    , m_packet(packet.shallow_clone())
{
}

DiffOrdinaryMainResult::~DiffOrdinaryMainResult()
{
}

std::string_view DiffOrdinaryMainResult::sql() const
{
    if (m_sql.empty())
    {
        m_sql = parser().helper().get_sql(m_packet);
    }

    return m_sql;
}

uint8_t DiffOrdinaryMainResult::command() const
{
    if (m_command == 0)
    {
        m_command = parser().helper().get_command(m_packet);
    }

    return m_command;
}

std::string_view DiffOrdinaryMainResult::canonical() const
{
    if (m_canonical.empty())
    {
        m_canonical = parser().get_canonical(m_packet);
    }

    return m_canonical;
}

DiffResult::Hash DiffOrdinaryMainResult::canonical_hash() const
{
    if (m_canonical_hash == 0)
    {
        m_canonical_hash = DiffRegistry::hash_for(canonical());
    }

    return m_canonical_hash;
}

std::chrono::nanoseconds DiffOrdinaryMainResult::close(const mxs::Reply& reply)
{
    auto rv = DiffResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (std::shared_ptr<DiffOrdinaryOtherResult> sDependent : dependents)
    {
        sDependent->main_was_closed();
    }

    return rv;
}


/**
 * DiffOtherResult
 */
DiffOtherResult::DiffOtherResult(DiffOtherBackend* pBackend)
    : DiffResult(pBackend)
{
}



/**
 * DiffOrdinaryOtherResult
 */

DiffOrdinaryOtherResult::DiffOrdinaryOtherResult(DiffOtherBackend* pBackend,
                                                 Handler* pHandler,
                                                 std::shared_ptr<DiffOrdinaryMainResult> sMain_result)
    : DiffOtherResult(pBackend)
    , m_handler(*pHandler)
    , m_sMain_result(sMain_result)
{
}

DiffOrdinaryOtherResult::~DiffOrdinaryOtherResult()
{
}

std::chrono::nanoseconds DiffOrdinaryOtherResult::close(const mxs::Reply& reply)
{
    auto rv = DiffResult::close(reply);

    if (m_sMain_result->closed())
    {
        m_handler.ready(*this);
        deregister_from_main();
    }

    return rv;
}

void DiffOrdinaryOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
        deregister_from_main();
    }
}


/**
 * DiffExplainResult
 */

std::chrono::nanoseconds DiffExplainResult::close(const mxs::Reply& reply)
{
    DiffResult::close(reply);

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
 * DiffExplainMainResult
 */
DiffExplainMainResult::DiffExplainMainResult(DiffMainBackend* pBackend,
                                             std::shared_ptr<DiffOrdinaryMainResult> sMain_result)
    : DiffExplainResult(pBackend)
    , m_sMain_result(sMain_result)
{
    mxb_assert(m_sMain_result);
}

DiffExplainMainResult::~DiffExplainMainResult()
{
}

std::chrono::nanoseconds DiffExplainMainResult::close(const mxs::Reply& reply)
{
    auto rv = DiffExplainResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (std::shared_ptr<DiffExplainOtherResult> sDependent : dependents)
    {
        sDependent->main_was_closed();
    }

    static_cast<DiffMainBackend&>(backend()).ready(*this);

    return rv;
}

/**
 * DiffExplainOtherResult
 */
DiffExplainOtherResult::DiffExplainOtherResult(Handler* pHandler,
                                               std::shared_ptr<const DiffOrdinaryOtherResult> sOther_result,
                                               std::shared_ptr<DiffExplainMainResult> sExplain_main_result)
    : DiffExplainResult(&sOther_result->backend())
    , m_handler(*pHandler)
    , m_sOther_result(sOther_result)
    , m_sExplain_main_result(sExplain_main_result)
{
    mxb_assert(m_sOther_result);
}

DiffExplainOtherResult::~DiffExplainOtherResult()
{
}

std::chrono::nanoseconds DiffExplainOtherResult::close(const mxs::Reply& reply)
{
    auto rv = DiffExplainResult::close(reply);

    if (!m_sExplain_main_result || m_sExplain_main_result->closed())
    {
        m_handler.ready(*this);

        if (m_sExplain_main_result)
        {
            deregister_from_main();
        }
    }

    return rv;
}

void DiffExplainOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
        deregister_from_main();
    }
}
