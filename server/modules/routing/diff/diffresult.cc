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
 * DiffOrdinaryMainResult
 */
DiffOrdinaryMainResult::DiffOrdinaryMainResult(DiffMainBackend* pBackend, const GWBUF& packet)
    : Base(pBackend)
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
 * DiffOrdinaryOtherResult
 */

DiffOrdinaryOtherResult::DiffOrdinaryOtherResult(DiffOtherBackend* pBackend,
                                                 Handler* pHandler,
                                                 std::shared_ptr<DiffOrdinaryMainResult> sMain_result)
    : Base(pBackend, pHandler, sMain_result)
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

/**
 * DiffExplainMainResult
 */
DiffExplainMainResult::DiffExplainMainResult(DiffMainBackend* pBackend,
                                             std::shared_ptr<DiffOrdinaryMainResult> sMain_result)
    : Base(pBackend)
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
    : Base(static_cast<DiffOtherBackend*>(&sOther_result->backend()), pHandler, sExplain_main_result)
    , m_sOther_result(sOther_result)
{
    mxb_assert(m_sOther_result);
}

DiffExplainOtherResult::~DiffExplainOtherResult()
{
}

std::chrono::nanoseconds DiffExplainOtherResult::close(const mxs::Reply& reply)
{
    auto rv = DiffExplainResult::close(reply);

    if (!m_sMain_result || m_sMain_result->closed())
    {
        m_handler.ready(*this);

        if (m_sMain_result)
        {
            deregister_from_main();
        }
    }

    return rv;
}
