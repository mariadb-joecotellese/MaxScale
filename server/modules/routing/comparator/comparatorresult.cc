/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorresult.hh"
#include <vector>
#include "comparatorbackend.hh"
#include "comparatorregistry.hh"

namespace
{

struct ThisUnit
{
    std::atomic<int64_t> id { 1 };
} this_unit;

}

/**
 * ComparatorResult
 */
ComparatorResult::~ComparatorResult()
{
}

void ComparatorResult::process(const GWBUF& buffer)
{
    mxb_assert(!closed());
    m_checksum.update(buffer);
}

std::chrono::nanoseconds ComparatorResult::close(const mxs::Reply& reply)
{
    mxb_assert(!closed());
    m_reply = reply;
    m_end = Clock::now();

    return duration();
}


/**
 * ComparatorMainResult
 */
ComparatorMainResult::ComparatorMainResult(ComparatorMainBackend* pBackend, const GWBUF& packet)
    : ComparatorResult(pBackend)
    , m_id(this_unit.id++)
    , m_packet(packet.shallow_clone())
{
}

std::string_view ComparatorMainResult::sql() const
{
    if (m_sql.empty())
    {
        m_sql = backend().phelper().get_sql(m_packet);
    }

    return m_sql;
}

uint8_t ComparatorMainResult::command() const
{
    if (m_command == 0)
    {
        m_command = backend().phelper().get_command(m_packet);
    }

    return m_command;
}

std::string_view ComparatorMainResult::canonical() const
{
    if (m_canonical.empty())
    {
        m_canonical = backend().parser().get_canonical(m_packet);
    }

    return m_canonical;
}

ComparatorResult::Hash ComparatorMainResult::hash() const
{
    if (m_hash == 0)
    {
        m_hash = ComparatorRegistry::hash_for(canonical());
    }

    return m_hash;
}

std::chrono::nanoseconds ComparatorMainResult::close(const mxs::Reply& reply)
{
    auto rv = ComparatorResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (ComparatorOtherResult* pDependent : dependents)
    {
        pDependent->main_was_closed();
    }

    return rv;
}

/**
 * ComparatorOtherResult
 */

ComparatorOtherResult::ComparatorOtherResult(ComparatorOtherBackend* pBackend,
                                             Handler* pHandler,
                                             std::shared_ptr<ComparatorMainResult> sMain_result)
    : ComparatorResult(pBackend)
    , m_handler(*pHandler)
    , m_sMain_result(sMain_result)
{
    m_sMain_result->add_dependent(this);
}

ComparatorOtherResult::~ComparatorOtherResult()
{
    m_sMain_result->remove_dependent(this);
}


std::chrono::nanoseconds ComparatorOtherResult::close(const mxs::Reply& reply)
{
    auto rv = ComparatorResult::close(reply);

    if (m_sMain_result->closed())
    {
        m_handler.ready(*this);
    }

    return rv;
}

void ComparatorOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
    }
}


/**
 * ComparatorExplainResult
 */

std::chrono::nanoseconds ComparatorExplainResult::close(const mxs::Reply& reply)
{
    ComparatorResult::close(reply);

    // Return 0, so that the duration of the EXPLAIN request is not
    // included in the total duration.
    return std::chrono::milliseconds { 0 };
}

/**
 * ComparatorExplainMainResult
 */
ComparatorExplainMainResult::ComparatorExplainMainResult(ComparatorMainBackend* pBackend,
                                                         std::shared_ptr<ComparatorMainResult> sMain_result)
    : ComparatorExplainResult(pBackend)
    , m_sMain_result(sMain_result)
{
    mxb_assert(m_sMain_result);
}

std::chrono::nanoseconds ComparatorExplainMainResult::close(const mxs::Reply& reply)
{
    auto rv = ComparatorExplainResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (ComparatorExplainOtherResult* pDependent : dependents)
    {
        pDependent->main_was_closed();
    }

    static_cast<ComparatorMainBackend&>(backend()).ready(*this);

    return rv;
}

/**
 * ComparatorExplainOtherResult
 */
std::chrono::nanoseconds ComparatorExplainOtherResult::close(const mxs::Reply& reply)
{
    auto rv = ComparatorExplainResult::close(reply);

    if (!m_sExplain_main_result || m_sExplain_main_result->closed())
    {
        m_handler.ready(*this);
    }

    return rv;
}

void ComparatorExplainOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
    }
}
