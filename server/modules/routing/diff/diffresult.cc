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
DiffMainResult::DiffMainResult(DiffMainBackend* pBackend, const GWBUF& packet)
    : DiffResult(pBackend)
    , m_id(this_unit.id++)
    , m_packet(packet.shallow_clone())
{
}

DiffMainResult::~DiffMainResult()
{
}

std::string_view DiffMainResult::sql() const
{
    if (m_sql.empty())
    {
        m_sql = backend().phelper().get_sql(m_packet);
    }

    return m_sql;
}

uint8_t DiffMainResult::command() const
{
    if (m_command == 0)
    {
        m_command = backend().phelper().get_command(m_packet);
    }

    return m_command;
}

std::string_view DiffMainResult::canonical() const
{
    if (m_canonical.empty())
    {
        m_canonical = backend().parser().get_canonical(m_packet);
    }

    return m_canonical;
}

DiffResult::Hash DiffMainResult::hash() const
{
    if (m_hash == 0)
    {
        m_hash = DiffRegistry::hash_for(canonical());
    }

    return m_hash;
}

std::chrono::nanoseconds DiffMainResult::close(const mxs::Reply& reply)
{
    auto rv = DiffResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (std::shared_ptr<DiffOtherResult> sDependent : dependents)
    {
        sDependent->main_was_closed();
    }

    return rv;
}

/**
 * DiffOtherResult
 */

DiffOtherResult::DiffOtherResult(DiffOtherBackend* pBackend,
                                 Handler* pHandler,
                                 std::shared_ptr<DiffMainResult> sMain_result)
    : DiffResult(pBackend)
    , m_handler(*pHandler)
    , m_sMain_result(sMain_result)
{
}

DiffOtherResult::~DiffOtherResult()
{
}

std::chrono::nanoseconds DiffOtherResult::close(const mxs::Reply& reply)
{
    auto rv = DiffResult::close(reply);

    if (m_sMain_result->closed())
    {
        m_handler.ready(*this);
        m_sMain_result->remove_dependent(shared_from_this());
    }

    return rv;
}

void DiffOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
        m_sMain_result->remove_dependent(shared_from_this());
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
                                             std::shared_ptr<DiffMainResult> sMain_result)
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
                                               std::shared_ptr<const DiffOtherResult> sOther_result,
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
            m_sExplain_main_result->remove_dependent(shared_from_this());
        }
    }

    return rv;
}

void DiffExplainOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
        m_sExplain_main_result->remove_dependent(shared_from_this());
    }
}
