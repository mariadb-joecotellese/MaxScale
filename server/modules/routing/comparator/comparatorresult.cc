/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorresult.hh"
#include <vector>
#include "comparatorbackend.hh"

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
ComparatorMainResult::ComparatorMainResult(ComparatorMainBackend* pBackend,
                                           std::string_view sql,
                                           uint8_t command)
    : ComparatorResult(pBackend)
    , m_sql(sql)
    , m_command(command)
{
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

ComparatorExplainResult::ComparatorExplainResult(Handler* pHandler,
                                                 std::shared_ptr<const ComparatorOtherResult> sOther_result)
    : ComparatorResult(&sOther_result->backend())
    , m_handler(*pHandler)
    , m_sOther_result(sOther_result)
{
}

std::chrono::nanoseconds ComparatorExplainResult::close(const mxs::Reply& reply)
{
    auto rv = ComparatorResult::close(reply);

    const auto& e = reply.error();

    std::string error;
    std::string_view json;

    if (e)
    {
        error = e.message();
    }
    else
    {
        if (!reply.row_data().empty())
        {
            mxb_assert(reply.row_data().size() == 1);
            mxb_assert(reply.row_data().front().size() == 1);

            json = reply.row_data().front().front();
        }

        mxb_assert(reply.is_complete());
    }

    m_handler.ready(*this, error, json);

    // Return 0, so that the duration of the EXPLAIN request is not
    // included in the total duration.
    return std::chrono::milliseconds { 0 };
}
