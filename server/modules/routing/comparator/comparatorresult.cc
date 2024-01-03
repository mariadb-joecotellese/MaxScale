/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "comparatorresult.hh"
#include "comparatorbackend.hh"

/**
 * ComparatorResult
 */
ComparatorResult::~ComparatorResult()
{
}

void ComparatorResult::close(const mxs::Reply& reply)
{
    mxb_assert(!closed());
    m_reply = reply;
    m_end = Clock::now();
}


/**
 * ComparatorMainResult
 */
ComparatorMainResult::ComparatorMainResult(ComparatorMainBackend* pBackend,
                                           const std::string& sql,
                                           uint8_t command)
    : ComparatorResult(pBackend)
    , m_sql(sql)
    , m_command(command)
{
}

void ComparatorMainResult::close(const mxs::Reply& reply)
{
    ComparatorResult::close(reply);

    // A dependent may end up removing itself.
    auto dependents = m_dependents;

    for (ComparatorOtherResult* pDependent : dependents)
    {
        pDependent->main_was_closed();
    }
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


void ComparatorOtherResult::close(const mxs::Reply& reply)
{
    ComparatorResult::close(reply);

    if (m_sMain_result->closed())
    {
        m_handler.ready(*this);
    }
}

void ComparatorOtherResult::main_was_closed()
{
    if (closed())
    {
        m_handler.ready(*this);
    }
}
