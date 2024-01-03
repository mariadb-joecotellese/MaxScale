/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <maxbase/checksum.hh>
#include <maxscale/target.hh>

class ComparatorBackend;
class ComparatorMainBackend;
class ComparatorOtherBackend;

/**
 * @class ComparatorResult
 *
 * The result of executing one particular statement.
 */
class ComparatorResult
{
public:
    ComparatorResult(const ComparatorResult&) = delete;
    ComparatorResult& operator=(const ComparatorResult&) = delete;

    using Clock = std::chrono::steady_clock;

    virtual ~ComparatorResult();

    bool closed() const
    {
        return m_end != Clock::time_point::max();
    }

    void update_checksum(const GWBUF& buffer)
    {
        mxb_assert(!closed());
        m_checksum.update(buffer);
    }

    virtual void close(const mxs::Reply& reply);

    void reset()
    {
        m_start = Clock::now();
        m_end = m_start;
        m_checksum.reset();
        m_reply.clear();
    }

    ComparatorBackend& backend() const
    {
        return m_backend;
    }

    const mxb::CRC32& checksum() const
    {
        mxb_assert(closed());
        return m_checksum;
    }

    const mxs::Reply& reply() const
    {
        mxb_assert(closed());
        return m_reply;
    }

    std::chrono::milliseconds duration() const
    {
        mxb_assert(closed());
        return std::chrono::duration_cast<std::chrono::milliseconds>(m_end - m_start);
    }

protected:
    ComparatorResult(ComparatorBackend* pBackend)
        : m_backend(*pBackend)
        , m_start(Clock::now())
        , m_end(Clock::time_point::max())
    {
        mxb_assert(pBackend);
    }

private:
    ComparatorBackend& m_backend;
    Clock::time_point  m_start;
    Clock::time_point  m_end;
    mxb::CRC32         m_checksum;
    mxs::Reply         m_reply;
};

class ComparatorOtherResult;

class ComparatorMainResult final : public ComparatorResult
{
public:
    ComparatorMainResult(ComparatorMainBackend* pBackend, const std::string& sql, uint8_t command);

    ~ComparatorMainResult() override = default;

    const std::string& sql() const
    {
        return m_sql;
    }

    uint8_t command() const
    {
        return m_command;
    }

    void close(const mxs::Reply& reply) override;

private:
    friend class ComparatorOtherResult;

    void add_dependent(ComparatorOtherResult* pDependent)
    {
        mxb_assert(m_dependents.find(pDependent) == m_dependents.end());
        m_dependents.insert(pDependent);
    }

    void remove_dependent(ComparatorOtherResult* pDependent)
    {
        auto it = m_dependents.find(pDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    std::string                      m_sql;
    uint8_t                          m_command;
    std::set<ComparatorOtherResult*> m_dependents;
};

class ComparatorOtherResult final : public ComparatorResult
{
public:
    class Handler
    {
    public:
        virtual void ready(const ComparatorOtherResult& other_result) = 0;
    };

    ComparatorOtherResult(ComparatorOtherBackend* pBackend,
                          Handler* pHandler,
                          std::shared_ptr<ComparatorMainResult> sMain_result);

    ~ComparatorOtherResult();

    const ComparatorMainResult& main_result() const
    {
        mxb_assert(m_sMain_result);
        return *m_sMain_result.get();
    }

    void close(const mxs::Reply& reply) override;

private:
    friend ComparatorMainResult;

    void main_was_closed();

private:
    Handler&                              m_handler;
    std::shared_ptr<ComparatorMainResult> m_sMain_result;
};
