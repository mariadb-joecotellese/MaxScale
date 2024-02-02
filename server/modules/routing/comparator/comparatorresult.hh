/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <memory>
#include <maxbase/checksum.hh>
#include <maxscale/target.hh>
#include "comparatorregistry.hh"

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
    enum class Kind
    {
        INTERNAL, // Result of internally generated request.
        EXTERNAL  // Result of a client originating request.
    };

    using Hash = ComparatorHash;

    ComparatorResult(const ComparatorResult&) = delete;
    ComparatorResult& operator=(const ComparatorResult&) = delete;

    using Clock = std::chrono::steady_clock;

    virtual ~ComparatorResult();

    virtual Kind kind() const
    {
        return Kind::INTERNAL;
    }

    bool closed() const
    {
        return m_end != Clock::time_point::max();
    }

    virtual void process(const GWBUF& buffer);

    virtual std::chrono::nanoseconds close(const mxs::Reply& reply);

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

    std::chrono::nanoseconds duration() const
    {
        mxb_assert(closed());
        return std::chrono::duration_cast<std::chrono::nanoseconds>(m_end - m_start);
    }

    void set_explainers(const ComparatorRegistry::Entries& explainers)
    {
        m_explainers = explainers;
    }

    const ComparatorRegistry::Entries& explainers() const
    {
        return m_explainers;
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
    ComparatorBackend&          m_backend;
    Clock::time_point           m_start;
    Clock::time_point           m_end;
    mxb::CRC32                  m_checksum;
    mxs::Reply                  m_reply;
    ComparatorRegistry::Entries m_explainers;
};


class ComparatorOtherResult;

class ComparatorMainResult final : public ComparatorResult
                                 , public std::enable_shared_from_this<ComparatorMainResult>

{
public:
    ComparatorMainResult(ComparatorMainBackend* pBackend, const GWBUF& packet);

    ~ComparatorMainResult() override = default;

    Kind kind() const override
    {
        return Kind::EXTERNAL;
    }

    int64_t id() const
    {
        return m_id;
    }

    std::string_view sql() const;

    uint8_t command() const;

    std::string_view canonical() const;

    Hash hash() const;

    bool is_explainable() const
    {
        return !sql().empty();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

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
    const int64_t                    m_id;
    GWBUF                            m_packet;
    mutable std::string_view         m_sql;
    mutable uint32_t                 m_command {0};
    mutable std::string_view         m_canonical;
    mutable Hash                     m_hash {0};
    std::set<ComparatorOtherResult*> m_dependents;
};


class ComparatorOtherResult final : public ComparatorResult
                                  , public std::enable_shared_from_this<ComparatorOtherResult>
{
public:
    class Handler
    {
    public:
        virtual void ready(ComparatorOtherResult& other_result) = 0;
    };

    ComparatorOtherResult(ComparatorOtherBackend* pBackend,
                          Handler* pHandler,
                          std::shared_ptr<ComparatorMainResult> sMain_result);

    ~ComparatorOtherResult();

    ComparatorMainResult& main_result() const
    {
        mxb_assert(m_sMain_result);
        return *m_sMain_result.get();
    }

    int64_t id() const
    {
        return m_sMain_result->id();
    }

    std::string_view sql() const
    {
        return m_sMain_result->sql();
    }

    uint8_t command() const
    {
        return m_sMain_result->command();
    }

    std::string_view canonical() const
    {
        return m_sMain_result->canonical();
    }

    Hash hash() const
    {
        return m_sMain_result->hash();
    }

    bool is_explainable() const
    {
        return m_sMain_result->is_explainable();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend ComparatorMainResult;

    void main_was_closed();

private:
    Handler&                              m_handler;
    std::shared_ptr<ComparatorMainResult> m_sMain_result;
};


class ComparatorExplainResult : public ComparatorResult
{
public:
    virtual std::string_view sql() const = 0;

    const std::string& error() const
    {
        const auto& r = reply();
        mxb_assert(r.is_complete());

        return r.error().message();
    }

    std::string_view json() const
    {
        const auto& r = reply();
        mxb_assert(r.is_complete());

        std::string_view s;

        if (!r.row_data().empty())
        {
            mxb_assert(r.row_data().size() == 1);
            mxb_assert(r.row_data().front().size() == 1);

            s = r.row_data().front().front();
        }

        return s;
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

protected:
    ComparatorExplainResult(ComparatorBackend* pBackend)
        : ComparatorResult(pBackend)
    {
    }

};


class ComparatorExplainOtherResult;

class ComparatorExplainMainResult final : public ComparatorExplainResult
{
public:
    ComparatorExplainMainResult(ComparatorMainBackend* pBackend,
                                std::shared_ptr<ComparatorMainResult> sMain_result);

    std::string_view sql() const override
    {
        return m_sMain_result->sql();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend class ComparatorExplainOtherResult;

    void add_dependent(ComparatorExplainOtherResult* pDependent)
    {
        mxb_assert(m_dependents.find(pDependent) == m_dependents.end());
        m_dependents.insert(pDependent);
    }

    void remove_dependent(ComparatorExplainOtherResult* pDependent)
    {
        auto it = m_dependents.find(pDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    std::shared_ptr<ComparatorMainResult>   m_sMain_result;
    std::set<ComparatorExplainOtherResult*> m_dependents;
};


class ComparatorExplainOtherResult final : public ComparatorExplainResult
{
public:
    class Handler
    {
    public:
        virtual void ready(const ComparatorExplainOtherResult& explain_other_result) = 0;
    };

    ComparatorExplainOtherResult(Handler* pHandler,
                                 std::shared_ptr<const ComparatorOtherResult> sOther_result,
                                 std::shared_ptr<ComparatorExplainMainResult> sExplain_main_result)
        : ComparatorExplainResult(&sOther_result->backend())
        , m_handler(*pHandler)
        , m_sOther_result(sOther_result)
        , m_sExplain_main_result(sExplain_main_result)
    {
        mxb_assert(m_sOther_result);

        if (m_sExplain_main_result)
        {
            m_sExplain_main_result->add_dependent(this);
        }
    }

    ~ComparatorExplainOtherResult()
    {
        if (m_sExplain_main_result)
        {
            m_sExplain_main_result->remove_dependent(this);
        }
    }

    std::string_view sql() const override
    {
        return m_sOther_result->sql();
    }

    const ComparatorOtherResult& other_result() const
    {
        return *m_sOther_result.get();
    }

    const ComparatorExplainMainResult* explain_main_result() const
    {
        return m_sExplain_main_result.get();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend ComparatorExplainMainResult;

    void main_was_closed();

private:
    Handler&                                     m_handler;
    std::shared_ptr<const ComparatorOtherResult> m_sOther_result;
    std::shared_ptr<ComparatorExplainMainResult> m_sExplain_main_result;
};
