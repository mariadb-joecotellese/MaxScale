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
#include "cregistry.hh"

class CBackend;
class CMainBackend;
class COtherBackend;

/**
 * @class CResult
 *
 * The result of executing one particular statement.
 */
class CResult
{
public:
    enum class Kind
    {
        INTERNAL, // Result of internally generated request.
        EXTERNAL  // Result of a client originating request.
    };

    using Hash = CHash;

    CResult(const CResult&) = delete;
    CResult& operator=(const CResult&) = delete;

    using Clock = std::chrono::steady_clock;

    virtual ~CResult();

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

    CBackend& backend() const
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

    void set_explainers(const CRegistry::Entries& explainers)
    {
        m_explainers = explainers;
    }

    const CRegistry::Entries& explainers() const
    {
        return m_explainers;
    }

protected:
    CResult(CBackend* pBackend)
        : m_backend(*pBackend)
        , m_start(Clock::now())
        , m_end(Clock::time_point::max())
    {
        mxb_assert(pBackend);
    }

private:
    CBackend&          m_backend;
    Clock::time_point  m_start;
    Clock::time_point  m_end;
    mxb::CRC32         m_checksum;
    mxs::Reply         m_reply;
    CRegistry::Entries m_explainers;
};


class COtherResult;

class CMainResult final : public CResult
                        , public std::enable_shared_from_this<CMainResult>

{
public:
    CMainResult(CMainBackend* pBackend, const GWBUF& packet);

    ~CMainResult() override = default;

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
    friend class COtherResult;

    void add_dependent(COtherResult* pDependent)
    {
        mxb_assert(m_dependents.find(pDependent) == m_dependents.end());
        m_dependents.insert(pDependent);
    }

    void remove_dependent(COtherResult* pDependent)
    {
        auto it = m_dependents.find(pDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    const int64_t            m_id;
    GWBUF                    m_packet;
    mutable std::string_view m_sql;
    mutable uint32_t         m_command {0};
    mutable std::string_view m_canonical;
    mutable Hash             m_hash {0};
    std::set<COtherResult*>  m_dependents;
};


class COtherResult final : public CResult
                         , public std::enable_shared_from_this<COtherResult>
{
public:
    class Handler
    {
    public:
        virtual void ready(COtherResult& other_result) = 0;
    };

    COtherResult(COtherBackend* pBackend,
                 Handler* pHandler,
                 std::shared_ptr<CMainResult> sMain_result);

    ~COtherResult();

    CMainResult& main_result() const
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
    friend CMainResult;

    void main_was_closed();

private:
    Handler&                     m_handler;
    std::shared_ptr<CMainResult> m_sMain_result;
};


class CExplainResult : public CResult
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
    CExplainResult(CBackend* pBackend)
        : CResult(pBackend)
    {
    }

};


class CExplainOtherResult;

class CExplainMainResult final : public CExplainResult
{
public:
    CExplainMainResult(CMainBackend* pBackend, std::shared_ptr<CMainResult> sMain_result);

    std::string_view sql() const override
    {
        return m_sMain_result->sql();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend class CExplainOtherResult;

    void add_dependent(CExplainOtherResult* pDependent)
    {
        mxb_assert(m_dependents.find(pDependent) == m_dependents.end());
        m_dependents.insert(pDependent);
    }

    void remove_dependent(CExplainOtherResult* pDependent)
    {
        auto it = m_dependents.find(pDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    std::shared_ptr<CMainResult>   m_sMain_result;
    std::set<CExplainOtherResult*> m_dependents;
};


class CExplainOtherResult final : public CExplainResult
{
public:
    class Handler
    {
    public:
        virtual void ready(const CExplainOtherResult& explain_other_result) = 0;
    };

    CExplainOtherResult(Handler* pHandler,
                        std::shared_ptr<const COtherResult> sOther_result,
                        std::shared_ptr<CExplainMainResult> sExplain_main_result)
        : CExplainResult(&sOther_result->backend())
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

    ~CExplainOtherResult()
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

    const COtherResult& other_result() const
    {
        return *m_sOther_result.get();
    }

    const CExplainMainResult* explain_main_result() const
    {
        return m_sExplain_main_result.get();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend CExplainMainResult;

    void main_was_closed();

private:
    Handler&                            m_handler;
    std::shared_ptr<const COtherResult> m_sOther_result;
    std::shared_ptr<CExplainMainResult> m_sExplain_main_result;
};
