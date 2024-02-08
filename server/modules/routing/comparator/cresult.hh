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

    ~CMainResult();

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

    void add_dependent(std::shared_ptr<COtherResult> sDependent)
    {
        mxb_assert(m_dependents.find(sDependent) == m_dependents.end());
        m_dependents.insert(sDependent);
    }

    void remove_dependent(std::shared_ptr<COtherResult> sDependent)
    {
        auto it = m_dependents.find(sDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    const int64_t                           m_id;
    GWBUF                                   m_packet;
    mutable std::string_view                m_sql;
    mutable uint32_t                        m_command {0};
    mutable std::string_view                m_canonical;
    mutable Hash                            m_hash {0};
    std::set<std::shared_ptr<COtherResult>> m_dependents;
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

    void register_at_main()
    {
        m_sMain_result->add_dependent(shared_from_this());
    }

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
        return m_json;
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

protected:
    CExplainResult(CBackend* pBackend)
        : CResult(pBackend)
    {
    }

private:
    std::string m_json;
};


class CExplainOtherResult;

class CExplainMainResult final : public CExplainResult
{
public:
    CExplainMainResult(CMainBackend* pBackend, std::shared_ptr<CMainResult> sMain_result);

    ~CExplainMainResult();

    std::string_view sql() const override
    {
        return m_sMain_result->sql();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend class CExplainOtherResult;

    void add_dependent(std::shared_ptr<CExplainOtherResult> sDependent)
    {
        mxb_assert(m_dependents.find(sDependent) == m_dependents.end());
        m_dependents.insert(sDependent);
    }

    void remove_dependent(std::shared_ptr<CExplainOtherResult> sDependent)
    {
        auto it = m_dependents.find(sDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    std::shared_ptr<CMainResult>                   m_sMain_result;
    std::set<std::shared_ptr<CExplainOtherResult>> m_dependents;
};


class CExplainOtherResult final : public CExplainResult
                                , public std::enable_shared_from_this<CExplainOtherResult>

{
public:
    class Handler
    {
    public:
        virtual void ready(const CExplainOtherResult& explain_other_result) = 0;
    };

    CExplainOtherResult(Handler* pHandler,
                        std::shared_ptr<const COtherResult> sOther_result,
                        std::shared_ptr<CExplainMainResult> sExplain_main_result);
    ~CExplainOtherResult();

    void register_at_main()
    {
        if (m_sExplain_main_result)
        {
            m_sExplain_main_result->add_dependent(shared_from_this());
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
