/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <memory>
#include <maxbase/checksum.hh>
#include <maxscale/target.hh>
#include "diffregistry.hh"

class DiffBackend;
class DiffMainBackend;
class DiffOtherBackend;

/**
 * @class DiffResult
 *
 * The result of executing one particular statement.
 */
class DiffResult
{
public:
    enum class Kind
    {
        INTERNAL, // Result of internally generated request.
        EXTERNAL  // Result of a client originating request.
    };

    using Hash = CHash;

    DiffResult(const DiffResult&) = delete;
    DiffResult& operator=(const DiffResult&) = delete;

    using Clock = std::chrono::steady_clock;

    virtual ~DiffResult();

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

    DiffBackend& backend() const
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

    void set_explainers(const DiffRegistry::Entries& explainers)
    {
        m_explainers = explainers;
    }

    const DiffRegistry::Entries& explainers() const
    {
        return m_explainers;
    }

protected:
    DiffResult(DiffBackend* pBackend);

    const mxs::Parser& parser() const
    {
        return m_parser;
    }

private:
    DiffBackend&          m_backend;
    const mxs::Parser&    m_parser;
    Clock::time_point     m_start;
    Clock::time_point     m_end;
    mxb::CRC32            m_checksum;
    mxs::Reply            m_reply;
    DiffRegistry::Entries m_explainers;
};


class DiffOtherResult;

class DiffMainResult final : public DiffResult
                           , public std::enable_shared_from_this<DiffMainResult>

{
public:
    DiffMainResult(DiffMainBackend* pBackend, const GWBUF& packet);

    ~DiffMainResult();

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

    Hash canonical_hash() const;

    bool is_explainable() const
    {
        return !sql().empty();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend class DiffOtherResult;

    void add_dependent(std::shared_ptr<DiffOtherResult> sDependent)
    {
        mxb_assert(m_dependents.find(sDependent) == m_dependents.end());
        m_dependents.insert(sDependent);
    }

    void remove_dependent(std::shared_ptr<DiffOtherResult> sDependent)
    {
        auto it = m_dependents.find(sDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    const int64_t                              m_id;
    GWBUF                                      m_packet;
    mutable std::string_view                   m_sql;
    mutable uint32_t                           m_command {0};
    mutable std::string_view                   m_canonical;
    mutable Hash                               m_canonical_hash {0};
    std::set<std::shared_ptr<DiffOtherResult>> m_dependents;
};


class DiffOtherResult final : public DiffResult
                            , public std::enable_shared_from_this<DiffOtherResult>
{
public:
    class Handler
    {
    public:
        virtual void ready(DiffOtherResult& other_result) = 0;
    };

    DiffOtherResult(DiffOtherBackend* pBackend,
                    Handler* pHandler,
                    std::shared_ptr<DiffMainResult> sMain_result);

    ~DiffOtherResult();

    bool registered_at_main() const
    {
        return m_registered_at_main;
    }

    void register_at_main()
    {
        mxb_assert(!m_registered_at_main);
        m_sMain_result->add_dependent(shared_from_this());
        m_registered_at_main = true;
    }

    void deregister_from_main()
    {
        mxb_assert(m_registered_at_main);
        m_sMain_result->remove_dependent(shared_from_this());
        m_registered_at_main = false;
    }

    DiffMainResult& main_result() const
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

    Hash canonical_hash() const
    {
        return m_sMain_result->canonical_hash();
    }

    bool is_explainable() const
    {
        return m_sMain_result->is_explainable();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend DiffMainResult;

    void main_was_closed();

private:
    Handler&                        m_handler;
    std::shared_ptr<DiffMainResult> m_sMain_result;
    bool                            m_registered_at_main { false };
};


class DiffExplainResult : public DiffResult
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
    DiffExplainResult(DiffBackend* pBackend)
        : DiffResult(pBackend)
    {
    }

private:
    std::string m_json;
};


class DiffExplainOtherResult;

class DiffExplainMainResult final : public DiffExplainResult
{
public:
    DiffExplainMainResult(DiffMainBackend* pBackend, std::shared_ptr<DiffMainResult> sMain_result);

    ~DiffExplainMainResult();

    std::string_view sql() const override
    {
        return m_sMain_result->sql();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend class DiffExplainOtherResult;

    void add_dependent(std::shared_ptr<DiffExplainOtherResult> sDependent)
    {
        mxb_assert(m_dependents.find(sDependent) == m_dependents.end());
        m_dependents.insert(sDependent);
    }

    void remove_dependent(std::shared_ptr<DiffExplainOtherResult> sDependent)
    {
        auto it = m_dependents.find(sDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

private:
    std::shared_ptr<DiffMainResult>                m_sMain_result;
    std::set<std::shared_ptr<DiffExplainOtherResult>> m_dependents;
};


class DiffExplainOtherResult final : public DiffExplainResult
                                   , public std::enable_shared_from_this<DiffExplainOtherResult>

{
public:
    class Handler
    {
    public:
        virtual void ready(const DiffExplainOtherResult& explain_other_result) = 0;
    };

    DiffExplainOtherResult(Handler* pHandler,
                           std::shared_ptr<const DiffOtherResult> sOther_result,
                           std::shared_ptr<DiffExplainMainResult> sExplain_main_result);
    ~DiffExplainOtherResult();

    bool registered_at_main() const
    {
        return m_registered_at_main;
    }

    void register_at_main()
    {
        mxb_assert(!m_registered_at_main);

        if (m_sExplain_main_result)
        {
            m_sExplain_main_result->add_dependent(shared_from_this());
            m_registered_at_main = true;
        }
    }

    void deregister_from_main()
    {
        mxb_assert(m_registered_at_main);
        m_sExplain_main_result->remove_dependent(shared_from_this());
        m_registered_at_main = false;
    }

    std::string_view sql() const override
    {
        return m_sOther_result->sql();
    }

    const DiffOtherResult& other_result() const
    {
        return *m_sOther_result.get();
    }

    const DiffExplainMainResult* explain_main_result() const
    {
        return m_sExplain_main_result.get();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    friend DiffExplainMainResult;

    void main_was_closed();

private:
    Handler&                               m_handler;
    std::shared_ptr<const DiffOtherResult> m_sOther_result;
    std::shared_ptr<DiffExplainMainResult> m_sExplain_main_result;
    bool                                   m_registered_at_main { false };
};
