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

    Clock::time_point start() const
    {
        return m_start;
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

    virtual std::string_view canonical() const = 0;

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


/**
 * @template DiffMainResult
 *
 * A result originating from the 'main' server.
 * To be instantiated with the class from the 'other' hierarchy that
 * matches the 'main' class that is derived from this template.
 */
template<class OtherResult_>
class DiffMainResult : public DiffResult
{
public:
    using Backend = DiffMainBackend;
    using OtherResult = OtherResult_;

    void add_dependent(std::shared_ptr<OtherResult> sDependent)
    {
        mxb_assert(m_dependents.find(sDependent) == m_dependents.end());
        m_dependents.insert(sDependent);
    }

    void remove_dependent(std::shared_ptr<OtherResult> sDependent)
    {
        auto it = m_dependents.find(sDependent);

        mxb_assert(it != m_dependents.end());

        m_dependents.erase(it);
    }

protected:
    DiffMainResult(DiffMainBackend* pBackend)
        : DiffResult(pBackend)
    {
    }

protected:
    std::set<std::shared_ptr<OtherResult>> m_dependents;
};


/**
 * @class DiffOrdinaryMainResult
 *
 * Contains results related to an ordinary request sent to 'main'.
 */
class DiffOrdinaryOtherResult;

class DiffOrdinaryMainResult final : public DiffMainResult<DiffOrdinaryOtherResult>
                                   , public std::enable_shared_from_this<DiffOrdinaryMainResult>

{
public:
    using Base = DiffMainResult<DiffOrdinaryOtherResult>;

    DiffOrdinaryMainResult(DiffMainBackend* pBackend, const GWBUF& packet);

    ~DiffOrdinaryMainResult();

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

    std::string_view canonical() const override;

    Hash canonical_hash() const;

    bool is_explainable() const
    {
        return !sql().empty();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    const int64_t            m_id;
    GWBUF                    m_packet;
    mutable std::string_view m_sql;
    mutable uint32_t         m_command {0};
    mutable std::string_view m_canonical;
    mutable Hash             m_canonical_hash {0};
};


/**
 * @class DiffOtherResult
 *
 * A result originating from the 'other' server.
 */
class DiffOtherResult : public DiffResult
{
public:
    virtual bool registered_at_main() const = 0;

    virtual void deregister_from_main() = 0;

protected:
    using DiffResult::DiffResult;
};

/**
 * @template DiffOtherResultT
 *
 * A result originating from the 'other' server.
 * To be instantiated with the class from the 'main' hierarchy that
 * matches the 'other' class that is derived from this template.
 */
template<class MainResult_>
class DiffOtherResultT : public DiffOtherResult
{
public:
    using Backend = DiffOtherBackend;
    using MainResult = MainResult_;

    class Handler
    {
    public:
        virtual void ready(typename MainResult::OtherResult& other_result) = 0;
    };

    bool registered_at_main() const override
    {
        return m_registered_at_main;
    }

    void register_at_main()
    {
        mxb_assert(!m_registered_at_main);
        if (m_sMain_result)
        {
            auto* pThis = static_cast<typename MainResult::OtherResult*>(this);
            m_sMain_result->add_dependent(pThis->shared_from_this());
            m_registered_at_main = true;
        }
    }

    void deregister_from_main() override
    {
        mxb_assert(m_registered_at_main);
        auto* pThis = static_cast<typename MainResult::OtherResult*>(this);
        m_sMain_result->remove_dependent(pThis->shared_from_this());
        m_registered_at_main = false;
    }

private:
    friend MainResult;

    void main_was_closed()
    {
        if (closed())
        {
            auto* pThis = static_cast<typename MainResult::OtherResult*>(this);
            m_handler.ready(*pThis);
            deregister_from_main();
        }
    }

protected:
    DiffOtherResultT(DiffOtherBackend* pBackend,
                     Handler* pHandler,
                     std::shared_ptr<MainResult> sMain_result = std::shared_ptr<MainResult>())
        : DiffOtherResult(pBackend)
        , m_handler(*pHandler)
        , m_sMain_result(sMain_result)
    {
    }

    Handler&                    m_handler;
    std::shared_ptr<MainResult> m_sMain_result;

private:
    bool m_registered_at_main { false };
};


/**
 * @class DiffOrdinaryOtherResult
 *
 * Containes results related to an ordinary request sent to 'other'.
 */
class DiffOrdinaryOtherResult final : public DiffOtherResultT<DiffOrdinaryMainResult>
                                    , public std::enable_shared_from_this<DiffOrdinaryOtherResult>
{
public:
    using Base = DiffOtherResultT<DiffOrdinaryMainResult>;

    DiffOrdinaryOtherResult(DiffOtherBackend* pBackend,
                            Handler* pHandler,
                            std::shared_ptr<DiffOrdinaryMainResult> sMain_result);

    ~DiffOrdinaryOtherResult();

    DiffOrdinaryMainResult& main_result() const
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

    std::string_view canonical() const override
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
};


/**
 * @template DiffExplainResult
 *
 * An EXPLAIN result. Can be instantiated with a class from the 'main'
 * or 'other' hierarchy.
 */
template<class Base>
class DiffExplainResult : public Base
{
public:
    virtual std::string_view sql() const = 0;

    const std::string& error() const
    {
        const auto& r = this->reply();
        mxb_assert(r.is_complete());

        return r.error().message();
    }

    std::string_view json() const
    {
        return m_json;
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override
    {
        Base::close(reply);

        mxb_assert(reply.is_complete());

        if (!reply.row_data().empty())
        {
            mxb_assert(reply.row_data().size() == 1);
            mxb_assert(reply.row_data().front().size() == 1);

            m_json = reply.row_data().front().front();
        }

        // Return 0, so that the duration of the EXPLAIN request is not
        // included in the total duration.
        return std::chrono::nanoseconds { 0 };
    }

protected:
    using Base::Base;

private:
    std::string m_json;
};


/**
 * @class DiffExplainMainResult
 *
 * Contains results related to an EXPLAIN request sent to 'main'.
 */
class DiffExplainOtherResult;

class DiffExplainMainResult final : public DiffExplainResult<DiffMainResult<DiffExplainOtherResult>>
{
public:
    using Base = DiffExplainResult<DiffMainResult<DiffExplainOtherResult>>;

    DiffExplainMainResult(DiffMainBackend* pBackend, std::shared_ptr<DiffOrdinaryMainResult> sMain_result);

    ~DiffExplainMainResult();

    std::string_view canonical() const override
    {
        return m_sMain_result->canonical();
    }

    std::string_view sql() const override
    {
        return m_sMain_result->sql();
    }

    const DiffOrdinaryMainResult& origin_result() const
    {
        return *m_sMain_result.get();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    std::shared_ptr<DiffOrdinaryMainResult> m_sMain_result;
};


/**
 * @class DiffExplainOtherResult
 *
 * Containes results related to an EXPLAIN request sent to 'other'.
 */
class DiffExplainOtherResult final : public DiffExplainResult<DiffOtherResultT<DiffExplainMainResult>>
                                   , public std::enable_shared_from_this<DiffExplainOtherResult>

{
public:
    using Base = DiffExplainResult<DiffOtherResultT<DiffExplainMainResult>>;

    DiffExplainOtherResult(Handler* pHandler,
                           std::shared_ptr<const DiffOrdinaryOtherResult> sOther_result,
                           std::shared_ptr<DiffExplainMainResult> sExplain_main_result);
    ~DiffExplainOtherResult();

    std::string_view canonical() const override
    {
        return m_sOther_result->canonical();
    }

    std::string_view sql() const override
    {
        return m_sOther_result->sql();
    }

    const DiffOrdinaryOtherResult& origin_result() const
    {
        return *m_sOther_result.get();
    }

    const DiffExplainMainResult* explain_main_result() const
    {
        return m_sMain_result.get();
    }

    std::chrono::nanoseconds close(const mxs::Reply& reply) override;

private:
    std::shared_ptr<const DiffOrdinaryOtherResult> m_sOther_result;
};
