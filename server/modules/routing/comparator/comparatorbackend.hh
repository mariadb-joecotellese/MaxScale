/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <deque>
#include <vector>
#include <memory>
#include <maxscale/backend.hh>
#include <maxscale/router.hh>
#include <maxbase/checksum.hh>
#include "comparatorresult.hh"

class ComparatorMainBackend;
class ComparatorOtherBackend;
using SComparatorMainBackend = std::unique_ptr<ComparatorMainBackend>;
using SComparatorOtherBackend = std::unique_ptr<ComparatorOtherBackend>;
using SComparatorOtherBackends = std::vector<SComparatorOtherBackend>;

class ComparatorBackend : public mxs::Backend
{
public:
    using mxs::Backend::Backend;

    virtual void process_result(const GWBUF& buffer) = 0;
    virtual void finish_result(const mxs::Reply& reply) = 0;
};

template<typename R>
class ConcreteComparatorBackend : public ComparatorBackend
{
public:
    using Result = R;
    using SResult = std::shared_ptr<Result>;

    void process_result(const GWBUF& buffer) override
    {
        mxb_assert(!m_results.empty());

        m_results.front()->update_checksum(buffer);
    }

    void finish_result(const mxs::Reply& reply) override
    {
        mxb_assert(reply.is_complete());
        mxb_assert(!m_results.empty());

        SResult sResult = std::move(m_results.front());
        m_results.pop_front();

        sResult->close(reply);
    }

    void close(close_type type = CLOSE_NORMAL) override
    {
        ComparatorBackend::close(type);

        m_results.clear();
    }

    int32_t nBacklog() const
    {
        return m_results.size();
    }

protected:
    using ComparatorBackend::ComparatorBackend;

    std::deque<SResult> m_results;
};

class ComparatorMainBackend final : public ConcreteComparatorBackend<ComparatorMainResult>
{
public:
    using ConcreteComparatorBackend<ComparatorMainResult>::ConcreteComparatorBackend;

    SResult prepare(const std::string& sql, uint8_t command);

    const std::string& sql() const
    {
        return m_sql;
    }

    uint8_t command() const
    {
        return m_command;
    }

private:
    std::string m_sql;
    uint8_t     m_command;
};

class ComparatorOtherBackend final : public ConcreteComparatorBackend<ComparatorOtherResult>
                                   , public ComparatorOtherResult::Handler

{
public:
    ComparatorOtherBackend(mxs::Endpoint* pEndpoint)
        : ConcreteComparatorBackend<ComparatorOtherResult>(pEndpoint)
    {
    }

    void set_result_handler(ComparatorOtherResult::Handler* pResult_handler)
    {
        m_pResult_handler = pResult_handler;
    }

    void prepare(const ComparatorMainBackend::SResult& sMain_result);

    void ready(const ComparatorOtherResult& other_result) override;

private:
    ComparatorOtherResult::Handler* m_pResult_handler { nullptr };
};

namespace comparator
{

std::pair<SComparatorMainBackend, SComparatorOtherBackends>
backends_from_endpoints(const mxs::Target& main_target, const mxs::Endpoints& endpoints);

}
