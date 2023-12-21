/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "comparatordefs.hh"
#include <map>
#include "comparatorresult.hh"

/**
 * @class ComparatorRound
 *
 * The results of executing one particular statement on different backends.
 */
class ComparatorRound final
{
public:
    using Results = std::map<const ComparatorBackend*, ComparatorResult>;

    ComparatorRound(const std::string& query, uint8_t command, const ComparatorBackend* pBackend)
        : m_query(query)
        , m_command(command)
    {
        add_backend(pBackend);
    }

    bool ready() const
    {
        bool rv = true;

        for (auto kv : m_results)
        {
            if (!kv.second.closed())
            {
                rv = false;
            }
        }

        return rv;
    }

    void add_backend(const ComparatorBackend* pBackend)
    {
        mxb_assert(m_results.find(pBackend) == m_results.end());

        m_results.emplace(pBackend, ComparatorResult {});
    }

    void remove_backend(const ComparatorBackend* pBackend)
    {
        auto it = m_results.find(pBackend);
        mxb_assert(it != m_results.end());
        m_results.erase(it);
    }

    void clear()
    {
        m_query.clear();
        m_command = 0;

        for (auto kv : m_results)
        {
            kv.second.reset();
        }
    }

    const std::string& query() const
    {
        return m_query;
    }

    uint8_t command() const
    {
        return m_command;
    }

    const Results& results() const
    {
        return m_results;
    }

    const ComparatorResult* get_result(const ComparatorBackend* pBackend) const
    {
        auto it = m_results.find(pBackend);

        return it != m_results.end() ? &it->second : nullptr;
    }

    void set_result(const ComparatorBackend* pBackend, const ComparatorResult& result)
    {
        m_results[pBackend] = result;
    }

private:
    std::string                                          m_query;
    uint8_t                                              m_command { 0 };
    std::map<const ComparatorBackend*, ComparatorResult> m_results;
};
