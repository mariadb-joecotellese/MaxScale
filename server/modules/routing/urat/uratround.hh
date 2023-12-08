/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "uratdefs.hh"
#include <map>
#include "uratresult.hh"

/**
 * @class UratRound
 *
 * The results of executing one particular statement on different backends.
 */
class UratRound final
{
public:
    using Results = std::map<const UratBackend*, UratResult>;

    UratRound()
    {
    }

    UratRound(const std::string& query, uint8_t command)
        : m_query(query)
        , m_command(command)
    {
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

    const UratResult& get_result(const UratBackend* pBackend)
    {
        return m_results[pBackend];
    }

    void set_result(const UratBackend* pBackend, const UratResult& result)
    {
        m_results[pBackend] = result;
    }

private:
    std::string                              m_query;
    uint8_t                                  m_command { 0 };
    std::map<const UratBackend*, UratResult> m_results;
};
