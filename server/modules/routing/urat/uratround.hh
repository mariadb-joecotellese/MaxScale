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

    void clear()
    {
        for (auto kv : m_results)
        {
            kv.second.clear();
        }
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
    std::map<const UratBackend*, UratResult> m_results;
};
