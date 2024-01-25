/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "comparatorexplainregistry.hh"
#include <maxbase/checksum.hh>

namespace
{

struct ThisUnit
{
    mxb::xxHasher hash;
} this_unit;

}

ComparatorExplainRegistry::ComparatorExplainRegistry()
{
}

//static
ComparatorExplainRegistry::Hash ComparatorExplainRegistry::hash_for(std::string_view canonical_sql)
{
    return this_unit.hash(canonical_sql);
}

bool ComparatorExplainRegistry::is_explained(Hash hash, int64_t id, Ids* pIds)
{
    bool rv = false;

    size_t nExplain_iterations = 0;

    std::shared_lock shared_lock(m_explained_lock);

    auto it = m_explained.find(hash);

    if (it != m_explained.end())
    {
        nExplain_iterations = it->second.size();

        if (nExplain_iterations >= m_nExplain_iterations)
        {
            *pIds = it->second;
            rv = true;
        }
    }

    shared_lock.unlock();

    if (!rv)
    {
        // More EXPLAINs needed, lock mutex, this time for update.
        std::unique_lock unique_lock(m_explained_lock);

        // Look up again, because there was room for someone else between
        // the shared lock being unlocked and the unique lock being locked
        // to do something.
        it = m_explained.find(hash);

        if (it == m_explained.end())
        {
            Ids& ids = m_explained[hash];
            ids.reserve(m_nExplain_iterations);
            ids.push_back(id);
        }
        else
        {
            nExplain_iterations = it->second.size();

            if (nExplain_iterations >= m_nExplain_iterations)
            {
                *pIds = it->second;
                rv = true;
            }
            else
            {
                // Add the id, following the assumption that the caller
                // will now do the EXPLAIN. This means that in the output,
                // a query may refer (for the EXPLAIN result) to a query
                // appearing after that first query. Seems better than to
                // register the execution of the EXPLAIN only after it has
                // been performed, as that may lead to a thundering herd kind
                // of effect.
                it->second.push_back(id);

                if (nExplain_iterations + 1 == m_nExplain_iterations)
                {
                    // Final EXPLAIN, ensure the vector is exactly sized.
                    it->second.shrink_to_fit();
                }
            }
        }
    }

    return rv;
}



