/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "comparatorregistry.hh"
#include <maxbase/checksum.hh>

namespace
{

struct ThisUnit
{
    mxb::xxHasher hash;
} this_unit;

}

CRegistry::CRegistry()
{
}

//static
CRegistry::Hash CRegistry::hash_for(std::string_view canonical_sql)
{
    return this_unit.hash(canonical_sql);
}

bool CRegistry::is_explained(mxb::TimePoint now, Hash hash, int64_t id, Entries* pEntries)
{
    bool rv = false;

    std::shared_lock shared_lock(m_entries_lock);

    auto it = m_entries.find(hash);

    if (it != m_entries.end())
    {
        if (!needs_updating(now, it->second))
        {
            *pEntries = it->second;
            rv = true;
        }
    }

    shared_lock.unlock();

    if (!rv)
    {
        // More EXPLAINs needed, lock mutex, this time for update.
        std::unique_lock unique_lock(m_entries_lock);

        // Look up again, because there was room for someone else between
        // the shared lock being unlocked and the unique lock being locked
        // to do something.
        it = m_entries.find(hash);

        if (it == m_entries.end())
        {
            Entries& entries = m_entries[hash];
            entries.reserve(max_entries());
            entries.push_back({now, id});
        }
        else
        {
            if (!needs_updating(now, it->second))
            {
                *pEntries = it->second;
                rv = true;
            }
            else
            {
                *pEntries = it->second;

                // Add the id, following the assumption that the caller
                // will now do the EXPLAIN. This means that in the output,
                // a query may refer (for the EXPLAIN result) to a query
                // appearing after that first query. Seems better than to
                // register the execution of the EXPLAIN only after it has
                // been performed, as that may lead to a thundering herd kind
                // of effect.
                it->second.push_back({now, id});

                if (it->second.size() == max_entries())
                {
                    // Final EXPLAIN, ensure the vector is exactly sized.
                    it->second.shrink_to_fit();
                }
            }
        }
    }

    return rv;
}


bool CRegistry::needs_updating(mxb::TimePoint now, std::vector<Entry>& entries)
{
    bool rv = false;

    auto threshold = now - period();

    auto nToo_old = 0;
    for (const auto& entry : entries)
    {
        if (entry.when <= threshold)
        {
            ++nToo_old;
        }
        else
        {
            break;
        }
    }

    if (nToo_old > 0)
    {
        std::move(entries.begin() + nToo_old, entries.end(), entries.begin());
        entries.resize(entries.size() - nToo_old);
    }

    return entries.size() < max_entries();
}
