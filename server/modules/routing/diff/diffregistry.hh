/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once
#include "diffdefs.hh"
#include <shared_mutex>
#include <unordered_map>
#include "diffconfig.hh"

class DiffRegistry final
{
public:
    struct Entry
    {
        mxb::TimePoint when;
        int64_t        id {0};
    };

    using Entries = std::vector<Entry>;
    using Hash = CHash;

    DiffRegistry(const DiffRegistry&) = delete;
    DiffRegistry& operator=(const DiffRegistry&);

    DiffRegistry();
    ~DiffRegistry() = default;

    /**
     * Specify how many times a statement should be explained.
     *
     * @param nMax_entries  The number of times a particular kind
     *                      of statement should be explained.
     */
    void set_max_entries(size_t nMax_entries)
    {
        m_nMax_entries = nMax_entries;
    }

    /**
     * @return The number of times a problematic statement should be explained.
     */
    size_t max_entries() const
    {
        return m_nMax_entries;
    }

    /**
     * Specify the period over which max entries is applied.
     *
     * @param period The period.
     */
    void set_period(std::chrono::milliseconds period)
    {
        m_period = period;
    }

    /**
     * @return The current period.
     */
    std::chrono::milliseconds period() const
    {
        return m_period;
    }

    /**
     * @param canonical_sql  The canonical version of a statement.
     *
     * @return The hash used when book-keeping that statement.
     */
    static Hash hash_for(std::string_view canonical_sql);

    /**
     * Has an SQL statement identified using @c hash been explained.
     *
     * @param hash      A hash obtained using @c hash_for().
     * @param id        The id of the query being executed.
     * @param pEntries  On output, if @c true is returned, the ids of the
     *                  queries that EXPLAIN the query identified with the hash.
     *
     * @return @c True if the statement identified by the hash has been
     *            sufficiently explained and thus need not be explained
     *            again. In that case @c *pEntries contains the ids of the
     *            queries that EXPLAINed it.
     *            False otherwise, in which case @c *pIds in untouched.
     */
    bool is_explained(mxb::TimePoint now, Hash hash, int64_t id, Entries* pEntries);

private:
    bool needs_updating(mxb::TimePoint now, std::vector<Entry>& entries);

    using EntriesByHash = std::unordered_map<Hash, Entries>;

    std::atomic<size_t>       m_nMax_entries;
    std::chrono::milliseconds m_period { 0 };
    mutable std::shared_mutex m_entries_lock;
    mutable EntriesByHash     m_entries;
};
