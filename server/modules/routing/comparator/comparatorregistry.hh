/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "comparatordefs.hh"
#include <shared_mutex>
#include <unordered_map>
#include "comparatorconfig.hh"

class ComparatorRegistry final
{
public:
    using Hash = ComparatorHash;
    using Ids = std::vector<int64_t>;

    ComparatorRegistry(const ComparatorRegistry&) = delete;
    ComparatorRegistry& operator=(const ComparatorRegistry&);

    ComparatorRegistry();
    ~ComparatorRegistry() = default;

    /**
     * Specify how many times a statement should be explained.
     *
     * @param nExplain_iterations  The number of times a particular kind
     *                             of statement should be explained.
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
     * @param hash  A hash obtained using @c hash_for().
     * @param id    The id of the query being executed.
     * @param pIds  On output, if @c true is returned, the ids of the
     *              queries that EXPLAIN the query identified with the hash.
     *
     * @return @c True if the statement identified by the hash has been
     *            sufficiently explained and thus need not be explained
     *            again. In that case @c *pIds contains the ids of the
     *            queries that EXPLAINed it.
     *            False otherwise, in which case @c *pIds in untouched.
     */
    bool is_explained(Hash hash, int64_t id, Ids* pIds);

private:
    using IdsByHash = std::unordered_map<Hash, Ids>;

    std::atomic<size_t>       m_nMax_entries { DEFAULT_ENTRIES };
    mutable std::shared_mutex m_explained_lock;
    mutable IdsByHash         m_explained;
    std::chrono::milliseconds m_period { 0 };
};
