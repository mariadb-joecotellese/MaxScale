/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "comparatordefs.hh"
#include <shared_mutex>
#include <unordered_map>

class ComparatorExplainRegistry final
{
public:
    static const int DEFAULT_EXPLANATIONS = 5;

    using Ids = std::vector<int64_t>;

    ComparatorExplainRegistry(const ComparatorExplainRegistry&) = delete;
    ComparatorExplainRegistry& operator=(const ComparatorExplainRegistry&);

    ComparatorExplainRegistry();
    ~ComparatorExplainRegistry() = default;

    /**
     * Specify how many times a statement should be explained.
     *
     * @param nExplanations  The number of times a particular kind
     *                       of statement should be explained.
     */
    void set_required_explanations(size_t nExplanations)
    {
        m_nExplanations = nExplanations;
    }

    /**
     * @return The number of times a problematic statement should be explained.
     */
    size_t required_explanations() const
    {
        return m_nExplanations;
    }

    /**
     * @param canonical_sql  The canonical version of a statement.
     *
     * @return The hash used when book-keeping that statement.
     */
    static std::string hash_for(std::string_view canonical_sql);

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
    bool is_explained(const std::string& hash, int64_t id, Ids* pIds);

private:
    using IdsByHash = std::unordered_map<std::string, Ids>;

    std::atomic<size_t>       m_nExplanations { DEFAULT_EXPLANATIONS };
    mutable std::shared_mutex m_explained_lock;
    mutable IdsByHash         m_explained;
};
