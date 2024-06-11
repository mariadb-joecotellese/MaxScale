/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2029-02-28
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxscale/ccdefs.hh>
#include <maxbase/lru_cache.hh>
#include "cachefilter.hh"
#include "cache_storage_api.hh"
#include "storage.hh"

class LRUStorage : public Storage
{
public:
    ~LRUStorage();

    /**
     * @see Storage::create_token
     *
     * @return Always NULL.
     */
    bool create_token(std::shared_ptr<Token>* psToken) override final;

    /**
     * @see Storage::get_config
     */
    void get_config(Config* pConfig) override final;

    /**
     * @see Storage::get_limits
     */
    void get_limits(Limits* pLimits) override final;

protected:
    LRUStorage(const Config& config, Storage* pStorage);

    /**
     * @see Storage::get_info
     */
    cache_result_t do_get_info(uint32_t what, json_t** ppInfo) const;

    /**
     * @see Storage::get_value
     */
    cache_result_t do_get_value(Token* pToken,
                                const CacheKey& key,
                                uint32_t flags,
                                uint32_t soft_ttl,
                                uint32_t hard_ttl,
                                GWBUF* pValue);

    /**
     * @see Storage::put_value
     */
    cache_result_t do_put_value(Token* pToken,
                                const CacheKey& key,
                                const std::vector<std::string>& invalidation_words,
                                const GWBUF& value);

    /**
     * @see Storage::del_value
     */
    cache_result_t do_del_value(Token* pToken,
                                const CacheKey& key);

    /**
     * @see Storage::invalidate
     */
    cache_result_t do_invalidate(Token* pToken,
                                 const std::vector<std::string>& words);

    /**
     * @see Storage::clear
     */
    cache_result_t do_clear(Token* pToken);

    /**
     * @see Storage::get_head
     */
    cache_result_t do_get_head(CacheKey* pKey, GWBUF* pValue);

    /**
     * @see Storage::get_tail
     */
    cache_result_t do_get_tail(CacheKey* pKey, GWBUF* ppValue);

    /**
     * @see Storage::getSize
     */
    cache_result_t do_get_size(uint64_t* pSize) const;

    /**
     * @see Storage::getItems
     */
    cache_result_t do_get_items(uint64_t* pItems) const;

private:
    LRUStorage(const LRUStorage&);
    LRUStorage& operator=(const LRUStorage&);

    enum access_approach_t
    {
        APPROACH_GET,   // Update head
        APPROACH_PEEK   // Do not update head
    };

    cache_result_t access_value(access_approach_t approach,
                                const CacheKey& key,
                                uint32_t flags,
                                uint32_t soft_ttl,
                                uint32_t hard_ttl,
                                GWBUF* pValue);

    cache_result_t peek_value(const CacheKey& key,
                              uint32_t flags,
                              GWBUF* pValue)
    {
        return access_value(APPROACH_PEEK, key, flags, CACHE_USE_CONFIG_TTL, CACHE_USE_CONFIG_TTL, pValue);
    }

    /**
     * The Node class is used for maintaining LRU information.
     */
    class Node
    {
    public:
        Node()
            : m_pKey(NULL)
            , m_size(0)
        {
        }

        const CacheKey* key() const
        {
            return m_pKey;
        }

        size_t size() const
        {
            return m_size;
        }

        const std::vector<std::string>& invalidation_words() const
        {
            return m_invalidation_words;
        }

        void reset(const CacheKey* pKey,
                   size_t size,
                   const std::vector<std::string>& invalidation_words)
        {
            m_pKey = pKey;
            m_size = size;
            m_invalidation_words = invalidation_words;
        }

        void clear()
        {
            m_pKey = nullptr;
            m_size = 0;
            m_invalidation_words.clear();
        }

    private:
        // TODO: Replace string with char* that points to a shared string.
        // TODO: No sense in storing the same table name a million times.
        using Words = std::vector<std::string>;

        const CacheKey* m_pKey;                 /*< Points at the key stored in nodes_by_key below. */
        size_t          m_size;                 /*< The size of the data referred to by m_pKey. */
        Words           m_invalidation_words;   /*< Words that invalidate this node. */
    };

    typedef mxb::lru_cache<CacheKey, Node*> NodesByKey;

    enum class Context
    {
        EVICTION,        /*< Evict (aka free) LRU node and cache value. */
        INVALIDATION,    /*< Invalidate (aka free) LRU node and cache value. */
        LRU_INVALIDATION /*< Invalidate (aka free) LRU node, but leave cache value. */
    };

    void vacate_lru();
    void vacate_lru(size_t space);
    bool  free_node_data(Node* pNode, Context context);

    enum class InvalidatorAction
    {
        IGNORE, // Ignore the invalidator, just free the node.
        REMOVE, // Free the node and remove it from the invalidator.
    };

    void free_node(Node* pNode, InvalidatorAction action) const;
    void free_node(NodesByKey::iterator& i, InvalidatorAction action) const;

    cache_result_t get_existing_node(NodesByKey::iterator& i, const GWBUF& value);
    cache_result_t get_new_node(const CacheKey& key,
                                const GWBUF& value,
                                NodesByKey::iterator* pI);

    bool invalidate(Node* pNode, Context context);

    class Invalidator;
    class NullInvalidator;
    class LRUInvalidator;
    class FullInvalidator;
    class StorageInvalidator;

    Storage* storage() const
    {
        return m_pStorage;
    }

private:
    struct Stats
    {
        void fill(json_t* pObject) const;

        uint64_t size = 0;          /*< The total size of the stored values. */
        uint64_t items = 0;         /*< The number of stored items. */
        uint64_t hits = 0;          /*< How many times a key was found in the cache. */
        uint64_t misses = 0;        /*< How many times a key was not found in the cache. */
        uint64_t updates = 0;       /*< How many times an existing key in the cache was updated. */
        uint64_t deletes = 0;       /*< How many times an existing key in the cache was deleted. */
        uint64_t evictions = 0;     /*< How many times an item has been evicted from the cache. */
        uint64_t invalidations = 0; /*< How many times an item has been invalidated. */
        uint64_t cleared = 0;       /*< How many times the cache has been cleared. */
    };

    using SInvalidator = std::unique_ptr<Invalidator>;

    const Config         m_config;          /*< The configuration. */
    Storage*             m_pStorage;        /*< The actual storage. */
    const uint64_t       m_max_count;       /*< The maximum number of items in the LRU list, */
    const uint64_t       m_max_size;        /*< The maximum size of all cached items. */
    mutable Stats        m_stats;           /*< Cache statistics. */
    mutable NodesByKey   m_nodes_by_key;    /*< Mapping from cache keys to corresponding Node. */
    mutable SInvalidator m_sInvalidator;    /*< The invalidator. */
};
