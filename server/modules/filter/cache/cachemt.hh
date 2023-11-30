/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxscale/ccdefs.hh>

#include <mutex>

#include "cachesimple.hh"

class CacheMT : public CacheSimple
{
public:
    ~CacheMT();

    static CacheMT* create(const std::string& name,
                           const CacheRules::SVector& sRules,
                           const CacheConfig* pConfig);

    json_t* get_info(uint32_t what) const override final;

    bool must_refresh(const CacheKey& key, const CacheFilterSession* pSession) override final;

    void refreshed(const CacheKey& key, const CacheFilterSession* pSession) override final;

    CacheRules::SVector all_rules() const override final;

    void set_all_rules(const CacheRules::SVector& sRules) override final;

private:
    CacheMT(const std::string& name,
            const CacheConfig* pConfig,
            const CacheRules::SVector& sRules,
            SStorageFactory sFactory,
            Storage* pStorage);

    static CacheMT* create(const std::string& name,
                           const CacheConfig* pConfig,
                           const CacheRules::SVector& sRules,
                           SStorageFactory sFactory);

private:
    CacheMT(const CacheMT&);
    CacheMT& operator=(const CacheMT&);

private:
    mutable std::mutex m_lock_pending;      // Lock used for protecting 'pending'.
    mutable std::mutex m_lock_rules;
};
