/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxscale/ccdefs.hh>
#include "testerstorage.hh"


class TesterRawStorage : public TesterStorage
{
public:
    /**
     * Constructor
     *
     * @param pOut      Pointer to the stream to be used for (user) output.
     * @param pFactory  Pointer to factory to be used.
     */
    TesterRawStorage(std::ostream* pOut, StorageFactory* pFactory);

    /**
     * @see TesterStorage::run
     */
    int execute(size_t n_threads, size_t n_seconds, const CacheItems& cache_items) override;

    /**
     * @see TesterStorage::get_storage
     */
    Storage* get_storage(const Storage::Config& config) const override;

private:
    TesterRawStorage(const TesterRawStorage&);
    TesterRawStorage& operator=(const TesterRawStorage&);
};
