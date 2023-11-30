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

#include <maxbase/ccdefs.hh>

#include <pthread.h>
#include <shared_mutex>

namespace maxbase
{

// Minimal implementation of std::shared_mutex from C++17 without the trylock methods
class shared_mutex
{
public:
    shared_mutex(const shared_mutex&) = delete;
    shared_mutex& operator=(const shared_mutex&) = delete;
    shared_mutex() = default;

    ~shared_mutex()
    {
        pthread_rwlock_destroy(&m_lock);
    }

    void lock()
    {
        pthread_rwlock_wrlock(&m_lock);
    }

    void unlock()
    {
        pthread_rwlock_unlock(&m_lock);
    }

    void lock_shared()
    {
        pthread_rwlock_rdlock(&m_lock);
    }

    void unlock_shared()
    {
        pthread_rwlock_unlock(&m_lock);
    }

private:
    pthread_rwlock_t m_lock = PTHREAD_RWLOCK_INITIALIZER;
};
}
