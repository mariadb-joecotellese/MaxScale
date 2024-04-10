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

#include <maxbase/ccdefs.hh>

/**
 * Pre 4.7 GCC doesn't support the __atomic builtin functions. The older __sync
 * builtins don't have proper store/load functionality so we use a somewhat ugly
 * hack to emulate the store/load.
 */
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 7)
#ifndef MXB_USE_ATOMIC_BUILTINS
#define MXB_USE_ATOMIC_BUILTINS 1
#endif
#endif

namespace maxbase
{

namespace atomic
{

constexpr int RELAXED = __ATOMIC_RELAXED;
constexpr int CONSUME = __ATOMIC_CONSUME;
constexpr int ACQUIRE = __ATOMIC_ACQUIRE;
constexpr int RELEASE = __ATOMIC_RELEASE;
constexpr int ACQ_REL = __ATOMIC_ACQ_REL;
constexpr int SEQ_CST = __ATOMIC_SEQ_CST;

/**
 * Perform atomic fetch-and-add operation
 *
 * @param t    Variable to add to
 * @param v    Value to add
 * @param mode Memory ordering
 *
 * @return The old value
 */
template<class T, class R>
T add(T* t, R v, int mode = SEQ_CST)
{
    return __atomic_fetch_add(t, v, mode);
}

/**
 * Perform atomic load operation
 *
 * @param t    Variable to load
 * @param mode Memory ordering
 *
 * @return The loaded value
 */
template<class T>
T load(T* t, int mode = SEQ_CST)
{
    return __atomic_load_n(t, mode);
}

/**
 * Perform atomic store operation
 *
 * @param t    Variable where the value is stored
 * @param v    Value to store
 * @param mode Memory ordering
 *
 * @return The loaded value
 */
template<class T, class R>
void store(T* t, R v, int mode = SEQ_CST)
{
    __atomic_store_n(t, v, mode);
}

/**
 * Perform atomic compare and exchange operation
 *
 * @param ptr           Variable where the value is stored
 * @param expected      Expected value of the variable
 * @param desired       The desired new value of the variable
 * @param success_model On success, this memory model is used
 * @param fail_model    On failure, this memory model is used
 *
 * @return True if value was exchanged, false if exchange failed
 */
template<class T>
bool compare_exchange(T* ptr, T* expected, T desired, int success_model = ACQ_REL, int fail_model = ACQUIRE)
{
    return __atomic_compare_exchange_n(ptr, expected, desired, true, success_model, fail_model);
}

}
}
