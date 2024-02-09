/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#include <maxscale/threadpool.hh>

namespace
{

struct ThisUnit
{
    mxb::ThreadPool thread_pool;
};

ThisUnit this_unit;

}

namespace maxscale
{

mxb::ThreadPool& thread_pool()
{
    return this_unit.thread_pool;
}

}
