/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

#include <maxscale/ccdefs.hh>
#include <iostream>
#include <maxscale/paths.hh>
#include "teststorage.hh"
#include "testerlrustorage.hh"
#include "../../../../core/test/test_utils.hh"

using namespace std;

namespace
{

class TestLRUStorage : public TestStorage
{
public:
    TestLRUStorage(std::ostream* pOut)
        : TestStorage(pOut)
    {
    }

private:
    int execute(StorageFactory& factory,
                size_t threads,
                size_t seconds,
                size_t items,
                size_t min_size,
                size_t max_size) override
    {
        TesterLRUStorage tester(&out(), &factory);

        return tester.run(threads, seconds, items, min_size, max_size);
    }
};
}

int main(int argc, char* argv[])
{
    init_test_env();

    TestLRUStorage test(&cout);
    int rv = test.run(argc, argv);

    return rv;
}
