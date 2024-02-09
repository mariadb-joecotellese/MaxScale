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

/**
 * @file different_size_rwsplit.cpp Tries INSERTs with size close to 0x0ffffff * N
 * - executes inserts with size: from 0x0ffffff * N - X up to 0x0ffffff * N - X
 * (N = 3, X = 50 or 20 for 'soke' test)
 * - check if Maxscale is still alive
 */


#include <iostream>
#include <maxtest/different_size.hh>
#include <maxtest/testconnections.hh>

using namespace std;

int main(int argc, char* argv[])
{
    TestConnections* Test = new TestConnections(argc, argv);

    different_packet_size(Test, false);

    Test->reset_timeout();
    Test->repl->sync_slaves();
    Test->check_maxscale_alive();
    int rval = Test->global_result;
    delete Test;
    return rval;
}
