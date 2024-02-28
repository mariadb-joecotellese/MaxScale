/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * MXS-1926: LOAD DATA LOCAL INFILE interrupted by server shutdown
 *
 * https://jira.mariadb.org/browse/MXS-1926
 */

#include <maxtest/testconnections.hh>
#include <stdlib.h>
#include <thread>
#include <fstream>
#include <chrono>
#include <atomic>

using namespace std::chrono;

typedef high_resolution_clock Clock;

std::atomic<int> ROWCOUNT {10000};

std::string create_tmpfile()
{
    char filename[] = "/tmp/data.csv.XXXXXX";
    int fd = mkstemp(filename);
    std::ofstream file(filename);
    close(fd);

    for (int i = 0; i < ROWCOUNT; i++)
    {
        file << "1, 2, 3, 4\n";
    }

    return filename;
}

void tune_rowcount(TestConnections& test)
{
    milliseconds dur {1};
    test.tprintf("Tuning data size so that an insert takes 10 seconds");
    test.maxscale->connect();
    test.try_query(test.maxscale->conn_rwsplit, "SET sql_log_bin=0");

    while (dur < seconds(10))
    {
        std::string filename = create_tmpfile();

        auto start = Clock::now();
        test.try_query(test.maxscale->conn_rwsplit,
                       "LOAD DATA LOCAL INFILE '%s' INTO TABLE test.t1",
                       filename.c_str());
        auto end = Clock::now();
        dur = duration_cast<milliseconds>(end - start);
        test.try_query(test.maxscale->conn_rwsplit, "TRUNCATE TABLE test.t1");

        remove(filename.c_str());

        int orig = ROWCOUNT;
        ROWCOUNT = orig / (dur.count() + 1) * 10000;
        test.tprintf("Loading %d rows took %ld ms, setting row count to %d",
                     orig,
                     dur.count(),
                     ROWCOUNT.load());
    }

    test.maxscale->disconnect();
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    test.repl->connect();

    // Create the table
    execute_query(test.repl->nodes[0], "CREATE OR REPLACE TABLE test.t1 (a INT, b INT, c INT, d INT)");
    test.repl->sync_slaves();

    // Tune the amount of data so that the loading will take approximately 15 seconds
    tune_rowcount(test);

    std::string filename = create_tmpfile();

    // Connect to MaxScale and load enough data so that we have
    test.maxscale->connect();

    // Disable replication of the LOAD DATA LOCAL INFILE
    test.try_query(test.maxscale->conn_rwsplit, "SET sql_log_bin=0");

    // This works around a limitation in 2.5 where non-participating connections must not process any queries
    // while the LOAD DATA LOCAL INFILE is in progress.

    test.tprintf("Loading %d rows of data while stopping a slave", ROWCOUNT.load());
    std::thread thr([&]() {
        std::this_thread::sleep_for(milliseconds(10));
        test.repl->stop_node(3);
        test.repl->start_node(3);
    });
    test.try_query(test.maxscale->conn_rwsplit,
                   "LOAD DATA LOCAL INFILE '%s' INTO TABLE test.t1",
                   filename.c_str());
    test.tprintf("Load complete");
    thr.join();

    test.maxscale->disconnect();

    // Cleanup
    execute_query(test.repl->nodes[0], "DROP TABLE test.t1");
    test.repl->sync_slaves();
    test.repl->disconnect();

    remove(filename.c_str());
    return test.global_result;
}
