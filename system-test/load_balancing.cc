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
 * @file load_balancing.cpp Checks how Maxscale balances load
 *
 * - also used for 'load_balancing_pers1' and 'load_balancing_pers10' tests (with 'persistpoolmax=1' and
 *'persistpoolmax=10' for all servers)
 *
 * - start two groups of threads: each group consists of 25 threads, each thread creates connections to
 * RWSplit,
 * threads from first group try to execute as many SELECTs as possible, from second group - one query per
 * second
 * - after 100 seconds all threads are stopped
 * - check number of connections to every slave: test PASSED if COM_SELECT difference between slaves is not
 * greater then 3 times and no
 * more then 10% of quesries went to Master
 */

#include <maxtest/big_load.hh>
#include <maxtest/testconnections.hh>

int main(int argc, char* argv[])
{

    TestConnections* Test = new TestConnections(argc, argv);
    Test->reset_timeout();
    long int q;
    int threads_num = 25;

    long int selects[256];
    long int inserts[256];
    long int new_selects[256];
    long int new_inserts[256];
    long int i1, i2;

    if (Test->smoke)
    {
        threads_num = 15;
    }
    Test->tprintf("Increasing connection and error limits on backend nodes.\n");
    Test->repl->connect();
    for (int i = 0; i < Test->repl->N; i++)
    {
        execute_query(Test->repl->nodes[i], (char*) "set global max_connections = 300;");
        execute_query(Test->repl->nodes[i], (char*) "set global max_connect_errors = 100000;");
    }
    Test->repl->close_connections();

    Test->tprintf("Creating query load with %d threads...\n", threads_num);
    Test->reset_timeout();
    load(&new_inserts[0],
         &new_selects[0],
         &selects[0],
         &inserts[0],
         threads_num,
         Test,
         &i1,
         &i2,
         1,
         false,
         true);

    long int avr = (i1 + i2 ) / (Test->repl->N);
    Test->tprintf("average number of quries per node %ld\n", avr);
    long int min_q = avr / 3;
    long int max_q = avr * 3;
    Test->tprintf("Acceplable value for every node from %ld until %ld\n", min_q, max_q);

    for (int i = 1; i < Test->repl->N; i++)
    {
        q = new_selects[i] - selects[i];
        if ((q > max_q) || (q < min_q))
        {
            Test->add_result(1, "number of queries for node %d is %ld\n", i + 1, q);
        }
    }

    if ((new_selects[0] - selects[0]) > avr / 3)
    {
        Test->add_result(1,
                         "number of queries for master greater then 30%% of averange number of queries per node\n");
    }

    Test->tprintf("Restoring nodes\n");
    Test->repl->connect();
    for (int i = 0; i < Test->repl->N; i++)
    {
        execute_query(Test->repl->nodes[i], (char*) "flush hosts;");
        execute_query(Test->repl->nodes[i], (char*) "set global max_connections = 151;");
    }
    Test->repl->close_connections();


    Test->check_maxscale_alive();

    int rval = Test->global_result;
    delete Test;
    return rval;
}
