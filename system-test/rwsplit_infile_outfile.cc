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
 * @file bug519.cpp - Jira task is MAX-345
 * - fill t1 with data
 * - execute SELECT * INTO OUTFILE '/tmp/t1.csv' FROM t1; against all maxscales->routers[0]
 * - DROP TABLE t1
 * - LOAD DATA LOCAL INFILE 't1.csv' INTO TABLE t1; using RWSplit
 * - check if t1 contains right data
 * - DROP t1 again and repeat LOAD DATA LOCAL INFILE 't1.csv' INTO TABLE t1; using ReadConn master
 */

/*
 *
 *  It seems that LOAD DATA LOCAL INFILE is not handled by readwritesplit? Maybe it's a bigger problem
 * elsewhere in MaxScale?
 *
 *  I can execute the command, it looks like it is getting sent to the master, but ... no data is actually
 * loaded. Does/can MaxScale handle LOAD DATA LOCAL INFILE?
 *  Comment 1 Kolbe Kegel 2014-09-03 02:39:47 UTC
 *  The LOAD DATA LOCAL INFILE statement is stuck in "Reading from net" until some timeout is hit:
 *
 | 22 | maxuser     | 192.168.30.38:59996 | test | Query   |   10 | Reading from net   | load data local
 |infile '/Users/kolbe/Devel/seattleparking/Street_Parking_Signs.csv' into table parki |    0.000 |
 |
 |  The client never sees the statement end, though, even after the server has long ago killed its
 |connection...
 |
 |  When I start a *new* connection to MaxScale and I try to execute the LOAD DATA LOCAL INFILE statement
 |again, I have some problems:
 |
 |  mysql 5.5.38-MariaDB (maxuser) [test]> source /Users/kolbe/Devel/seattleparking/loaddata.sql
 |  ERROR 2013 (HY000) at line 1 in file: '/Users/kolbe/Devel/seattleparking/loaddata.sql': Lost connection to
 |MySQL server during query
 |  mysql 5.5.38-MariaDB (maxuser) [test]> select @@wsrep_node_address;
 |  ERROR 2006 (HY000): MySQL server has gone away
 |  No connection. Trying to reconnect...
 |  Connection id:    1709
 |  Current database: test
 |
 +----------------------+
 | @@wsrep_node_address |
 +----------------------+
 | 192.168.30.32        |
 +----------------------+
 |  1 row in set (0.01 sec)
 |
 |  mysql 5.5.38-MariaDB (maxuser) [test]> source /Users/kolbe/Devel/seattleparking/loaddata.sql
 |  ERROR 2013 (HY000) at line 1 in file: '/Users/kolbe/Devel/seattleparking/loaddata.sql': Lost connection to
 |MySQL server during query
 |  mysql 5.5.38-MariaDB (maxuser) [test]> select @@wsrep_node_address;
 |  ERROR 2006 (HY000): MySQL server has gone away
 |  No connection. Trying to reconnect...
 |  Connection id:    1709
 |  Current database: test
 |
 +----------------------+
 | @@wsrep_node_address |
 +----------------------+
 | 192.168.30.32        |
 +----------------------+
 |  1 row in set (0.01 sec)
 |
 |  mysql 5.5.38-MariaDB (maxuser) [test]> source /Users/kolbe/Devel/seattleparking/loaddata.sql
 |  ERROR 2013 (HY000) at line 1 in file: '/Users/kolbe/Devel/seattleparking/loaddata.sql': Lost connection to
 |MySQL server during query
 |  mysql 5.5.38-MariaDB (maxuser) [test]> Bye
 */


#include <iostream>
#include <maxtest/testconnections.hh>
#include <maxtest/sql_t1.hh>

using namespace std;

int main(int argc, char* argv[])
{
    TestConnections* Test = new TestConnections(argc, argv);
    int N = 4;
    int iterations = 2;
    if (Test->smoke)
    {
        iterations = 1;
    }
    char str[1024];
    Test->reset_timeout();

    MYSQL*& rc_master = Test->maxscale->conn_master;
    Test->maxscale->connect_maxscale();
    Test->repl->connect();

    Test->tprintf("Create t1\n");
    create_t1(Test->maxscale->conn_rwsplit);
    Test->tprintf("Insert data into t1\n");
    Test->reset_timeout();
    insert_into_t1(Test->maxscale->conn_rwsplit, N);
    Test->repl->sync_slaves();
    Test->reset_timeout();

    auto sudo = Test->repl->access_sudo(0);
    sprintf(str, "%s rm -f /tmp/t*.csv; %s chmod 777 /tmp", sudo, sudo);
    Test->tprintf("%s\n", str);
    for (int k = 0; k < Test->repl->N; k++)
    {
        Test->repl->ssh_node(k, str, false);
    }
    // system(str);

    Test->tprintf("Copying data from t1 to file...\n");
    Test->tprintf("using RWSplit: SELECT * INTO OUTFILE '/tmp/t1.csv' FROM t1;\n");
    Test->try_query(Test->maxscale->conn_rwsplit, (char*) "SELECT * INTO OUTFILE '/tmp/t1.csv' FROM t1;");
    Test->tprintf("using ReadConn master: SELECT * INTO OUTFILE '/tmp/t2.csv' FROM t1;\n");
    Test->try_query(rc_master, (char*) "SELECT * INTO OUTFILE '/tmp/t2.csv' FROM t1;");
    Test->tprintf("using ReadConn slave: SELECT * INTO OUTFILE '/tmp/t3.csv' FROM t1;\n");
    Test->try_query(Test->maxscale->conn_slave, (char*) "SELECT * INTO OUTFILE '/tmp/t3.csv' FROM t1;");

    Test->tprintf("Copying t1.cvs from Maxscale machine:\n");
    Test->repl->copy_from_node(0, "/tmp/t1.csv", "./t1.csv");

    MYSQL* srv[2];

    srv[0] = Test->maxscale->conn_rwsplit;
    srv[1] = rc_master;
    for (int i = 0; i < iterations; i++)
    {
        Test->reset_timeout();
        Test->tprintf("Dropping t1 \n");
        Test->try_query(Test->maxscale->conn_rwsplit, (char*) "DROP TABLE t1;");
        Test->repl->sync_slaves();

        Test->reset_timeout();
        Test->tprintf("Create t1\n");
        create_t1(Test->maxscale->conn_rwsplit);
        Test->tprintf("Loading data to t1 from file\n");
        Test->try_query(srv[i], (char*) "LOAD DATA LOCAL INFILE 't1.csv' INTO TABLE t1;");
        Test->repl->sync_slaves();

        Test->reset_timeout();
        Test->tprintf("SELECT: rwsplitter\n");
        Test->add_result(select_from_t1(Test->maxscale->conn_rwsplit, N), "Wrong data in 't1'");
        Test->tprintf("SELECT: master\n");
        Test->add_result(select_from_t1(rc_master, N), "Wrong data in 't1'");
        Test->tprintf("SELECT: slave\n");
        Test->add_result(select_from_t1(Test->maxscale->conn_slave, N), "Wrong data in 't1'");
    }

    Test->repl->close_connections();
    Test->check_maxscale_alive();

    int rval = Test->global_result;
    delete Test;
    return rval;
}
