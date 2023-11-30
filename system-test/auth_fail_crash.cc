/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

/**
 * @file bug572.cpp  regression case for bug 572 ( " If reading a user from users table fails, MaxScale fails"
 *)
 *
 * - try GRANT with wrong IP using all Maxscale services:
 *  + GRANT ALL PRIVILEGES ON *.* TO  'foo'@'*.foo.notexists' IDENTIFIED BY 'foo';
 *  + GRANT ALL PRIVILEGES ON *.* TO  'bar'@'127.0.0.*' IDENTIFIED BY 'bar'
 *  + DROP USER 'foo'@'*.foo.notexists'
 *  + DROP USER 'bar'@'127.0.0.*'
 * - do "select * from mysql.user" using RWSplit to check if Maxsclae crashed
 */


#include <iostream>
#include <unistd.h>
#include <maxtest/testconnections.hh>

using namespace std;

void create_drop_bad_user(MYSQL* conn, TestConnections* Test)
{

    Test->try_query(conn,
                    (char*)
                    "GRANT ALL PRIVILEGES ON *.* TO  'foo'@'*.foo.notexists' IDENTIFIED BY 'foo';");
    Test->try_query(conn, (char*) "GRANT ALL PRIVILEGES ON *.* TO  'bar'@'127.0.0.*' IDENTIFIED BY 'bar'");
    Test->try_query(conn, (char*) "DROP USER 'foo'@'*.foo.notexists'");
    Test->try_query(conn, (char*) "DROP USER 'bar'@'127.0.0.*'");
}

int main(int argc, char* argv[])
{
    TestConnections* Test = new TestConnections(argc, argv);

    Test->repl->connect();
    Test->maxscale->connect_maxscale();

    Test->tprintf("Trying GRANT for with bad IP: RWSplit\n");
    create_drop_bad_user(Test->maxscale->conn_rwsplit, Test);

    Test->tprintf("Trying SELECT to check if Maxscale hangs\n");
    Test->try_query(Test->maxscale->conn_rwsplit, (char*) "select * from mysql.user");

    int rval = Test->global_result;
    delete Test;
    return rval;
}
