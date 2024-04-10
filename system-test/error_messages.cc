/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

/**
 * Regression case for the bug "Different error messages from MariaDB and Maxscale"
 *
 * - try to connect to non existing DB directly to MariaDB server and via Maxscale
 * - compare error messages
 * - repeat for RWSplit, ReadConn
 */

#include <maxtest/testconnections.hh>
#include <iostream>
#include <algorithm>

using std::cout;
using std::endl;

std::string remove_host(std::string str)
{
    auto start = std::find(str.begin(), str.end(), '@');
    if (start != str.end())
    {
        start += 2;
        auto end = std::find(start, str.end(), '\'');
        if (end != str.end())
        {
            str.erase(start, end);
        }
    }

    return str;
}


bool is_equal_error(MYSQL* direct, MYSQL* conn)
{
    bool rval = true;
    std::string direct_err = remove_host(mysql_error(direct));
    std::string conn_err = remove_host(mysql_error(conn));

    if (direct_err != conn_err)
    {
        rval = false;
        cout << "Wrong error: `" << conn_err << "` (original: `" << direct_err << "`)" << endl;
    }

    return rval;
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    auto mxs_ip = test.maxscale->ip4();
    auto node_ip = test.repl->ip4(0);
    int node_port = test.repl->port(0);

    cout << "Non-existent database" << endl;
    test.repl->connect(0, "non_existing_db");
    test.maxscale->connect("non_existing_db");
    test.expect(is_equal_error(test.repl->nodes[0], test.maxscale->conn_rwsplit), "readwritesplit returned wrong error");
    test.expect(is_equal_error(test.repl->nodes[0], test.maxscale->conn_master), "readconnroute returned wrong error");
    test.repl->disconnect();
    test.maxscale->disconnect();

    cout << "Non-existent user" << endl;
    auto conn_direct = open_conn(node_port, node_ip, "not-a-user", "not-a-password", false);
    auto conn_rwsplit = open_conn(test.maxscale->rwsplit_port, mxs_ip, "not-a-user", "not-a-password", false);
    auto conn_rconn = open_conn(test.maxscale->rwsplit_port, mxs_ip, "not-a-user", "not-a-password", false);

    test.expect(is_equal_error(conn_direct, conn_rwsplit), "readwritesplit returned wrong error");
    test.expect(is_equal_error(conn_direct, conn_rconn), "readconnroute returned wrong error");

    mysql_close(conn_direct);
    mysql_close(conn_rwsplit);
    mysql_close(conn_rconn);

    cout << "Wrong password" << endl;
    conn_direct = open_conn(node_port, node_ip, "skysql", "not-a-password", false);
    conn_rwsplit = open_conn(test.maxscale->rwsplit_port, mxs_ip, "skysql", "not-a-password", false);
    conn_rconn = open_conn(test.maxscale->rwsplit_port, mxs_ip, "skysql", "not-a-password", false);

    test.expect(is_equal_error(conn_direct, conn_rwsplit), "readwritesplit returned wrong error");
    test.expect(is_equal_error(conn_direct, conn_rconn), "readconnroute returned wrong error");

    mysql_close(conn_direct);
    mysql_close(conn_rwsplit);
    mysql_close(conn_rconn);

    // Create a database and a user without access to it
    test.repl->connect();
    test.try_query(test.repl->nodes[0], "%s", "CREATE USER 'bob'@'%' IDENTIFIED BY 's3cret'");
    test.try_query(test.repl->nodes[0], "%s", "CREATE DATABASE error_messages");
    test.repl->sync_slaves();
    test.repl->disconnect();

    cout << "No permissions on database" << endl;
    conn_direct = open_conn_db(node_port, node_ip, "error_messages", "bob", "s3cret", false);
    conn_rwsplit = open_conn_db(test.maxscale->rwsplit_port, mxs_ip, "error_messages", "bob", "s3cret", false);
    conn_rconn = open_conn_db(test.maxscale->rwsplit_port, mxs_ip, "error_messages", "bob", "s3cret", false);

    test.expect(is_equal_error(conn_direct, conn_rwsplit), "readwritesplit returned wrong error");
    test.expect(is_equal_error(conn_direct, conn_rconn), "readconnroute returned wrong error");

    mysql_close(conn_direct);
    mysql_close(conn_rwsplit);
    mysql_close(conn_rconn);

    // Create a database and a user without access to it
    test.repl->connect();
    test.try_query(test.repl->nodes[0], "%s", "DROP USER 'bob'@'%'");
    test.try_query(test.repl->nodes[0], "%s", "DROP DATABASE error_messages");
    test.repl->disconnect();

    return test.global_result;
}
