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
 * @file routing_hints.cpp - Test routing hints
 * - execute a number of 'select @@server_id' with different hints and check that
 * query goes to backend according hint
 */


#include <iostream>
#include <maxtest/testconnections.hh>

#define SERVER1 0
#define SERVER2 1
#define SERVER3 2
#define SERVER4 3
#define NOT_MASTER -1

static struct result
{
    const char* query;
    int         reply;
} queries[] =
{
    {"select @@server_id; -- maxscale begin route to master",                       SERVER1},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; -- maxscale route to server server3",                     SERVER3},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; -- maxscale end",                                         NOT_MASTER},
    {"select @@server_id; -- maxscale named1 prepare route to master",              NOT_MASTER},
    {"select @@server_id; -- maxscale named1 begin",                                SERVER1},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; -- maxscale route to server server3",                     SERVER3},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; -- maxscale end",                                         NOT_MASTER},
    {"select @@server_id; -- maxscale shorthand1 begin route to server server2",    SERVER2},
    {"select @@server_id;",                                                         SERVER2},
    {"select @@server_id; -- maxscale route to server server3",                     SERVER3},
    {"select @@server_id;",                                                         SERVER2},
    {"select @@server_id; -- maxscale end",                                         NOT_MASTER},
    {"select @@server_id; # maxscale begin route to master",                        SERVER1},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; # maxscale route to server server3",                      SERVER3},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; # maxscale end",                                          SERVER2},
    {"select @@server_id; # maxscale named2 prepare route to master",               NOT_MASTER},
    {"select @@server_id; # maxscale named2 begin",                                 SERVER1},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; # maxscale route to server server3",                      SERVER3},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id; # maxscale end",                                          NOT_MASTER},
    {"select @@server_id; # maxscale shorthand2 begin route to server server2",     SERVER2},
    {"select @@server_id;",                                                         SERVER2},
    {"select @@server_id; # maxscale route to server server3",                      SERVER3},
    {"select @@server_id;",                                                         SERVER2},
    {"select @@server_id; # maxscale end",                                          NOT_MASTER},
    {"select @@server_id/* maxscale begin route to master */;",                     SERVER1},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id/* maxscale route to server server3 */;",                   SERVER3},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id/* maxscale end */;",                                       NOT_MASTER},
    {"select @@server_id/* maxscale named3 prepare route to master */;",            NOT_MASTER},
    {"select @@server_id/* maxscale named3 begin */;",                              SERVER1},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id/* maxscale route to server server3 */;",                   SERVER3},
    {"select @@server_id;",                                                         SERVER1},
    {"select @@server_id/* maxscale end */;",                                       NOT_MASTER},
    {"select @@server_id/* maxscale shorthand3 begin route to server server2 */; ", SERVER2},
    {"select @@server_id;",                                                         SERVER2},
    {"select @@server_id/* maxscale route to server server3 */;",                   SERVER3},
    {"select @@server_id;",                                                         SERVER2},
    {"select @@server_id/* maxscale end */;",                                       NOT_MASTER},
    {NULL,                                                                          SERVER1}
};

int main(int argc, char** argv)
{
    TestConnections* test = new TestConnections(argc, argv);
    test->repl->connect();
    test->maxscale->connect_maxscale();

    char server_id[test->repl->N][1024];

    /** Get server_id for each node */
    for (int i = 0; i < test->repl->N; i++)
    {
        sprintf(server_id[i], "%d", test->repl->get_server_id(i));
    }

    for (int i = 0; queries[i].query; i++)
    {
        char str[1024];
        find_field(test->maxscale->conn_rwsplit, queries[i].query, "@@server_id", str);
        if (queries[i].reply == NOT_MASTER)
        {
            test->expect(strcmp(server_id[0], str) != 0,
                         "%s: Query should not go to master.", queries[i].query);
        }
        else if (strcmp(server_id[queries[i].reply], str) != 0)
        {
            test->add_result(1,
                             "%s: Expected %s but got %s.\n",
                             queries[i].query,
                             server_id[queries[i].reply],
                             str);
        }
    }
    int rval = test->global_result;
    delete test;
    return rval;
}
