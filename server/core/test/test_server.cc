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

// To ensure that ss_info_assert asserts also when builing in non-debug mode.
#if !defined (SS_DEBUG)
#define SS_DEBUG
#endif
#if defined (NDEBUG)
#undef NDEBUG
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <maxscale/server.hh>
#include <maxscale/paths.hh>
#include <maxscale/config.hh>

#include "../internal/config.hh"
#include "../internal/server.hh"
#include "../internal/servermanager.hh"
#include "../internal/config_runtime.hh"
#include "test_utils.hh"

static mxs::ConfigParameters params;

/**
 * test1    Allocate a server and do lots of other things
 *
 */
static int test1()
{
    std::string status;

    /* Server tests */
    fprintf(stderr, "testserver : creating server called MyServer");
    Server* server = ServerManager::create_server("uniquename", params);
    mxb_assert_message(server, "Allocating the server should not fail");

    fprintf(stderr, "\t..done\nTesting Unique Name for Server.");
    mxb_assert_message(NULL == ServerManager::find_by_unique_name("non-existent"),
                       "Should not find non-existent unique name.");
    mxb_assert_message(server == ServerManager::find_by_unique_name("uniquename"),
                       "Should find by unique name.");
    fprintf(stderr, "\t..done\nTesting Status Setting for Server.");
    // The tests don't expect this status. It's a transient status anyways so there's no real point
    // in testing it.
    server->clear_status(SERVER_NEED_DNS);
    status = server->status_string();
    mxb_assert_message(status == "Down", "Status of Server should be Down.");
    server->set_status(SERVER_RUNNING | SERVER_MASTER);
    status = server->status_string();
    mxb_assert_message(status == "Master, Running", "Should find correct status.");
    server->clear_status(SERVER_MASTER);
    status = server->status_string();
    mxb_assert_message(status == "Running",
                       "Status of Server should be Running after master status cleared.");
    fprintf(stderr, "\t..done\nFreeing Server.");
    server->deactivate();
    fprintf(stderr, "\t..done\n");
    return 0;
}

#define TEST(A, B) do {if (!(A)) {printf(B "\n"); return false;}} while (false)

bool test_load_config(const char* input, Server* server)
{
    ConfigSectionMap ccontext;
    auto load_res = mxb::ini::parse_config_file_to_map(input);
    if (config_add_to_context(input,
                              ConfigSection::SourceType::ADDITIONAL, load_res.config, ccontext))
    {
        auto& obj = ccontext.begin()->second;
        auto& params2 = obj.m_parameters;

        TEST(obj.m_name == server->name(), "Server names differ");
        TEST(params2.get_string("address") == server->address(), "Server addresses differ");
        Server* new_server = ServerManager::create_server(obj.name(), obj.m_parameters);
        TEST(new_server, "Failed to create server from loaded config");
        TEST(new_server->port() == server->port(), "Server ports differ");
    }

    return true;
}

bool test_serialize()
{
    char name[] = "serialized-server";
    char config_name[] = "serialized-server.cnf";
    char old_config_name[] = "serialized-server.cnf.old";
    mxs::set_config_persistdir("./");
    Server* server = ServerManager::create_server(name, params);
    TEST(server, "Server allocation failed");

    /** Make sure the files don't exist */
    unlink(config_name);
    unlink(old_config_name);

    /** Serialize server to disk */
    std::ostringstream ss;
    server->persist(ss);
    TEST(runtime_save_config(server->name(), ss.str()), "Failed to synchronize original server");

    // Deactivate the server to prevent port conflicts
    server->deactivate();

    /** Load it again */
    TEST(test_load_config(config_name, server), "Failed to load the serialized server");

    /** We should have two identical servers */
    Server* created = ServerManager::find_by_unique_name(name);

    rename(config_name, old_config_name);

    ss.str("");
    created->persist(ss);

    /** Serialize the loaded server to disk */
    TEST(runtime_save_config(created->name(), ss.str()), "Failed to synchronize the copied server");

    /** Check that they serialize to identical files */
    char cmd[1024];
    sprintf(cmd, "diff ./%s ./%s", config_name, old_config_name);
    TEST(system(cmd) == 0, "The files are not identical");

    return true;
}

int main(int argc, char** argv)
{
    int result = 0;

    run_unit_test([&]() {
                      params.set("address", "localhost");
                      result += test1();

                      if (!test_serialize())
                      {
                          result++;
                      }
                  });

    return result;
}
