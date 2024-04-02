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

#include <getopt.h>
#include <libgen.h>
#include <stdarg.h>
#include <time.h>
#include <csignal>
#include <iostream>
#include <string>
#include <fstream>
#include <future>
#include <algorithm>

#include <maxbase/assert.hh>
#include <maxbase/format.hh>
#include <maxbase/stacktrace.hh>
#include <maxbase/string.hh>
#include <maxbase/stopwatch.hh>

#include <maxtest/galera_cluster.hh>
#include <maxtest/log.hh>
#include <maxtest/replication_cluster.hh>
#include <maxtest/mariadb_func.hh>
#include <maxtest/sql_t1.hh>
#include <maxtest/testconnections.hh>
#include <maxtest/test_info.hh>
#include "envv.hh"

using std::cout;
using std::endl;
using std::string;
using std::move;

namespace
{
// These must match the labels recognized by MDBCI.
const string label_repl_be = "REPL_BACKEND";
const string label_galera_be = "GALERA_BACKEND";
const string label_2nd_mxs = "SECOND_MAXSCALE";
const string label_cs_be = "COLUMNSTORE_BACKEND";

const StringSet recognized_mdbci_labels =
{label_repl_be, label_galera_be, label_2nd_mxs, label_cs_be};

const int MDBCI_FAIL = 200;     // Exit code when failure caused by MDBCI non-zero exit
const int BROKEN_VM_FAIL = 201; // Exit code when failure caused by broken VMs
}

namespace
{
bool start_maxscale = true;
string required_repl_version;
bool restart_galera = false;
}

static void signal_set(int sig, void (* handler)(int))
{
    struct sigaction sigact = {};
    sigact.sa_handler = handler;

    do
    {
        errno = 0;
        sigaction(sig, &sigact, NULL);
    }
    while (errno == EINTR);
}

static int call_system(const string& command)
{
    int rv = system(command.c_str());
    if (rv == -1)
    {
        printf("error: Could not execute '%s'.\n", command.c_str());
    }

    return rv;
}

void sigfatal_handler(int i)
{
    mxb::dump_stacktrace();
    signal_set(i, SIG_DFL);
    raise(i);
}

void TestConnections::skip_maxscale_start(bool value)
{
    start_maxscale = !value;
}

void TestConnections::require_repl_version(const char* version)
{
    required_repl_version = version;
}

TestConnections::TestConnections()
    : global_result(m_shared.log.m_n_fails)
    , m_n_time_wait(count_tcp_time_wait())
{
}

TestConnections::TestConnections(int argc, char* argv[])
    : TestConnections()
{
    int rc = prepare_for_test(argc, argv);
    if (rc != 0)
    {
        cleanup();
        exit(rc);
    }
    m_state = State::RUNNING;
}

int TestConnections::prepare_for_test(int argc, char* argv[])
{
    if (m_state != State::NONE)
    {
        tprintf("ERROR: prepare_for_test called more than once.");
        return 1;
    }

    m_state = State::INIT;

    std::ios::sync_with_stdio(true);
    set_signal_handlers();

    // Read basic settings from env variables first, as cmdline may override.
    read_basic_settings();

    int rc = 1;
    if (read_cmdline_options(argc, argv) && read_test_info())
    {
        if (m_shared.settings.mdbci_test)
        {
            if (check_create_vm_dir())
            {
                rc = setup_vms();
            }
        }
        else if (setup_backends())
        {
            rc = 0;
        }
    }

    if (rc != 0)
    {
        return rc;
    }

    // Stop MaxScale to prevent it from interfering with replication setup.
    if (!m_mxs_manual_debug)
    {
        stop_all_maxscales();
    }

    if (galera && restart_galera && m_shared.settings.mdbci_test)
    {
        galera->stop_nodes();
        galera->start_replication();
    }

    if (m_check_nodes)
    {
        if (repl)
        {
            // The replication cluster has extra backends if too many are configured due to a previously
            // ran big test. Hide the extra ones.
            repl->remove_extra_backends();

            // Remove stale connections to the database. These sometimes appear when the connection to the
            // database is blocked. Extra connections cause connection count calculations to be off which
            // currently causes problems.
            repl->close_active_connections();
        }

        auto check_node = [&rc](MariaDBCluster* cluster) {
            if (cluster)
            {
                if (!cluster->fix_replication() || !cluster->check_create_test_db())
                {
                    rc = BROKEN_VM_FAIL;
                }
            }
        };

        check_node(repl);
        check_node(galera);
    }

    if (rc == 0)
    {
        if (!check_backend_versions())
        {
            if (ok())
            {
                tprintf("Skipping test.");
                rc = TEST_SKIPPED;
            }
            else
            {
                rc = global_result;
            }
        }
    }

    if (rc == 0)
    {
        if (m_init_maxscale && !m_mxs_manual_debug)
        {
            init_maxscales();
        }

        if (m_mdbci_called)
        {
            auto res = maxscale->ssh_output("maxscale --version-full", false);
            if (res.rc != 0)
            {
                tprintf("Error retrieving MaxScale version info");
            }
            else
            {
                tprintf("Maxscale_full_version_start:\n%s\nMaxscale_full_version_end\n", res.output.c_str());
            }
        }

        string create_logdir_cmd = "mkdir -p ";
        create_logdir_cmd += mxt::BUILD_DIR;
        create_logdir_cmd += "/LOGS/" + m_shared.test_name;
        if (run_shell_command(create_logdir_cmd, "Failed to create logs directory."))
        {
            if (m_enable_timeout)
            {
                m_timeout_thread = std::thread(&TestConnections::timeout_thread_func, this);
            }
            tprintf("Starting test");
            logger().reset_timer();
            rc = 0;
        }
    }

    if (!ok())
    {
        rc = 1;
    }

    return rc;
}

TestConnections::~TestConnections()
{
    if (m_state != State::CLEANUP_DONE)
    {
        // Gets here if cleanup has not been explicitly called.
        int rc = cleanup();
        if (rc != 0)
        {
            exit(rc);
        }
        if (global_result)
        {
            // This causes the test to fail if a core dump is found
            exit(1);
        }
    }
    delete repl;
    delete galera;
    delete maxscale;
    delete maxscale2;

    // Wait for TCP sockets in the TIME_WAIT state to die. This can happen if the test repeatedly creates
    // connections and then closes them. Checking that we're ending the test with roughly as many connections
    // as we start with should help stabilize tests that follow a heavier tests.
    //
    // Anything below 1000 TCP connections is acceptable but if we start with a higher value, wait for at
    // least that value plus some extra to be reached. This should make sure that large spikes of TCP
    // connections in TIME_WAIT get smoothed out but a slow constant growth of them doesn't stop the test.
    int limit = std::max(m_n_time_wait + 100, 1000);
    int num = count_tcp_time_wait();

    if (num > limit)
    {
        tprintf("Found %d TCP connections in TIME_WAIT state, waiting for it to drop below %d", num, limit);
    }

    // Don't wait forever in case it's something else that's creating the connections.
    for (int i = 0; i < 120 && num > limit; i++)
    {
        sleep(1);
        num = count_tcp_time_wait();
    }
}

int TestConnections::cleanup()
{
    mxb_assert(m_state == State::INIT || m_state == State::RUNNING);
    m_state = State::CLEANUP;

    if (global_result > 0)
    {
        printf("\nTEST FAILURES:\n");
        printf("%s\n", logger().all_errors_to_string().c_str());
    }
    else
    {
        tprintf("Test complete");
    }

    // Because cleanup is called even when system test init fails, we need to check fields exist before
    // access.
    if (!m_mxs_manual_debug)
    {
        // Stop all MaxScales to detect crashes on exit.
        bool sleep_more = false;
        for (int i = 0; i < n_maxscales(); i++)
        {
            auto mxs = my_maxscale(i);
            mxs->stop_and_check_stopped();
            if (mxs->use_valgrind())
            {
                sleep_more = true;
            }
        }

        if (sleep_more)
        {
            sleep(15);      // Sleep to allow more time for log writing. TODO: Really need 15s?
        }
    }

    if (m_fix_clusters_after)
    {
        mxt::BoolFuncArray funcs;
        if (repl)
        {
            funcs.push_back([this]() {
                return repl->fix_replication();
            });
        }
        if (galera)
        {
            funcs.push_back([this]() {
                return galera->fix_replication();
            });
        }
        m_shared.concurrent_run(funcs);
    }

    m_stop_threads = true;
    if (m_timeout_thread.joinable())
    {
        m_timeout_cv.notify_one();
        m_timeout_thread.join();
    }
    if (m_log_copy_thread.joinable())
    {
        m_log_copy_thread.join();
    }

    copy_all_logs();
    m_state = State::CLEANUP_DONE;
    return 0;
}

int TestConnections::setup_vms()
{
    auto call_mdbci_and_check = [this](const char* mdbci_options = "") {
        bool vms_found = false;
        if (call_mdbci(mdbci_options))
        {
            m_mdbci_called = true;
            // Network config should exist now.
            if (read_network_config())
            {
                if (required_machines_are_running())
                {
                    vms_found = true;
                }
                else
                {
                    add_failure("Still missing VMs after running MDBCI.");
                }
            }
            else
            {
                add_failure("Failed to read network_config or configured_labels after running MDBCI.");
            }
        }
        else
        {
            add_failure("MDBCI failed.");
        }
        return vms_found;
    };

    bool maxscale_installed = false;

    bool vms_found = false;
    if (m_recreate_vms)
    {
        // User has requested to recreate all VMs required by current test.
        if (call_mdbci_and_check("--recreate"))
        {
            vms_found = true;
            maxscale_installed = true;
        }
    }
    else
    {
        if (read_network_config() && required_machines_are_running())
        {
            vms_found = true;
        }
        else
        {
            // Not all VMs were found. Call MDBCI.
            if (call_mdbci_and_check())
            {
                vms_found = true;
                maxscale_installed = true;
            }
        }
    }

    int rval = MDBCI_FAIL;
    if (vms_found && initialize_nodes())
    {
        rval = 0;
        if (m_reinstall_maxscale)
        {
            if (reinstall_maxscales())
            {
                maxscale_installed = true;
            }
            else
            {
                add_failure("Failed to install Maxscale: target is %s", m_target.c_str());
                rval = MDBCI_FAIL;
            }
        }

        if (rval == 0 && maxscale_installed)
        {
            string src = string(mxt::SOURCE_DIR) + "/mdbci/add_core_cnf.sh";
            for (int i = 0; i < n_maxscales(); i++)
            {
                auto mxs = my_maxscale(i);
                auto homedir = mxs->access_homedir();
                mxs->copy_to_node(src.c_str(), homedir);
                mxs->ssh_node_f(true, "%s/add_core_cnf.sh %s", homedir, verbose() ? "verbose" : "");
            }
        }
    }

    return rval;
}

void TestConnections::add_result(bool result, const char* format, ...)
{
    if (result)
    {
        va_list argp;
        va_start(argp, format);
        logger().add_failure_v(format, argp);
        va_end(argp);

        if (m_state == State::RUNNING)
        {
            maxscale->write_in_log(logger().latest_error());
        }
    }
}

bool TestConnections::expect(bool result, const char* format, ...)
{
    va_list argp;
    va_start(argp, format);
    logger().expect_v(result, format, argp);
    va_end(argp);

    if (!result && m_state == State::RUNNING)
    {
        maxscale->write_in_log(logger().latest_error());
    }
    return result;
}

void TestConnections::add_failure(const char* format, ...)
{
    va_list argp;
    va_start(argp, format);
    logger().add_failure_v(format, argp);
    va_end(argp);

    if (m_state == State::RUNNING)
    {
        maxscale->write_in_log(logger().latest_error());
    }
}

/**
 * Read the contents of both the 'network_config' and 'configured_labels'-files. The files may not exist
 * if the VM setup has not yet been initialized (first test of the test run).
 *
 * @return True on success
 */
bool TestConnections::read_network_config()
{
    m_network_config.clear();
    m_configured_mdbci_labels.clear();
    const char warnmsg_fmt[] = "Warning: Failed to open '%s'. File needs to be created.";
    bool rval = false;

    string nwconf_filepath = m_vm_path + "_network_config";
    std::ifstream nwconf_file(nwconf_filepath);
    if (nwconf_file.is_open())
    {
        string line;
        while (std::getline(nwconf_file, line))
        {
            if (!line.empty())
            {
                // The line should be of form <key> = <value>.
                auto eq_pos = line.find('=');
                if (eq_pos != string::npos && eq_pos > 0 && eq_pos < line.length() - 1)
                {
                    string key = line.substr(0, eq_pos);
                    string val = line.substr(eq_pos + 1, string::npos);
                    mxb::trim(key);
                    mxb::trim(val);
                    if (!key.empty() && !val.empty())
                    {
                        m_network_config.insert(std::make_pair(key, val));
                    }
                }
            }
        }

        string labels_filepath = m_vm_path + "_configured_labels";
        std::ifstream labels_file(labels_filepath);
        if (labels_file.is_open())
        {
            // The file should contain just one line.
            line.clear();
            std::getline(labels_file, line);
            m_configured_mdbci_labels = parse_to_stringset(line);
            if (!m_configured_mdbci_labels.empty())
            {
                rval = true;
            }
            else
            {
                tprintf("Warning: Could not read any labels from '%s'", labels_filepath.c_str());
            }
        }
        else
        {
            tprintf(warnmsg_fmt, labels_filepath.c_str());
        }
    }
    else
    {
        tprintf(warnmsg_fmt, nwconf_filepath.c_str());
    }
    return rval;
}

void TestConnections::read_basic_settings()
{
    // The following settings can be overridden by cmdline settings, but not by mdbci.
    maxscale_ssl = readenv_bool("ssl", false);
    m_use_ipv6 = readenv_bool("use_ipv6", false);
    backend_ssl = readenv_bool("backend_ssl", false);
    smoke = readenv_bool("smoke", true);
    m_threads = readenv_int("threads", 4);
    m_maxscale_log_copy = !readenv_bool("no_maxscale_log_copy", false);

    if (readenv_bool("no_nodes_check", false))
    {
        m_check_nodes = false;
    }

    if (readenv_bool("no_maxscale_start", false))
    {
        start_maxscale = false;
    }

    // The following settings are final, and not modified by either command line parameters or mdbci.
    m_backend_log_copy = !readenv_bool("no_backend_log_copy", false);
    m_mdbci_vm_path = envvar_get_set("MDBCI_VM_PATH", "%s/vms/", getenv("HOME"));
    m_mdbci_config_name = envvar_get_set("mdbci_config_name", "local");
    mxb_assert(!m_mdbci_vm_path.empty() && !m_mdbci_config_name.empty());
    m_vm_path = m_mdbci_vm_path + "/" + m_mdbci_config_name;

    m_mdbci_template = envvar_get_set("template", "default");
    m_target = envvar_get_set("target", "develop");
}

/**
 * Using the test name as given on the cmdline, get test config file and labels.
 */
bool TestConnections::read_test_info()
{
    const TestDefinition* found = nullptr;
    for (int i = 0; test_definitions[i].name; i++)
    {
        auto* test = &test_definitions[i];
        if (test->name == m_shared.test_name)
        {
            found = test;
            break;
        }
    }

    if (found)
    {
        m_cnf_template_path = found->config_template;
        // Parse the labels-string to a set.
        auto test_labels = parse_to_stringset(found->labels);

        /**
         * MDBCI recognizes labels which affect backend configuration. Save those labels to a separate field.
         * Also save a string version, as that is needed for mdbci.
         */
        StringSet mdbci_labels;
        mdbci_labels.insert("MAXSCALE");
        std::set_intersection(test_labels.begin(), test_labels.end(),
                              recognized_mdbci_labels.begin(), recognized_mdbci_labels.end(),
                              std::inserter(mdbci_labels, mdbci_labels.begin()));

        m_required_mdbci_labels = mdbci_labels;
        m_required_mdbci_labels_str = flatten_stringset(mdbci_labels);

        tprintf("Test: '%s', MaxScale config file: '%s', all labels: '%s', mdbci labels: '%s'",
                m_shared.test_name.c_str(), m_cnf_template_path.c_str(), found->labels,
                m_required_mdbci_labels_str.c_str());

        if (test_labels.count("BACKEND_SSL") > 0)
        {
            backend_ssl = true;
        }

        if (test_labels.count("LISTENER_SSL") > 0)
        {
            maxscale_ssl = true;
        }
    }
    else
    {
        add_failure("Could not find '%s' in the CMake-generated test definitions array.",
                    m_shared.test_name.c_str());
    }
    return found != nullptr;
}

/**
 * Process a MaxScale configuration file. Replaces the placeholders in the text with correct values.
 *
 * @param mxs MaxScale to configure
 * @param config_file_path Config file template path
 * @return True on success
 */
bool
TestConnections::process_template(mxt::MaxScale& mxs, const string& config_file_path)
{
    tprintf("Processing MaxScale config file %s\n", config_file_path.c_str());
    std::ifstream config_file(config_file_path);
    string file_contents;
    if (config_file.is_open())
    {
        std::ostringstream ss;
        ss << config_file.rdbuf();
        file_contents = ss.str();
    }

    if (file_contents.empty())
    {
        int eno = errno;
        add_failure("Failed to read MaxScale config file template '%s' or file was empty. Error %i: %s",
                    config_file_path.c_str(), eno, mxb_strerror(eno));
        return false;
    }

    // Replace various items in the config file text, then write it to disk. Define a helper function.
    auto replace_text = [&file_contents](const string& what, const string& replacement) {
        size_t pos = 0;
        while (pos != string::npos)
        {
            pos = file_contents.find(what, pos);
            if (pos != string::npos)
            {
                file_contents.replace(pos, what.length(), replacement);
            }
        }
    };

    // The order of the replacements matters, as some may lead to others.
    replace_text("###threads###", std::to_string(m_threads));
    replace_text("###access_homedir###", mxs.access_homedir());

    const string ssh_user_tag = "###ssh_user###";
    if (file_contents.find(ssh_user_tag) != string::npos)
    {
        // The "repl"-field may not exist so only read it when the tag is found.
        replace_text("###ssh_user###", repl->backend(0)->vm_node().access_user());
    }

    const string basic_mariadbmon =
        R"([MariaDB-Monitor]
type=monitor
module=mariadbmon
servers=###server_line###
user=mariadbmon
password=mariadbmon
monitor_interval=1s
replication_user=repl
replication_password=repl
backend_connect_timeout=5s
backend_read_timeout=5s
backend_write_timeout=5s)";
    replace_text("###mariadb_monitor###", basic_mariadbmon);

    const string basic_rwsplit_svc =
        R"([RW-Split-Router]
type=service
router=readwritesplit
servers=###server_line###
user=maxservice
password=maxservice)";
    replace_text("###rwsplit_service###", basic_rwsplit_svc);

    const string basic_rwsplit_lst = R"([RW-Split-Listener]
type=listener
service=RW-Split-Router
port=4006)";
    replace_text("###rwsplit_listener###", basic_rwsplit_lst);

    const string ssl_files_ph = "###mxs_cert_files###";
    if (file_contents.find(ssl_files_ph) != string::npos)
    {
        string ssl_cert_files = mxb::string_printf(
            "ssl_cert=%s\nssl_key=%s\nssl_ca_cert=%s",
            mxs.cert_path().c_str(), mxs.cert_key_path().c_str(), mxs.ca_cert_path().c_str());
        replace_text(ssl_files_ph, ssl_cert_files);
    }

    MariaDBCluster* clusters[] = {repl, galera};
    for (auto cluster : clusters)
    {
        if (cluster)
        {
            bool using_ip6 = cluster->using_ipv6();
            auto& nw_conf_prefix = cluster->nwconf_prefix();

            for (int i = 0; i < cluster->N; i++)
            {
                // These placeholders in the config template use the network config node name prefix,
                // not the MaxScale config server name prefix.
                string ip_ph = mxb::string_printf("###%s_server_IP_%0d###", nw_conf_prefix.c_str(), i + 1);
                string ip_str = using_ip6 ? cluster->ip6(i) : cluster->ip_private(i);
                replace_text(ip_ph, ip_str);

                string port_ph = mxb::string_printf("###%s_server_port_%0d###",
                                                    nw_conf_prefix.c_str(), i + 1);
                string port_str = std::to_string(cluster->port(i));
                replace_text(port_ph, port_str);
            }

            // The following generates basic server definitions for all servers. These, confusingly,
            // use the MaxScale config file name prefix in the placeholder.
            const char* prefix = cluster->cnf_server_prefix().c_str();
            string all_servers_ph = mxb::string_printf("###%s###", prefix);
            string all_servers_str = cluster->cnf_servers();
            replace_text(all_servers_ph, all_servers_str);
            // Allow also an alternate form, e.g. "server_definitions".
            string all_servers_ph2 = mxb::string_printf("###%s_definitions###", prefix);
            replace_text(all_servers_ph2, all_servers_str);

            // The following generates one line with server names. Used with monitors and services.
            string all_servers_line_ph = mxb::string_printf("###%s_line###", prefix);
            string all_server_line_str = cluster->cnf_servers_line();
            replace_text(all_servers_line_ph, all_server_line_str);
            // Allow "server_names".
            string all_servers_line_ph2 = mxb::string_printf("###%s_names###", prefix);
            replace_text(all_servers_line_ph2, all_server_line_str);
        }
    }

    bool rval = false;
    // Simple replacements are done. Parse the config file, and enable ssl for servers if required.
    auto parse_res = mxb::ini::parse_config_text_to_map(file_contents);
    if (parse_res.errors.empty())
    {
        auto& config = parse_res.config;

        auto enable_ssl = [this, &config](const string& mod_type, const string& ssl_cert,
                                          const string& ssl_key, const string& ssl_ca_cert) {
            // Check every section with the correct "type".
            std::vector<string> affected_sections;
            for (auto& section : config)
            {
                auto& kvs = section.second.key_values;
                auto it = kvs.find("type");
                if (it != kvs.end() && it->second.value == mod_type)
                {
                    // Only edit the section if "ssl" is not set.
                    if (kvs.count("ssl") == 0)
                    {
                        kvs.emplace("ssl", "true");
                        kvs.emplace("ssl_cert", ssl_cert);
                        kvs.emplace("ssl_key", ssl_key);
                        kvs.emplace("ssl_ca_cert", ssl_ca_cert);
                        kvs.emplace("ssl_cert_verify_depth", "9");
                        kvs.emplace("ssl_version", "MAX");
                        affected_sections.push_back(section.first);
                    }
                }
            }
            auto list_str = mxb::create_list_string(affected_sections, ", ", " and ");
            if (!list_str.empty())
            {
                tprintf("Configured ssl for %s.", list_str.c_str());
            }
        };

        if (backend_ssl || maxscale_ssl)
        {
            // Use the same certificate in listener and server sections, as it's the same host.
            string ssl_cert = mxs.cert_path();
            string ssl_key = mxs.cert_key_path();
            string ssl_ca_cert = mxs.ca_cert_path();
            if (backend_ssl)
            {
                enable_ssl("server", ssl_cert, ssl_key, ssl_ca_cert);
            }
            if (maxscale_ssl)
            {
                enable_ssl("listener", ssl_cert, ssl_key, ssl_ca_cert);
            }
        }

        // TODO: Add more "smartness". Check which routers are enabled, etc ...

        // Need to have some manual code here depending on if MaxScale is local or remote. In the remote case,
        // first generate the file locally, then copy to node. In the local case, just write directly to
        // destination.
        const bool remote_mxs = mxs.vm_node().is_remote();
        string target_file = remote_mxs ? "gen_maxscale.cnf" : mxs.cnf_path();
        std::ofstream output_file(target_file);
        if (output_file.is_open())
        {
            output_file << mxb::ini::config_map_to_string(config);
            output_file.close();
            rval = remote_mxs ? mxs.vm_node().copy_to_node_sudo(target_file, mxs.cnf_path()) : true;
        }
        else
        {
            int eno = errno;
            add_failure("Could not write to '%s'. Error %i, %s", target_file.c_str(), eno, mxb_strerror(eno));
        }
    }
    else
    {
        add_failure("Could not parse MaxScale configuration. Errors:");
        for (auto& s : parse_res.errors)
        {
            tprintf("%s", s.c_str());
        }
    }

    return rval;
}

/**
 * Copy maxscale.cnf and start MaxScale on all Maxscale VMs.
 */
void TestConnections::init_maxscales()
{
    // Always initialize the first MaxScale
    init_maxscale(0);

    if (m_required_mdbci_labels.count(label_2nd_mxs))
    {
        init_maxscale(1);
    }
    else if (n_maxscales() > 1)
    {
        // Second MaxScale exists but is not required by test.
        my_maxscale(1)->stop();
    }
}

void TestConnections::init_maxscale(int m)
{
    auto mxs = my_maxscale(m);

    // The config file path can be multivalued when running a test with multiple MaxScales.
    // Select the correct file.
    auto filepaths = mxb::strtok(m_cnf_template_path, ";");
    int n_files = filepaths.size();
    if (m < n_files)
    {
        // Have a separate config file for this MaxScale.
        process_template(*mxs, filepaths[m]);
    }
    else if (n_files >= 1)
    {
        // Not enough config files given for all MaxScales. Use the config of first MaxScale. This can
        // happen with the "check_backends"-test.
        tprintf("MaxScale %i does not have a designated config file, only found %i files in test definition. "
                "Using main MaxScale config file instead.", m, n_files);
        process_template(*mxs, filepaths[0]);
    }
    else
    {
        tprintf("No MaxScale config files defined. MaxScale may not start.");
    }

    string mxs_cert_dir = mxb::string_printf("%s/certs", mxs->access_homedir());
    string test_cmd = mxb::string_printf("test -d %s", mxs_cert_dir.c_str());
    if (mxs->vm_node().run_cmd_output(test_cmd).rc != 0)
    {
        tprintf("SSL certificate dir '%s' not found on MaxScale node, creating it and copying certificate.",
                mxs_cert_dir.c_str());
        auto mkdir_res = mxs->ssh_node_f(false, "rm -rf %s;mkdir -p -m a+wrx %s;",
                                         mxs_cert_dir.c_str(), mxs_cert_dir.c_str());
        if (mkdir_res == 0)
        {
            string mxs_cert = mxb::string_printf("%s/ssl-cert/mxs.crt", mxt::SOURCE_DIR);
            string mxs_key = mxb::string_printf("%s/ssl-cert/mxs.key", mxt::SOURCE_DIR);
            string ca_cert = mxb::string_printf("%s/ssl-cert/ca.crt", mxt::SOURCE_DIR);

            if (mxs->vm_node().is_remote())
            {
                mxs->copy_to_node(mxs_cert, mxs->cert_path());
                mxs->copy_to_node(mxs_key, mxs->cert_key_path());
                mxs->copy_to_node(ca_cert, mxs->ca_cert_path());
                mxs->ssh_node_f(true, "chmod -R a+rx %s;", mxs->access_homedir());
            }
            else
            {
                const char copy_fmt[] = "cp %s %s/";
                m_shared.run_shell_cmdf(copy_fmt, mxs_cert.c_str(), mxs_cert_dir.c_str());
                m_shared.run_shell_cmdf(copy_fmt, mxs_key.c_str(), mxs_cert_dir.c_str());
                m_shared.run_shell_cmdf(copy_fmt, ca_cert.c_str(), mxs_cert_dir.c_str());
            }
        }
        else
        {
            add_failure("Could not create SSL certificate dir on %s. Error %i.",
                        mxs->vm_node().name(), mkdir_res);
        }
    }

    mxs->delete_logs_and_rtfiles();

    if (start_maxscale)
    {
        expect(mxs->restart_maxscale() == 0, "Failed to start MaxScale");
        mxs->wait_for_monitor();
    }
    else
    {
        mxs->stop_maxscale();
    }
}

/**
 * Copies all MaxScale logs and (if happens) core to current workspace
 */
void TestConnections::copy_all_logs()
{
    string str = mxb::string_printf("mkdir -p %s/LOGS/%s", mxt::BUILD_DIR, m_shared.test_name.c_str());
    call_system(str);

    if (m_backend_log_copy)
    {
        if (repl)
        {
            repl->copy_logs("node");
        }
        if (galera)
        {
            galera->copy_logs("galera");
        }
    }

    if (m_maxscale_log_copy && !m_mxs_manual_debug)
    {
        copy_maxscale_logs(0);
    }
}

/**
 * Copies logs from all MaxScales.
 *
 * @param timestamp The timestamp to add to log file directory. 0 means no timestamp.
 */
void TestConnections::copy_maxscale_logs(int timestamp)
{
    for (int i = 0; i < n_maxscales(); i++)
    {
        auto mxs = my_maxscale(i);
        mxs->copy_log(i, timestamp, m_shared.test_name);
    }
}

/**
 * Copies all MaxScale logs and (if happens) core to current workspace and
 * sends time stamp to log copying script
 */
void TestConnections::copy_all_logs_periodic()
{
    copy_maxscale_logs(logger().time_elapsed_s());
}

void TestConnections::revert_replicate_from_master()
{
    repl->connect();
    execute_query(repl->nodes[0], "RESET MASTER");

    for (int i = 1; i < repl->N; i++)
    {
        repl->replicate_from(i, 0);
    }
}

bool TestConnections::log_matches(const char* pattern)
{
    return maxscale->log_matches(pattern);
}

void TestConnections::log_includes(const char* pattern)
{
    add_result(!log_matches(pattern), "Log does not match pattern '%s'", pattern);
}

void TestConnections::log_excludes(const char* pattern)
{
    add_result(log_matches(pattern), "Log matches pattern '%s'", pattern);
}

static int read_log(const char* name, char** err_log_content_p)
{
    FILE* f;
    *err_log_content_p = NULL;
    char* err_log_content;
    f = fopen(name, "rb");
    if (f != NULL)
    {

        int prev = ftell(f);
        fseek(f, 0L, SEEK_END);
        long int size = ftell(f);
        fseek(f, prev, SEEK_SET);
        err_log_content = (char*)malloc(size + 2);
        if (err_log_content != NULL)
        {
            fread(err_log_content, 1, size, f);
            for (int i = 0; i < size; i++)
            {
                if (err_log_content[i] == 0)
                {
                    // printf("null detected at position %d\n", i);
                    err_log_content[i] = '\n';
                }
            }
            // printf("s=%ld\n", strlen(err_log_content));
            err_log_content[size] = '\0';
            // printf("s=%ld\n", strlen(err_log_content));
            *err_log_content_p = err_log_content;
            fclose(f);
            return 0;
        }
        else
        {
            printf("Error allocationg memory for the log\n");
            return 1;
        }
    }
    else
    {
        printf ("Error reading log %s \n", name);
        return 1;
    }
}

int TestConnections::find_connected_slave1()
{
    int conn_num;
    int current_slave = -1;
    repl->connect();
    for (int i = 0; i < repl->N; i++)
    {
        conn_num = get_conn_num(repl->nodes[i], maxscale->ip(), maxscale->hostname(), (char*) "test");
        tprintf("connections to %d: %u\n", i, conn_num);
        if ((i != 0) && (conn_num != 0))
        {
            current_slave = i;
        }
    }
    tprintf("Now connected slave node is %d (%s)\n", current_slave, repl->ip4(current_slave));
    repl->close_connections();
    return current_slave;
}

bool TestConnections::stop_all_maxscales()
{
    bool rval = true;
    for (int i = 0; i < n_maxscales(); i++)
    {
        if (my_maxscale(i)->stop_maxscale() != 0)
        {
            rval = false;
        }
    }
    return rval;
}

int TestConnections::check_maxscale_alive()
{
    int gr = global_result;
    tprintf("Connecting to Maxscale\n");
    add_result(maxscale->connect_maxscale(), "Can not connect to Maxscale\n");
    tprintf("Trying simple query against all sevices\n");
    tprintf("RWSplit \n");
    try_query(maxscale->conn_rwsplit, "show databases;");
    tprintf("ReadConn Master \n");
    try_query(maxscale->conn_master, "show databases;");
    tprintf("ReadConn Slave \n");
    try_query(maxscale->conn_slave, "show databases;");
    maxscale->close_maxscale_connections();
    add_result(global_result - gr, "Maxscale is not alive\n");
    my_maxscale(0)->expect_running_status(true);

    return global_result - gr;
}

int TestConnections::test_maxscale_connections(bool rw_split, bool rc_master, bool rc_slave)
{
    int rval = 0;
    int rc;

    tprintf("Testing RWSplit, expecting %s\n", (rw_split ? "success" : "failure"));
    rc = execute_query(maxscale->conn_rwsplit, "select 1");
    if ((rc == 0) != rw_split)
    {
        tprintf("Error: Query %s\n", (rw_split ? "failed" : "succeeded"));
        rval++;
    }

    tprintf("Testing ReadConnRoute Master, expecting %s\n", (rc_master ? "success" : "failure"));
    rc = execute_query(maxscale->conn_master, "select 1");
    if ((rc == 0) != rc_master)
    {
        tprintf("Error: Query %s", (rc_master ? "failed" : "succeeded"));
        rval++;
    }

    tprintf("Testing ReadConnRoute Slave, expecting %s\n", (rc_slave ? "success" : "failure"));
    rc = execute_query(maxscale->conn_slave, "select 1");
    if ((rc == 0) != rc_slave)
    {
        tprintf("Error: Query %s", (rc_slave ? "failed" : "succeeded"));
        rval++;
    }
    return rval;
}


int TestConnections::create_connections(int conn_N, bool rwsplit_flag, bool master_flag, bool slave_flag,
                                        bool galera_flag)
{
    int i;
    int local_result = 0;
    MYSQL* rwsplit_conn[conn_N];
    MYSQL* master_conn[conn_N];
    MYSQL* slave_conn[conn_N];
    MYSQL* galera_conn[conn_N];
    const bool verbose = this->verbose();

    tprintf("Opening %d connections to each router\n", conn_N);
    for (i = 0; i < conn_N; i++)
    {
        if (verbose)
        {
            tprintf("opening %d-connection: ", i + 1);
        }

        if (rwsplit_flag)
        {
            if (verbose)
            {
                printf("RWSplit \t");
            }

            rwsplit_conn[i] = maxscale->open_rwsplit_connection();
            if (!rwsplit_conn[i])
            {
                local_result++;
                tprintf("RWSplit connection failed\n");
            }
        }
        if (master_flag)
        {
            if (verbose)
            {
                printf("ReadConn master \t");
            }

            master_conn[i] = maxscale->open_readconn_master_connection();
            if (mysql_errno(master_conn[i]) != 0)
            {
                local_result++;
                tprintf("ReadConn master connection failed, error: %s\n", mysql_error(master_conn[i]));
            }
        }
        if (slave_flag)
        {
            if (verbose)
            {
                printf("ReadConn slave \t");
            }

            slave_conn[i] = maxscale->open_readconn_slave_connection();
            if (mysql_errno(slave_conn[i]) != 0)
            {
                local_result++;
                tprintf("ReadConn slave connection failed, error: %s\n", mysql_error(slave_conn[i]));
            }
        }
        if (galera_flag)
        {
            if (verbose)
            {
                printf("Galera \n");
            }

            galera_conn[i] =
                open_conn(4016, maxscale->ip4(), maxscale->user_name(), maxscale->password(), maxscale_ssl);
            if (mysql_errno(galera_conn[i]) != 0)
            {
                local_result++;
                tprintf("Galera connection failed, error: %s\n", mysql_error(galera_conn[i]));
            }
        }
    }
    for (i = 0; i < conn_N; i++)
    {
        if (verbose)
        {
            tprintf("Trying query against %d-connection: ", i + 1);
        }

        if (rwsplit_flag)
        {
            if (verbose)
            {
                tprintf("RWSplit \t");
            }
            local_result += execute_query(rwsplit_conn[i], "select 1;");
        }
        if (master_flag)
        {
            if (verbose)
            {
                tprintf("ReadConn master \t");
            }
            local_result += execute_query(master_conn[i], "select 1;");
        }
        if (slave_flag)
        {
            if (verbose)
            {
                tprintf("ReadConn slave \t");
            }
            local_result += execute_query(slave_conn[i], "select 1;");
        }
        if (galera_flag)
        {
            if (verbose)
            {
                tprintf("Galera \n");
            }
            local_result += execute_query(galera_conn[i], "select 1;");
        }
    }

    // global_result += check_pers_conn(Test, pers_conn_expected);
    tprintf("Closing all connections\n");
    for (i = 0; i < conn_N; i++)
    {
        if (rwsplit_flag)
        {
            mysql_close(rwsplit_conn[i]);
        }
        if (master_flag)
        {
            mysql_close(master_conn[i]);
        }
        if (slave_flag)
        {
            mysql_close(slave_conn[i]);
        }
        if (galera_flag)
        {
            mysql_close(galera_conn[i]);
        }
    }

    return local_result;
}

void TestConnections::reset_timeout(uint32_t limit)
{
    m_reset_timeout = limit;
}

void TestConnections::set_log_copy_interval(uint32_t interval_seconds)
{
    // Add disabling if required. Currently periodic log copying is only used by a few long tests.
    mxb_assert(interval_seconds > 0);
    m_log_copy_interval = interval_seconds;

    // Assume that log copy thread not yet created. Start it. Calling this function twice in a test
    // will crash.
    m_log_copy_thread = std::thread(&TestConnections::log_copy_thread_func, this);
}

void TestConnections::tprintf(const char* format, ...)
{
    va_list argp;
    va_start(argp, format);
    logger().log_msg(format, argp);
    va_end(argp);
}

void TestConnections::log_printf(const char* format, ...)
{
    va_list argp;
    va_start(argp, format);
    string msg = mxb::string_vprintf(format, argp);
    va_end(argp);

    tprintf("%s", msg.c_str());
    if (m_state == State::RUNNING)
    {
        maxscale->write_in_log(std::move(msg));
    }
}

int TestConnections::get_master_server_id()
{
    int master_id = -1;
    MYSQL* conn = maxscale->open_rwsplit_connection();
    char str[100];
    if (find_field(conn, "SELECT @@server_id, @@last_insert_id;", "@@server_id", str) == 0)
    {
        char* endptr = NULL;
        auto colvalue = strtol(str, &endptr, 0);
        if (endptr && *endptr == '\0')
        {
            master_id = colvalue;
        }
    }
    mysql_close(conn);
    return master_id;
}

/**
 * Thread which terminates test application if it seems stuck.
 */
void TestConnections::timeout_thread_func()
{
    auto timeout_start = mxb::Clock::now();
    auto timeout_limit = mxb::from_secs(300);
    auto relax = std::memory_order_relaxed;

    while (!m_stop_threads.load(relax))
    {
        auto now = mxb::Clock::now();
        if (uint32_t timeout = m_reset_timeout.exchange(0, relax))
        {
            timeout_start = now;
            timeout_limit = mxb::from_secs(timeout);
        }

        if (now - timeout_start > timeout_limit)
        {
            logger().add_failure("**** Timeout reached! Copying logs and exiting. ****");

            for (int i = 0; i < n_maxscales(); i++)
            {
                my_maxscale(i)->create_report();
            }

            copy_all_logs();
            exit(250);
        }

        std::unique_lock<std::mutex> guard(m_timeout_lock);
        m_timeout_cv.wait_for(guard, std::chrono::milliseconds(500));
    }
}

/**
 * Function which periodically copies logs from Maxscale machine.
 */
void TestConnections::log_copy_thread_func()
{
    logger().log_msg("**** Periodic log copy thread started ****");
    auto last_log_copy = mxb::Clock::now();
    auto interval = mxb::from_secs(m_log_copy_interval);

    while (!m_stop_threads.load(std::memory_order_relaxed))
    {
        auto now = mxb::Clock::now();
        if (now - last_log_copy > interval)
        {
            logger().log_msg("**** Copying all logs ****");
            copy_all_logs_periodic();
            last_log_copy = now;
        }
        sleep(1);
    }

    logger().log_msg("**** Periodic log copy thread exiting ****");
}

int TestConnections::insert_select(int N)
{
    int result = 0;

    tprintf("Create t1\n");
    create_t1(maxscale->conn_rwsplit);

    tprintf("Insert data into t1\n");
    insert_into_t1(maxscale->conn_rwsplit, N);
    repl->sync_slaves();

    tprintf("SELECT: rwsplitter\n");
    result += select_from_t1(maxscale->conn_rwsplit, N);

    tprintf("SELECT: master\n");
    result += select_from_t1(maxscale->conn_master, N);

    tprintf("SELECT: slave\n");
    result += select_from_t1(maxscale->conn_slave, N);

    return result;
}

int TestConnections::use_db(char* db)
{
    int local_result = 0;
    char sql[100];

    sprintf(sql, "USE %s;", db);
    tprintf("selecting DB '%s' for rwsplit\n", db);
    local_result += execute_query(maxscale->conn_rwsplit, "%s", sql);
    tprintf("selecting DB '%s' for readconn master\n", db);
    local_result += execute_query(maxscale->conn_master, "%s", sql);
    tprintf("selecting DB '%s' for readconn slave\n", db);
    local_result += execute_query(maxscale->conn_slave, "%s", sql);
    for (int i = 0; i < repl->N; i++)
    {
        tprintf("selecting DB '%s' for direct connection to node %d\n", db, i);
        local_result += execute_query(repl->nodes[i], "%s", sql);
    }
    return local_result;
}

int TestConnections::check_t1_table(bool presence, char* db)
{
    const char* expected = presence ? "" : "NOT";
    const char* actual = presence ? "NOT" : "";
    int start_result = global_result;

    add_result(use_db(db), "use db failed\n");
    repl->sync_slaves();

    tprintf("Checking: table 't1' should %s be found in '%s' database\n", expected, db);
    int exists = check_if_t1_exists(maxscale->conn_rwsplit);

    if (exists == presence)
    {
        tprintf("RWSplit: ok\n");
    }
    else
    {
        add_result(1, "Table t1 is %s found in '%s' database using RWSplit\n", actual, db);
    }

    exists = check_if_t1_exists(maxscale->conn_master);

    if (exists == presence)
    {
        tprintf("ReadConn master: ok\n");
    }
    else
    {
        add_result(1,
                   "Table t1 is %s found in '%s' database using Readconnrouter with router option master\n",
                   actual,
                   db);
    }

    exists = check_if_t1_exists(maxscale->conn_slave);

    if (exists == presence)
    {
        tprintf("ReadConn slave: ok\n");
    }
    else
    {
        add_result(1,
                   "Table t1 is %s found in '%s' database using Readconnrouter with router option slave\n",
                   actual,
                   db);
    }


    for (int i = 0; i < repl->N; i++)
    {
        exists = check_if_t1_exists(repl->nodes[i]);
        if (exists == presence)
        {
            tprintf("Node %d: ok\n", i);
        }
        else
        {
            add_result(1,
                       "Table t1 is %s found in '%s' database using direct connect to node %d\n",
                       actual,
                       db,
                       i);
        }
    }

    return global_result - start_result;
}

int TestConnections::try_query(MYSQL* conn, const char* format, ...)
{
    va_list valist;

    va_start(valist, format);
    int message_len = vsnprintf(NULL, 0, format, valist);
    va_end(valist);

    char sql[message_len + 1];

    va_start(valist, format);
    vsnprintf(sql, sizeof(sql), format, valist);
    va_end(valist);

    int res = execute_query_silent(conn, sql, false);
    add_result(res,
               "Query '%.*s%s' failed!\n",
               message_len < 100 ? message_len : 100,
               sql,
               message_len < 100 ? "" : "...");
    return res;
}

StringSet TestConnections::get_server_status(const std::string& name)
{
    StringSet rval;
    auto res = maxscale->maxctrl("api get servers/" + name + " data.attributes.state");

    if (res.rc == 0 && res.output.length() > 2)
    {
        auto status = res.output.substr(1, res.output.length() - 2);

        for (const auto& a : mxb::strtok(status, ","))
        {
            rval.insert(mxb::trimmed_copy(a));
        }
    }

    return rval;
}

void TestConnections::check_current_operations(int value)
{
    for (int i = 0; i < repl->N; i++)
    {
        auto res = maxctrl("api get servers/server"
                           + std::to_string(i + 1)
                           + " data.attributes.statistics.active_operations");

        expect(std::stoi(res.output) == value,
               "Current no. of operations is not %d for server%d", value, i + 1);
    }
}

bool TestConnections::test_bad_config(const string& config)
{
    auto& mxs = *maxscale;
    if (process_template(mxs, config))
    {
        mxs.stop_and_check_stopped();
        if (ok())
        {
            // Try to start MaxScale, wait a bit and see if it's running.
            mxs.start_maxscale();
            sleep(1);
            mxs.expect_running_status(false);
            if (!ok())
            {
                logger().add_failure("MaxScale started successfully with bad config file '%s' when "
                                     "immediate shutdown was expected.", config.c_str());
            }
            mxs.stop_and_check_stopped();
        }
        auto rm_res = mxs.ssh_output("rm /etc/maxscale.cnf");
        logger().expect(rm_res.rc == 0, "Failed to delete config file: %s", rm_res.output.c_str());
    }
    return ok();
}

/**
 * Run MDBCI to bring up nodes.
 *
 * @return True on success
 */
bool TestConnections::call_mdbci(const char* options)
{
    if (access(m_vm_path.c_str(), F_OK) != 0)
    {
        // Directory does not exist, must be first time running mdbci.
        bool ok = false;
        if (process_mdbci_template())
        {
            string mdbci_gen_cmd = mxb::string_printf("mdbci --override --template %s.json generate %s",
                                                      m_vm_path.c_str(), m_mdbci_config_name.c_str());
            if (run_shell_command(mdbci_gen_cmd, "MDBCI failed to generate virtual machines description"))
            {
                string copy_cmd = mxb::string_printf("cp -r %s/mdbci/cnf %s/",
                                                     mxt::SOURCE_DIR, m_vm_path.c_str());
                if (run_shell_command(copy_cmd, "Failed to copy my.cnf files"))
                {
                    ok = true;
                }
            }
        }

        if (!ok)
        {
            return false;
        }
    }

    bool rval = false;
    string mdbci_up_cmd = mxb::string_printf("mdbci up %s --labels %s %s",
                                             m_mdbci_config_name.c_str(), m_required_mdbci_labels_str.c_str(),
                                             options);
    if (run_shell_command(mdbci_up_cmd, "MDBCI failed to bring up virtual machines"))
    {
        std::string team_keys = envvar_get_set("team_keys", "~/.ssh/id_rsa.pub");
        string keys_cmd = mxb::string_printf("mdbci public_keys --key %s %s",
                                             team_keys.c_str(), m_mdbci_config_name.c_str());
        if (run_shell_command(keys_cmd, "MDBCI failed to upload ssh keys."))
        {
            rval = true;
        }
    }

    return rval;
}

/**
 * Read template file from maxscale-system-test/mdbci/templates and replace all placeholders with
 * actual values.
 *
 * @return True on success
 */
bool TestConnections::process_mdbci_template()
{
    string box = envvar_get_set("box", "centos_7_libvirt");
    string backend_box = envvar_get_set("backend_box", "%s", box.c_str());
    envvar_get_set("vm_memory", "2048");
    envvar_get_set("maxscale_product", "maxscale_ci");
    envvar_get_set("force_maxscale_version", "true");
    envvar_get_set("force_backend_version", "true");

    string version = envvar_get_set("version", "10.5");
    envvar_get_set("galera_version", "%s", version.c_str());

    string product = envvar_get_set("product", "mariadb");
    string cnf_path;
    if (product == "mysql")
    {
        cnf_path = mxb::string_printf("%s/cnf/mysql56/", m_vm_path.c_str());
    }
    else
    {
        cnf_path = mxb::string_printf("%s/cnf/", m_vm_path.c_str());
    }
    setenv("cnf_path", cnf_path.c_str(), 1);

    string template_file = mxb::string_printf("%s/mdbci/templates/%s.json.template",
                                              mxt::SOURCE_DIR, m_mdbci_template.c_str());
    string target_file = m_vm_path + ".json";
    string subst_cmd = "envsubst < " + template_file + " > " + target_file;

    bool rval = false;
    if (run_shell_command(subst_cmd, "Failed to generate VM json config file."))
    {
        if (verbose())
        {
            tprintf("Generated VM json config file with '%s'.", subst_cmd.c_str());
        }

        string mdbci_gen_cmd = mxb::string_printf("mdbci --override --template %s generate %s",
                                                  target_file.c_str(), m_mdbci_config_name.c_str());
        if (run_shell_command(mdbci_gen_cmd, "MDBCI failed to generate VM configuration."))
        {
            rval = true;
            if (verbose())
            {
                tprintf("Generated VM configuration with '%s'.", subst_cmd.c_str());
            }
        }
    }
    return rval;
}

std::string dump_status(const StringSet& current, const StringSet& expected)
{
    std::stringstream ss;
    ss << "Current status: (";

    for (const auto& a : current)
    {
        ss << a << ",";
    }

    ss << ") Expected status: (";

    for (const auto& a : expected)
    {
        ss << a << ",";
    }

    ss << ")";

    return ss.str();
}

bool TestConnections::reinstall_maxscales()
{
    bool rval = true;
    for (int i = 0; i < n_maxscales(); i++)
    {
        if (!my_maxscale(i)->reinstall(m_target, m_mdbci_config_name))
        {
            rval = false;
        }
    }
    return rval;
}

std::string TestConnections::flatten_stringset(const StringSet& set)
{
    string rval;
    string sep;
    for (auto& elem : set)
    {
        rval += sep;
        rval += elem;
        sep = ",";
    }
    return rval;
}

StringSet TestConnections::parse_to_stringset(const string& source)
{
    string copy = source;
    StringSet rval;
    if (!copy.empty())
    {
        char* ptr = &copy[0];
        char* save_ptr = nullptr;
        // mdbci uses ',' and cmake uses ';'. Add ' ' and newline as well to ensure trimming.
        const char delim[] = ",; \n";
        char* token = strtok_r(ptr, delim, &save_ptr);
        while (token)
        {
            rval.insert(token);
            token = strtok_r(nullptr, delim, &save_ptr);
        }
    }
    return rval;
}

mxt::TestLogger& TestConnections::logger()
{
    return m_shared.log;
}

mxt::Settings& TestConnections::settings()
{
    return m_shared.settings;
}

bool TestConnections::read_cmdline_options(int argc, char* argv[])
{
    option long_options[] =
    {
        {"help",               no_argument,       0, 'h'},
        {"verbose",            no_argument,       0, 'v'},
        {"silent",             no_argument,       0, 'n'},
        {"quiet",              no_argument,       0, 'q'},
        {"no-maxscale-start",  no_argument,       0, 's'},
        {"no-maxscale-init",   no_argument,       0, 'i'},
        {"no-nodes-check",     no_argument,       0, 'r'},
        {"restart-galera",     no_argument,       0, 'g'},
        {"no-timeouts",        no_argument,       0, 'z'},
        {"local-test",         required_argument, 0, 'l'},
        {"reinstall-maxscale", no_argument,       0, 'm'},
        {"serial-run",         no_argument,       0, 'e'},
        {"fix-clusters",       no_argument,       0, 'f'},
        {"recreate-vms",       no_argument,       0, 'c'},
        {0,                    0,                 0, 0  }
    };

    bool rval = true;
    int c;
    int option_index = 0;

    while ((c = getopt_long(argc, argv, "hvnqsirgzl:mefc::", long_options, &option_index)) != -1)
    {
        switch (c)
        {
        case 'v':
            set_verbose(true);
            break;

        case 'n':
            set_verbose(false);
            break;

        case 'q':
            if (!freopen("/dev/null", "w", stdout))
            {
                printf("warning: Could not redirect stdout to /dev/null.\n");
            }
            break;

        case 'h':
            {
                printf("Options:\n");
                struct option* o = long_options;
                while (o->name)
                {
                    printf("-%c, --%s\n", o->val, o->name);
                    ++o;
                }
                rval = false;
            }
            break;

        case 's':
            printf("Maxscale won't be started\n");
            start_maxscale = false;
            m_mxs_manual_debug = true;
            break;

        case 'i':
            printf("Maxscale won't be started and Maxscale.cnf won't be uploaded\n");
            m_init_maxscale = false;
            break;

        case 'r':
            printf("Nodes are not checked before test and are not restarted\n");
            m_check_nodes = false;
            break;

        case 'g':
            printf("Restarting Galera setup\n");
            restart_galera = true;
            break;

        case 'z':
            m_enable_timeout = false;
            break;

        case 'l':
            {
                logger().log_msgf("Running test in local mode. Reading additional settings from '%s'.",
                                  optarg);
                m_shared.settings.mdbci_test = false;
                m_test_settings_file = optarg;
            }
            break;

        case 'm':
            printf("Maxscale will be reinstalled.\n");
            m_reinstall_maxscale = true;
            break;

        case 'e':
            printf("Preferring serial execution.\n");
            m_shared.settings.allow_concurrent_run = false;
            break;

        case 'f':
            printf("Fixing clusters after test.\n");
            m_fix_clusters_after = true;
            break;

        case 'c':
            printf("Recreating all test VMs.\n");
            m_recreate_vms = true;
            break;

        default:
            printf("UNKNOWN OPTION: %c\n", c);
            rval = false;
            break;
        }
    }

    m_shared.test_name = (optind < argc) ? argv[optind] : basename(argv[0]);
    return rval;
}

bool TestConnections::initialize_nodes()
{
    const char errmsg[] = "Failed to setup node group %s.";
    bool error = false;
    mxt::BoolFuncArray funcs;

    auto initialize_cluster = [&](MariaDBCluster* new_cluster, int n_min_expected, bool ipv6, bool be_ssl) {
        if (new_cluster->setup(m_network_config, n_min_expected))
        {
            new_cluster->set_use_ipv6(ipv6);
            new_cluster->set_use_ssl(be_ssl);
            auto prepare_cluster = [new_cluster]() {
                return new_cluster->basic_test_prepare();
            };
            funcs.push_back(move(prepare_cluster));
        }
        else
        {
            error = true;
            add_failure(errmsg, new_cluster->name().c_str());
        }
    };

    delete repl;
    repl = nullptr;
    bool use_repl = m_required_mdbci_labels.count(label_repl_be) > 0;
    if (use_repl)
    {
        int n_min_expected = 4;
        repl = new mxt::ReplicationCluster(&m_shared);
        initialize_cluster(repl, n_min_expected, m_use_ipv6, backend_ssl);
    }

    delete galera;
    galera = nullptr;
    bool use_galera = m_required_mdbci_labels.count(label_galera_be) > 0;
    if (use_galera)
    {
        galera = new mxt::GaleraCluster(&m_shared);
        initialize_cluster(galera, 4, false, backend_ssl);
    }

    auto initialize_maxscale = [this, &funcs](mxt::MaxScale*& mxs_storage, int vm_ind) {
        delete mxs_storage;
        mxs_storage = nullptr;
        string vm_name = mxb::string_printf("%s_%03d", mxt::MaxScale::prefix().c_str(), vm_ind);

        auto new_maxscale = std::make_unique<mxt::MaxScale>(&m_shared);
        if (new_maxscale->setup(m_network_config, vm_name))
        {
            new_maxscale->set_use_ipv6(m_use_ipv6);
            new_maxscale->set_ssl(maxscale_ssl);

            mxs_storage = new_maxscale.release();

            auto prepare_maxscales = [mxs_storage]() {
                return mxs_storage->prepare_for_test();
            };
            funcs.push_back(move(prepare_maxscales));
        }
    };

    initialize_maxscale(maxscale, 0);
    // Try to setup MaxScale2 even if test does not need it. It could be running and should be
    // shut down when not used.
    initialize_maxscale(maxscale2, 1);
    mxb_assert(settings().mdbci_test);

    int n_mxs_inited = n_maxscales();
    int n_mxs_expected = (m_required_mdbci_labels.count(label_2nd_mxs) > 0) ? 2 : 1;
    if (n_mxs_inited < n_mxs_expected)
    {
        error = true;
        add_failure("Not enough MaxScales. Test requires %i, found %i.",
                    n_mxs_expected, n_mxs_inited);
    }

    return error ? false : m_shared.concurrent_run(funcs);
}

bool TestConnections::check_backend_versions()
{
    auto tester = [](MariaDBCluster* cluster, const string& required_vrs_str) {
        bool rval = true;
        if (cluster && !required_vrs_str.empty())
        {
            int required_vrs = get_int_version(required_vrs_str);
            rval = cluster->check_backend_versions(required_vrs);
        }
        return rval;
    };

    auto repl_ok = tester(repl, required_repl_version);
    return repl_ok;
}

bool TestConnections::required_machines_are_running()
{
    StringSet missing_mdbci_labels;
    std::set_difference(m_required_mdbci_labels.begin(), m_required_mdbci_labels.end(),
                        m_configured_mdbci_labels.begin(), m_configured_mdbci_labels.end(),
                        std::inserter(missing_mdbci_labels, missing_mdbci_labels.begin()));

    bool rval = false;
    if (missing_mdbci_labels.empty())
    {
        if (verbose())
        {
            tprintf("Machines with all required labels '%s' are running, MDBCI UP call is not needed",
                    m_required_mdbci_labels_str.c_str());
        }
        rval = true;
    }
    else
    {
        string missing_labels_str = flatten_stringset(missing_mdbci_labels);
        tprintf("Machines with labels '%s' are not running, MDBCI UP call is needed",
                missing_labels_str.c_str());
    }

    return rval;
}

void TestConnections::set_verbose(bool val)
{
    m_shared.settings.verbose = val;
}

bool TestConnections::verbose() const
{
    return m_shared.settings.verbose;
}

void TestConnections::write_node_env_vars()
{
    auto write_env_vars = [](MariaDBCluster* cluster) {
        if (cluster)
        {
            cluster->write_env_vars();
        }
    };

    write_env_vars(repl);
    write_env_vars(galera);
    if (maxscale)
    {
        maxscale->write_env_vars();
    }
}

int TestConnections::n_maxscales() const
{
    // A maximum of two MaxScales are supported so far. Defining only the second MaxScale is an error.
    int rval = 0;
    if (maxscale)
    {
        rval = maxscale2 ? 2 : 1;
    }
    return rval;
}

int TestConnections::count_tcp_time_wait() const
{
    FILE* f = popen("netstat -an -A inet|grep -c TIME_WAIT", "r");
    char buf[256] = "";
    fgets(buf, sizeof(buf), f);
    pclose(f);

    // If netstat wasn't installed or failed for some reason, this returns 0.
    return strtol(buf, nullptr, 10);
}

int TestConnections::run_test(int argc, char* argv[], const std::function<void(TestConnections&)>& testfunc)
{
    int init_rc = prepare_for_test(argc, argv);
    if (init_rc == 0)
    {
        m_state = State::RUNNING;
        try
        {
            testfunc(*this);
        }
        catch (const std::exception& e)
        {
            add_failure("Caught exception: %s", e.what());
        }
    }
    int cleanup_rc = cleanup();

    // Return actual test error count only if init and cleanup succeed.
    int rval = 0;
    if (init_rc != 0)
    {
        rval = init_rc;
    }
    else if (cleanup_rc != 0)
    {
        rval = cleanup_rc;
    }
    else
    {
        rval = global_result;
    }
    return rval;
}

int TestConnections::run_test_script(const char* script, const char* name)
{
    write_node_env_vars();
    auto test_dir = mxt::SOURCE_DIR;
    setenv("src_dir", test_dir, 1);

    string script_cmd = access(script, F_OK) == 0 ?
        mxb::string_printf("%s %s", script, name) :
        mxb::string_printf("%s/%s %s", test_dir, script, name);
    int rc = system(script_cmd.c_str());

    if (WIFEXITED(rc))
    {
        rc = WEXITSTATUS(rc);
    }
    else
    {
        tprintf("Command '%s' failed. Error: %s", script_cmd.c_str(), mxb_strerror(errno));
        rc = 256;
    }

    expect(rc == 0, "Script %s exited with code %d", script_cmd.c_str(), rc);

    return global_result;
}

void TestConnections::set_signal_handlers()
{
    signal_set(SIGSEGV, sigfatal_handler);
    signal_set(SIGABRT, sigfatal_handler);
    signal_set(SIGFPE, sigfatal_handler);
    signal_set(SIGILL, sigfatal_handler);
#ifdef SIGBUS
    signal_set(SIGBUS, sigfatal_handler);
#endif
}

bool TestConnections::check_create_vm_dir()
{
    bool rval = false;
    string mkdir_cmd = "mkdir -p " + m_mdbci_vm_path;
    if (run_shell_command(mkdir_cmd, "Failed to create MDBCI VMs directory."))
    {
        rval = true;
    }
    return rval;
}

bool TestConnections::run_shell_command(const string& cmd, const string& errmsg)
{
    return m_shared.run_shell_command(cmd, errmsg);
}

mxt::CmdResult TestConnections::run_shell_cmd_output(const string& cmd, const string& errmsg)
{
    auto rval = m_shared.run_shell_cmd_output(cmd);
    auto rc = rval.rc;
    if (rc != 0)
    {
        string msgp2 = mxb::string_printf("Shell command '%s' returned %i: %s.",
                                          cmd.c_str(), rc, rval.output.c_str());
        if (errmsg.empty())
        {
            logger().add_failure("%s", msgp2.c_str());
        }
        else
        {
            logger().add_failure("%s %s", errmsg.c_str(), msgp2.c_str());
        }
    }
    return rval;
}

int TestConnections::get_repl_master_idx()
{
    int rval = -1;
    if (repl)
    {
        auto server_info = maxscale->get_servers();
        for (size_t i = 0; i < server_info.size() && rval < 0; i++)
        {
            auto& info = server_info.get(i);
            if (info.status & mxt::ServerInfo::MASTER)
            {
                for (int j = 0; j < repl->N; j++)
                {
                    auto* be = repl->backend(j);
                    if (be->status().server_id == info.server_id)
                    {
                        rval = j;
                        break;
                    }
                }
            }
        }
    }
    return rval;
}

mxt::MariaDBServer* TestConnections::get_repl_master()
{
    int idx = get_repl_master_idx();
    return idx >= 0 ? repl->backend(idx) : nullptr;
}

bool TestConnections::sync_repl_slaves()
{
    bool rval = false;
    int idx = get_repl_master_idx();
    if (idx >= 0)
    {
        rval = repl->sync_slaves(idx, 10);
    }
    return rval;
}

/**
 * Helper function for selecting correct MaxScale.
 *
 * @param m Index, 0 or 1.
 * @return MaxScale object
 */
mxt::MaxScale* TestConnections::my_maxscale(int m) const
{
    mxt::MaxScale* rval = nullptr;
    if (m == 0)
    {
        rval = maxscale;
    }
    else if (m == 1)
    {
        rval = maxscale2;
    }
    return rval;
}

mxt::SharedData& TestConnections::shared()
{
    return m_shared;
}

/**
 * Reads backend info from config file.
 *
 * @return True on success
 */
bool TestConnections::setup_backends()
{
    bool rval = false;
    auto load_res = mxb::ini::parse_config_file_to_map(m_test_settings_file);
    if (load_res.errors.empty())
    {
        // Expecting each section to have "type" and "location" keys. Type is "maxscale", "server" or
        // "galera". Location is "local", "docker" or "remote". Split the config into the supported types.
        using Sections = mxb::ini::map_result::Configuration;
        Sections maxscales_cfg;
        Sections servers_cfg;
        Sections galeras_cfg;

        string key_type = "type";
        string val_mxs = "maxscale";
        string val_srv = "server";
        string val_gal = "galera";
        bool error = false;

        for (const auto& it : load_res.config)
        {
            auto& kvs = it.second.key_values;
            auto it_type = kvs.find(key_type);
            if (it_type == kvs.end())
            {
                add_failure("No '%s' in '%s' section '%s'.", key_type.c_str(), m_test_settings_file.c_str(),
                            it.first.c_str());
                error = true;
            }
            else
            {
                auto& val_type = it_type->second.value;
                if (val_type == val_mxs)
                {
                    maxscales_cfg.insert(it);
                }
                else if (val_type == val_srv)
                {
                    servers_cfg.insert(it);
                }
                else if (val_type == val_gal)
                {
                    galeras_cfg.insert(it);
                }
                else
                {
                    add_failure("Unrecognized '%s' in '%s': '%s'. Only '%s', '%s' and '%s' are supported.",
                                key_type.c_str(), m_test_settings_file.c_str(), val_type.c_str(),
                                val_mxs.c_str(), val_srv.c_str(), val_gal.c_str());
                    error = true;
                }
            }
        }

        if (!error)
        {
            // Currently support just one MaxScale.
            if (maxscales_cfg.size() == 1)
            {
                auto new_mxs = std::make_unique<mxt::MaxScale>(&m_shared);
                if (new_mxs->setup(*maxscales_cfg.begin()))
                {
                    new_mxs->set_use_ipv6(m_use_ipv6);
                    new_mxs->set_ssl(maxscale_ssl);
                    maxscale = new_mxs.release();
                }
                else
                {
                    error = true;
                }
            }
            else
            {
                add_failure("'%s' must have one MaxScale section.", m_test_settings_file.c_str());
                error = true;
            }

            if (m_required_mdbci_labels.count(label_repl_be) > 0)
            {
                if (servers_cfg.empty())
                {
                    add_failure("Test requires replication backends but none configured.");
                    error = true;
                }
                else
                {
                    auto new_repl = std::make_unique<mxt::ReplicationCluster>(&m_shared);
                    if (new_repl->setup(servers_cfg, 4))
                    {
                        new_repl->set_use_ipv6(m_use_ipv6);
                        new_repl->set_use_ssl(backend_ssl);
                        repl = new_repl.release();
                    }
                    else
                    {
                        error = true;
                    }
                }
            }

            if (m_required_mdbci_labels.count(label_galera_be) > 0)
            {
                if (galeras_cfg.empty())
                {
                    add_failure("Test requires galera backends but none configured.");
                    error = true;
                }
                else
                {
                    auto new_galera = std::make_unique<mxt::GaleraCluster>(&m_shared);
                    if (new_galera->setup(servers_cfg, 4))
                    {
                        new_galera->set_use_ipv6(m_use_ipv6);
                        new_galera->set_use_ssl(backend_ssl);
                        galera = new_galera.release();
                    }
                }
            }

            rval = !error;
        }
    }
    else
    {
        string all_errors = mxb::create_list_string(load_res.errors, " ");
        add_failure("Could not parse test config from '%s': %s", m_test_settings_file.c_str(),
                    all_errors.c_str());
    }
    return rval;
}
