/*
 * Copyright (c) 2022 MariaDB Corporation Ab
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

#pragma once

#include <maxtest/ccdefs.hh>

#include <functional>
#include <string>
#include <thread>
#include <vector>
#include <maxbase/ini.hh>
#include <maxtest/mariadb_func.hh>
#include <maxtest/nodes.hh>

namespace maxtest
{
class MariaDB;

class TestLogger;

/**
 * Contains information about one server as seen by MaxScale.
 */
struct ServerInfo
{
    using bitfield = uint32_t;
    static constexpr bitfield UNKNOWN = 0;
    static constexpr bitfield RUNNING = (1 << 0);
    static constexpr bitfield MASTER = (1 << 1);
    static constexpr bitfield SLAVE = (1 << 2);
    static constexpr bitfield RELAY = (1 << 3);
    static constexpr bitfield MAINT = (1 << 4);
    static constexpr bitfield DRAINING = (1 << 5);
    static constexpr bitfield DRAINED = (1 << 6);
    static constexpr bitfield SYNCED = (1 << 7);
    static constexpr bitfield EXT_MASTER = (1 << 10);
    static constexpr bitfield DISK_LOW = (1 << 11);
    static constexpr bitfield BLR = (1 << 12);
    static constexpr bitfield DOWN = (1 << 13);

    static constexpr bitfield master_st = MASTER | RUNNING;
    static constexpr bitfield slave_st = SLAVE | RUNNING;

    static constexpr int GROUP_NONE = -1;
    static constexpr int RLAG_NONE = -1;
    static constexpr int SRV_ID_NONE = -1;

    static std::string status_to_string(bitfield status);
    std::string        status_to_string() const;
    bool               status_from_string(const std::string& source, const std::string& details);
    std::string        to_string_short() const;

    std::string name {"<unknown>"}; /**< Server name */
    bitfield    status {UNKNOWN};   /**< Status bitfield */
    int64_t     server_id {SRV_ID_NONE};
    int64_t     master_group {GROUP_NONE};
    int64_t     rlag {RLAG_NONE};
    int64_t     pool_conns {0};
    int64_t     connections {0};
    bool        read_only {false};
    bool        ssl_configured {false};
    std::string gtid;

    struct SlaveConnection
    {
        std::string name;
        std::string gtid;
        int64_t     master_id {SRV_ID_NONE};

        enum class IO_State
        {
            NO,
            CONNECTING,
            YES
        };
        IO_State io_running {IO_State::NO};
        bool     sql_running {false};
    };

    std::vector<SlaveConnection> slave_connections;
};

/**
 * Contains information about multiple servers as seen by MaxScale.
 */
class ServersInfo
{
public:
    ServersInfo(TestLogger* log);

    ServersInfo(const ServersInfo& rhs) = default;
    ServersInfo& operator=(const ServersInfo& rhs);
    ServersInfo(ServersInfo&& rhs) noexcept;
    ServersInfo& operator=(ServersInfo&& rhs) noexcept;

    void add(const ServerInfo& info);
    void add(ServerInfo&& info);

    const ServerInfo& get(size_t i) const;
    ServerInfo        get(const std::string& cnf_name) const;
    size_t            size() const;

    std::vector<ServerInfo>::iterator begin();
    std::vector<ServerInfo>::iterator end();

    /**
     * Return the server info of the master. If no masters are found, returns a default server info object.
     * If multiple masters are found, returns the first.
     *
     * @return Server info of master.
     */
    ServerInfo get_master() const;

    /**
     * Check that server status is as expected. Increments global error counter if differences found.
     *
     * @param expected_status Expected server statuses. Each status should be a bitfield of values defined
     * in the ServerInfo-class.
     */
    void check_servers_status(const std::vector<ServerInfo::bitfield>& expected_status);

    void check_master_groups(const std::vector<int>& expected_groups);
    void check_pool_connections(const std::vector<int>& expected_conns);
    void check_connections(const std::vector<int>& expected_conns);
    void check_read_only(const std::vector<bool>& expected_ro);

    void print();

    /**
     * Get starting server states for a master-slave cluster: master + 3 slaves.
     */
    static const std::vector<ServerInfo::bitfield>& default_repl_states();

    struct RoleInfo
    {
        int         masters {0};
        int         slaves {0};
        int         running {0};
        std::string master_name;
    };
    RoleInfo get_role_info() const;


private:
    std::vector<ServerInfo> m_servers;
    TestLogger*             m_log {nullptr};

    void check_servers_property(size_t n_expected, const std::function<void(size_t)>& tester);
};

class MaxScale
{
public:
    using SMariaDB = std::unique_ptr<mxt::MariaDB>;
    enum service
    {
        RWSPLIT,
        READCONN_MASTER,
        READCONN_SLAVE
    };

    MaxScale(mxt::SharedData* shared);
    ~MaxScale();

    bool setup(const mxt::NetworkConfig& nwconfig, const std::string& vm_name);
    bool setup(const mxb::ini::map_result::Configuration::value_type& config);

    void set_use_ipv6(bool use_ipv6);
    void set_ssl(bool ssl);

    const char* ip4() const;
    const char* ip() const;
    const char* ip_private() const;
    const char* hostname() const;

    const char* access_user() const;
    const char* access_homedir() const;
    const char* access_sudo() const;
    const char* sshkey() const;

    static const std::string& prefix();
    const std::string&        node_name() const;

    bool ssl() const;

    std::string cert_path() const;
    std::string cert_key_path() const;
    std::string ca_cert_path() const;

    int rwsplit_port {-1};          /**< RWSplit port */
    int readconn_master_port {-1};  /**< ReadConnection in master mode port */
    int readconn_slave_port {-1};   /**< ReadConnection in slave mode port */

    /**
     * @brief Get port number of a MaxScale service
     *
     * @param type Type of service
     * @return Port number of the service
     */
    int port(enum service type = RWSPLIT) const;

    MYSQL* conn_rwsplit {nullptr};      /**< Connection to RWSplit */
    MYSQL* conn_master {nullptr};       /**< Connection to ReadConnection in master mode */
    MYSQL* conn_slave {nullptr};        /**< Connection to ReadConnection in slave mode */

    /**< conn_rwsplit, conn_master, conn_slave */
    MYSQL* routers[3] {nullptr, nullptr, nullptr};
    int    ports[3] {-1, -1, -1};   /**< rwsplit_port, readconn_master_port, readconn_slave_port */

    const std::string& cnf_path() const;
    const std::string& user_name() const;
    const std::string& password() const;

    /**
     * @brief ConnectMaxscale   Opens connections to RWSplit, ReadConn master and ReadConn slave Maxscale
     * services
     * Opens connections to RWSplit, ReadConn master and ReadConn slave Maxscale services
     * Connections stored in maxscales->conn_rwsplit, maxscales->conn_master[0] and
     * maxscales->conn_slave[0] MYSQL structs
     * @return 0 in case of success
     */
    int connect_maxscale(const std::string& db = "test");
    int connect(const std::string& db = "test");

    /**
     * @brief CloseMaxscaleConn Closes connection that were opened by ConnectMaxscale()
     * @return 0
     */
    int close_maxscale_connections();
    int disconnect();

    /**
     * @brief ConnectRWSplit    Opens connections to RWSplit and store MYSQL struct in
     * maxscales->conn_rwsplit
     * @return 0 in case of success
     */
    int connect_rwsplit(const std::string& db = "test");

    /**
     * @brief ConnectReadMaster Opens connections to ReadConn master and store MYSQL struct in
     * maxscales->conn_master[0]
     * @return 0 in case of success
     */
    int connect_readconn_master(const std::string& db = "test");

    /**
     * @brief ConnectReadSlave Opens connections to ReadConn slave and store MYSQL struct in
     * maxscales->conn_slave[0]
     * @return 0 in case of success
     */
    int connect_readconn_slave(const std::string& db = "test");

    /**
     * @brief OpenRWSplitConn   Opens new connections to RWSplit and returns MYSQL struct
     * To close connection mysql_close() have to be called
     * @return MYSQL struct
     */
    MYSQL* open_rwsplit_connection(const std::string& db = "test");

    SMariaDB open_rwsplit_connection2(const std::string& db = "test");

    /**
     * Same as above except no default database.
     *
     * @return Connection object
     */
    SMariaDB open_rwsplit_connection2_nodb();

    /**
     * Try to open an RWSplit-connection using the given database. Failure is not a test error.
     *
     * @param db Database to connect to
     * @return Connection object. Call 'is_open' to check success.
     */
    SMariaDB try_open_rwsplit_connection(const std::string& db = "");
    SMariaDB try_open_rwsplit_connection(const std::string& user, const std::string& pass,
                                         const std::string& db = "");

    enum class SslMode {AUTO, ON, OFF};
    SMariaDB try_open_rwsplit_connection(SslMode ssl, const std::string& user, const std::string& pass,
                                         const std::string& db = "");

    SMariaDB try_open_connection(SslMode ssl, int port, const std::string& user, const std::string& pass,
                                 const std::string& db = "");

    SMariaDB try_open_connection(int port, const std::string& user, const std::string& pass,
                                 const std::string& db = "");

    /**
     * Get a readwritesplit Connection
     */
    Connection rwsplit(const std::string& db = "test");

    /**
     * Get a Connection to a specific port
     */
    Connection get_connection(int port, const std::string& db = "test");

    /**
     * @brief OpenReadMasterConn    Opens new connections to ReadConn master and returns MYSQL struct
     * To close connection mysql_close() have to be called
     * @return MYSQL struct
     */
    MYSQL* open_readconn_master_connection();

    /**
     * Get a readconnroute master Connection
     */
    Connection readconn_master(const std::string& db = "test");

    /**
     * @brief OpenReadSlaveConn    Opens new connections to ReadConn slave and returns MYSQL struct
     * To close connection mysql_close() have to be called
     * @return  MYSQL struct
     */
    MYSQL* open_readconn_slave_connection();

    /**
     * Get a readconnroute slave Connection
     */
    Connection readconn_slave(const std::string& db = "test");

    /**
     * @brief CloseRWSplit Closes RWplit connections stored in maxscales->conn_rwsplit
     */
    void close_rwsplit();

    /**
     * @brief CloseReadMaster Closes ReadConn master connections stored in maxscales->conn_master[0]
     */
    void close_readconn_master();

    /**
     * @brief restart_maxscale Issues 'service maxscale restart' command
     */
    int restart_maxscale();
    int restart();

    /**
     * Issues 'service maxscale start' command
     */
    int  start_maxscale();
    void start();

    /**
     * @brief stop_maxscale Issues 'service maxscale stop' command
     */
    int  stop_maxscale();
    void stop();

    bool start_and_check_started();
    bool stop_and_check_stopped();

    void delete_log();

    enum class Expect
    {
        SUCCESS, FAIL, ANY
    };

    /**
     * Execute a MaxCtrl command. Does not check result.
     *
     * @param cmd  Command to execute, without the `maxctrl` part
     * @param sudo Run the command as root
     *
     * @return The exit code and output of MaxCtrl
     */
    mxt::CmdResult maxctrl(const std::string& cmd, bool sudo = true);

    /**
     * Execute a MaxCtrl command, expecting success.
     */
    mxt::CmdResult maxctrlf(const char* fmt, ...) mxb_attribute((format (printf, 2, 3)));

    /**
     * Execute a MaxCtrl command with a specific expectation.
     */
    mxt::CmdResult maxctrlf(Expect expect, const char* fmt, ...) mxb_attribute((format (printf, 3, 4)));

    /**
     * @brief get_maxscale_memsize Gets size of the memory consumed by Maxscale process
     * @param m Number of Maxscale node
     * @return memory size in kilobytes
     */
    long unsigned get_maxscale_memsize(int m = 0);

    void copy_log(int mxs_ind, int timestamp, const std::string& test_name);

    mxt::ServersInfo get_servers();

    int get_master_server_id();

    /**
     * Wait until all running monitors have ticked.
     *
     * @param intervals The number of monitor intervals to wait
     */
    void wait_for_monitor(int intervals = 1);

    /**
     * First sleep a given number of seconds, then wait for monitor. This is required in some cases
     * where the test needs to wait for some external effect along with the monitor.
     *
     * @param sleep_s Sleep time in seconds
     * @param intervals Monitor intervals
     */
    void sleep_and_wait_for_monitor(int sleep_s, int intervals);

    /**
     * @brief Check whether log matches a pattern.
     *
     * @param pattern The `grep` compatible pattern.
     *
     * @return True, if the pattern is found, false otherwise.
     *
     */
    bool log_matches(const char* pattern) const
    {
        return log_matches(std::string(pattern));
    }

    bool log_matches(std::string pattern) const;

    mxt::CmdResult ssh_output(const std::string& cmd, bool sudo = true);

    int ssh_node(const std::string& cmd, bool sudo);

    int  ssh_node_f(bool sudo, const char* format, ...) mxb_attribute((format(printf, 3, 4)));
    bool copy_to_node(const char* src, const char* dest);
    bool copy_to_node(const std::string& src, const std::string& dest)
    {
        return copy_to_node(src.c_str(), dest.c_str());
    }
    bool copy_from_node(const char* src, const char* dest);
    bool copy_from_node(const std::string& src, const std::string& dest)
    {
        return copy_from_node(src.c_str(), dest.c_str());
    }

    /**
     * Copy rules file for firewall filter to MaxScale machine.
     *
     * @param rules_name Rule file source filename
     * @param rules_dir Rule file source directory
     */
    void copy_fw_rules(const std::string& rules_name, const std::string& rules_dir);

    /**
     * Check if MaxScale process is running or stopped. Wrong status is a test failure.
     *
     * @param expected True if expected to be running
     */
    void expect_running_status(bool expected);

    bool reinstall(const std::string& target, const std::string& mdbci_config_name);

    bool use_valgrind() const;
    bool prepare_for_test();
    void write_env_vars();

    mxt::Node& vm_node();

    /**
     * Check that server status is as expected. Increments global error counter if differences found.
     *
     * @param expected_status Expected server statuses. Each status should be a bitfield of values defined
     * in the ServerInfo-class.
     */
    void check_servers_status(const std::vector<mxt::ServerInfo::bitfield>& expected_status);

    void check_print_servers_status(const std::vector<mxt::ServerInfo::bitfield>& expected_status);

    void alter_monitor(const std::string& mon_name, const std::string& setting, const std::string& value);
    void alter_service(const std::string& svc_name, const std::string& setting, const std::string& value);
    void alter_server(const std::string& srv_name, const std::string& setting, const std::string& value);

    /**
     * Write a message to MaxScale log.
     *
     * @param str The message
     */
    void write_in_log(std::string&& str);

    /**
     * Controls whether leak checks are done on shutdown.
     *
     * @param value If true, leak checks are made (default). If not, any leaks are ignored and will not cause
     *              a test failure.
     */
    void leak_check(bool value)
    {
        m_leak_check = value;
    }

    void delete_logs_and_rtfiles();
    void create_report();

private:
    bool m_use_ipv6 {false};    /**< Default to ipv6-addresses */
    bool m_ssl {false};         /**< Use ssl when connecting to MaxScale */
    bool m_leak_check {true};

    int  m_valgrind_log_num {0};    /**< Counter for Maxscale restarts to avoid Valgrind log overwriting */
    bool m_use_valgrind {false};    /**< Run MaxScale under Valgrind? */
    bool m_use_callgrind {false};   /**< Run MaxScale under Valgrind with --callgrind option */

    std::string m_rest_user {"admin"};
    std::string m_rest_pw {"mariadb"};
    std::string m_rest_ip {"127.0.0.1"};
    std::string m_rest_port {"8989"};

    std::string m_user_name;        /**< User name to access backend nodes */
    std::string m_password;         /**< Password to access backend nodes */
    std::string m_cnf_path;         /**< Maxscale configuration file path */
    std::string m_local_maxctrl;    /**< Path to MaxCtrl */

    std::string m_log_dir {"/var/log/maxscale"};/**< Where is MaxScale writing its logs */
    std::string m_log_storage_dir;              /**< Where to store logs from this MaxScale after test */

    mxt::SharedData&           m_shared;
    std::unique_ptr<mxt::Node> m_vmnode;

    mxt::TestLogger& log() const;
    bool             verbose() const;
    mxt::CmdResult   curl_rest_api(const std::string& path);
    int              start_local_maxscale();
    void             set_log_dir(std::string&& str);
    mxt::CmdResult   vmaxctrl(Expect expect, const char* format, va_list args);
};
}
