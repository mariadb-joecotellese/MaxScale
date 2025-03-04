/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <maxtest/ccdefs.hh>
#include <maxbase/ini.hh>
#include <maxtest/mariadb_func.hh>
#include <maxtest/log.hh>

typedef std::set<std::string> StringSet;

namespace maxtest
{
struct SharedData;
class TestLogger;

/**
 * Abstract class that interfaces with a test node, such as one running MaxScale or a backend server.
 * Implementations of some commands (start, stop etc) of different node types (local, docker, remote)
 * are separated to their own classes.
 */
class Node
{
public:
    Node(SharedData& shared, std::string name, std::string mariadb_executable);
    Node(const Node&) = delete;

    virtual ~Node() = default;

    // Node type. Required in cases where other classes need to handle implementation differences.
    enum class Type {REMOTE, DOCKER, LOCAL};

    virtual Type type() const = 0;
    virtual bool configure(const mxb::ini::map_result::ConfigSection& cnf) = 0;

    /**
     * Init or check a direct connection to the node.
     */
    virtual bool init_connection() = 0;

    enum class CmdPriv
    {
        NORMAL, SUDO
    };

    /**
     * Run a command on the Node. No output.
     *
     * @param cmd Command string
     * @param priv Sudo or normal user
     * @return Return code
     */
    virtual int run_cmd(const std::string& cmd, CmdPriv priv) = 0;

    int run_cmd(const std::string& cmd);
    int run_cmd_sudo(const std::string& cmd);

    /**
     * Run a command on the node. Fetches output. Should only be used for singular commands,
     * as "sudo" only affects the first command in the string.
     *
     * @param cmd Command string
     * @param priv Sudo or normal user
     * @return Return code and command output
     */
    virtual mxt::CmdResult run_cmd_output(const std::string& cmd, CmdPriv priv) = 0;
    mxt::CmdResult         run_cmd_output(const std::string& cmd);

    mxt::CmdResult run_cmd_output_sudo(const std::string& cmd);
    mxt::CmdResult run_cmd_output_sudof(const char* fmt, ...) mxb_attribute((format (printf, 2, 3)));

    /**
     * Run an sql-query on the node so that its origin is the node itself.
     *
     * @param sql The query. Should not contain single quotes (')
     * @return Result struct. The output contains the result rows. Columns are separated by tab.
     */
    mxt::CmdResult run_sql_query(const std::string& sql);

    /**
     * Copy a local file to the node.
     *
     * @param src Source file on the local filesystem
     * @param dest Destination file on the remote file system
     * @return True on success
     */
    virtual bool copy_to_node(const std::string& src, const std::string& dest) = 0;

    /**
     * Copy a local file to the node with sudo privs. Required when the destination directory
     * is restricted. Implemented by first scp:ing the file to home dir, then copying it to destination
     * and finally deleting the temporary.
     *
     * @param src Source file on the local filesystem
     * @param dest Destination file on the remote file system
     * @return True on success
     */
    bool copy_to_node_sudo(const std::string& src, const std::string& dest);

    virtual bool copy_from_node(const std::string& src, const std::string& dest) = 0;

    void delete_from_node(const std::string& filepath);

    void add_linux_user(const std::string& name, const std::string& pw);
    void remove_linux_user(const std::string& name);
    void add_linux_group(const std::string& grp_name, const std::vector<std::string>& members);
    void remove_linux_group(const std::string& grp_name);

    const char* ip4() const;
    const char* priv_ip() const;
    const char* hostname() const;
    const char* access_user() const;
    const char* access_homedir() const;
    const char* access_sudo() const;
    const char* sshkey() const;
    const char* name() const;

    const std::string& ip4s() const;
    const std::string& ip6s() const;

    TestLogger& log();

    /**
     * Write node network info to environment variables. This is mainly needed by script-type tests.
     */
    void write_node_env_vars();

    void set_start_stop_reset_cmds(std::string&& start, std::string&& stop, std::string&& reset);

    const std::string m_name;   /**< E.g. "node_001" */

    bool is_remote() const;

    bool verbose() const;

    virtual bool start_process(std::string_view params) = 0;
    virtual bool stop_process() = 0;
    virtual bool reset_process_datafiles() = 0;

protected:
    bool base_configure(const mxb::ini::map_result::ConfigSection& cnf);

    SharedData& m_shared;

    std::string m_ip4;          /**< IPv4-address */
    std::string m_ip6;          /**< IPv6-address */
    std::string m_private_ip;   /**< Private IP-address for AWS */
    std::string m_hostname;     /**< Hostname */

    std::string m_username; /**< Unix user name to access nodes via ssh */
    std::string m_homedir;  /**< Home directory of username */
    std::string m_sudo;     /**< empty or "sudo " */
    std::string m_sshkey;   /**< Path to ssh key */

    std::string m_start_proc_cmd;       /**< Command to start MariaDB Server/MaxScale */
    std::string m_stop_proc_cmd;        /**< Command to stop MariaDB Server/MaxScale */
    std::string m_reset_data_cmd;       /**< Command to remove MariaDB Server/MaxScale data files */

private:
    std::string m_mariadb_executable;
};

/**
 * Remote node. Runs commands through ssh, with sudo privs.
 */
class VMNode final : public Node
{
public:
    VMNode(SharedData& shared, std::string name, std::string mariadb_executable);
    ~VMNode();

    Type type() const override;
    bool init_connection() override;
    void close_ssh_master();
    bool configure(const mxt::NetworkConfig& nwconfig);
    bool configure(const mxb::ini::map_result::ConfigSection& cnf) override;

    int            run_cmd(const std::string& cmd, CmdPriv priv) override;
    mxt::CmdResult run_cmd_output(const std::string& cmd, CmdPriv priv) override;

    bool copy_to_node(const std::string& src, const std::string& dest) override;
    bool copy_from_node(const std::string& src, const std::string& dest) override;

    bool start_process(std::string_view params) override;
    bool stop_process() override;
    bool reset_process_datafiles() override;

private:
    std::string m_ssh_cmd_p1;                   /**< Start of remote command string */
    FILE*       m_ssh_master_pipe{nullptr};     /**< Master ssh pipe. Kept open for ssh multiplex */
};

/**
 * Local server or MaxScale. Can run non-sudo commands. Cannot copy files.
 */
class LocalNode final : public Node
{
public:
    LocalNode(SharedData& shared, std::string name, std::string mariadb_executable);

    Type type() const override;
    bool configure(const mxb::ini::map_result::ConfigSection& cnf) override;
    bool init_connection() override;

    int            run_cmd(const std::string& cmd, CmdPriv priv) override;
    mxt::CmdResult run_cmd_output(const std::string& cmd, CmdPriv priv) override;

    bool copy_to_node(const std::string& src, const std::string& dest) override;
    bool copy_from_node(const std::string& src, const std::string& dest) override;

    bool start_process(std::string_view params) override;
    bool stop_process() override;
    bool reset_process_datafiles() override;
};

/**
 * Docker node.
 */
class DockerNode final : public Node
{
public:
    DockerNode(SharedData& shared, std::string name, std::string mariadb_executable);

    Type type() const override;
    bool configure(const mxb::ini::map_result::ConfigSection& cnf) override;

    bool init_connection() override;
    bool recreate_container();

    int            run_cmd(const std::string& cmd, CmdPriv priv) override;
    mxt::CmdResult run_cmd_output(const std::string& cmd, CmdPriv priv) override;

    bool copy_to_node(const std::string& src, const std::string& dest) override;
    bool copy_from_node(const std::string& src, const std::string& dest) override;

    bool start_process(std::string_view params) override;
    bool stop_process() override;
    bool reset_process_datafiles() override;
    bool is_new() const;

private:
    std::string m_container;    /**< Container name */
    std::string m_volume;       /**< Volume name */
    std::string m_volume_dest;  /**< Volume container mount dir */
    std::string m_image;        /**< Image name */
    bool        m_new {false};  /**< Was container just created? */

    bool exec_cmd(const std::string& cmd);
};

std::unique_ptr<mxt::Node> create_node(const mxb::ini::map_result::Configuration::value_type& config,
                                       mxt::SharedData& shared);
}

class Nodes
{
public:
    virtual ~Nodes() = default;

    const char* ip_private(int i = 0) const;

    bool verbose() const;

    /**
     * @brief mdbci_node_name
     * @param node
     * @return name of the node in MDBCI format
     */
    std::string mdbci_node_name(int node);

    mxt::CmdResult ssh_output(const std::string& cmd, int node = 0, bool sudo = true);

    /**
     * @brief executes shell command on the node using ssh
     * @param index number of the node (index)
     * @param ssh command to execute
     * @param sudo if true the command is executed with root privelegues
     * @return exit code of the coomand
     */
    int ssh_node(int node, const std::string& ssh, bool sudo);
    int ssh_node_f(int node, bool sudo, const char* format, ...) mxb_attribute((format(printf, 4, 5)));

    /**
     * @brief Copy a local file to the Node i machine
     * @param i Node index
     * @param src Source file on the local filesystem
     * @param dest Destination file on the remote file system
     * @return exit code of the system command or 1 in case of i > N
     */
    int copy_to_node(int i, const char* src, const char* dest);

    int copy_from_node(int i, const char* src, const char* dest);

    void write_env_vars();

    int n_nodes() const;

protected:
    mxt::SharedData& m_shared;

    Nodes(mxt::SharedData* shared);

    const char* ip4(int i) const;
    const char* ip6(int i) const;
    const char* hostname(int i) const;

    const char* access_user(int i) const;
    const char* access_homedir(int i) const;
    const char* access_sudo(int i) const;
    const char* sshkey(int i) const;

    mxt::Node*       node(int i);
    const mxt::Node* node(int i) const;

    void clear_vms();
    bool add_node(const mxt::NetworkConfig& nwconfig, const std::string& name);
    void add_node(std::unique_ptr<mxt::Node> node);

    virtual const char* mariadb_executable() const
    {
        return "mariadb";
    }

private:
    std::vector<std::unique_ptr<mxt::Node>> m_vms;
};
