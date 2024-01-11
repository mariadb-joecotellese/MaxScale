/*
 * Copyright (c) 2019 MariaDB Corporation Ab
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
#pragma once

#include <maxsql/ccdefs.hh>

#include <memory>
#include <string>
#include <maxbase/ssl.hh>
#include <maxbase/queryresult.hh>

#include <mysql.h>

namespace maxsql
{

class MariaDBQueryResult;
struct MariaDBOkResult;
struct MariaDBErrorResult;

/**
 * Convenience class for working with Connector-C.
 */
class MariaDB
{
public:
    MariaDB() = default;
    virtual ~MariaDB();
    MariaDB(const MariaDB& rhs) = delete;
    MariaDB& operator=(const MariaDB& rhs) = delete;

    MariaDB(MariaDB&& conn) noexcept;
    MariaDB& operator=(MariaDB&& rhs) noexcept;

    struct ConnectionSettings
    {
        friend class MariaDB;

        std::string user;
        std::string password;
        std::string alternate_password;

        std::string local_address;
        std::string plugin_dir;

        mxb::SSLConfig ssl;
        std::string    ssl_version;

        int  timeout {0};
        bool multiquery {true};
        bool auto_reconnect {false};
        bool clear_sql_mode {false};
        bool local_infile {true};

        std::string charset;

    private:
        enum class ProxyHeaderMode {NONE, LOCAL_TEXT, LOCAL_BIN, CUSTOM};
        ProxyHeaderMode proxy_header_mode {ProxyHeaderMode::NONE};

        std::vector<uint8_t> custom_proxy_header;
    };

    struct VersionInfo
    {
        uint64_t    version {0};
        uint64_t    capabilities {0};
        std::string info;
    };

    static constexpr unsigned int INTERNAL_ERROR = 1;
    static constexpr unsigned int USER_ERROR = 2;

    /**
     * Set the default plugin directory.
     *
     * This is the directory that is used if the configuration itself doesn't override it. The default value,
     * if not set, is /usr/lib/mysql/plugin/.
     *
     * @param dir The default for the plugin directory
     */
    static void set_default_plugin_dir(std::string&& dir);

    /**
     * Open a new database connection.
     *
     * @param host Server host/ip
     * @param port Server port
     * @param db Database to connect to
     * @return True on success
     */
    bool open(const std::string& host, int port, const std::string& db = "");

    /**
     * Open, with extra port. If extra port is greater than 0, try connecting with it. If the connection
     * fails due to too many connections or if the server is not listening, then try to connect to
     * normal port.
     *
     * @param host Server host/ip
     * @param port Server port
     * @param extra_port Extra port
     * @param db Database to connect to
     * @return True on success
     */
    bool open_extra(const std::string& host, int port, int extra_port, const std::string& db = "");

    /**
     * Closes any existing connection.
     */
    void close();

    /**
     * Run a query which returns no data.
     *
     * @param query SQL to run
     * @return True on success
     */
    bool cmd(const std::string& query);

    /**
     * Run a query which may return data.
     *
     * @param query SQL to run
     * @return Query results on success, null otherwise
     */
    std::unique_ptr<mxb::QueryResult> query(const std::string& query);

    /**
     * Run multiple queries as one. Each should return data. Multiqueries should be enabled in connection
     * settings.
     *
     * @param queries Array of queries. Will be executed as a single multiquery. Each individual query
     * should end in a semicolon.
     * @return Results from every query. If any kind of error occurs, returns an empty vector.
     */
    std::vector<std::unique_ptr<mxb::QueryResult>> multiquery(const std::vector<std::string>& queries);

    enum class ResultType
    {
        OK, ERROR, RESULTSET, NONE
    };
    ResultType streamed_query(const std::string& query);
    ResultType next_result();
    ResultType current_result_type();

    std::unique_ptr<mxq::MariaDBQueryResult> get_resultset();
    std::unique_ptr<mxq::MariaDBOkResult>    get_ok_result();
    std::unique_ptr<mxq::MariaDBErrorResult> get_error_result();

    /**
     * Ping the server.
     *
     * @return True on success
     */
    bool ping();

    /**
     * Test if the connection is still alive.
     *
     * This attempts to test if the connection is alive without actually pinging the server.
     *
     * @return True if the TCP socket, if one was used, was still readable
     */
    bool still_alive();

    /**
     * Run COM_CHANGE_USER. Connection settings are not changed.
     *
     * @param user New username
     * @param pw New password
     * @param db New database
     * @return True on success
     */
    bool change_user(const std::string& user, const std::string& pw, const std::string& db);

    /**
     * Reconnect to the server.
     *
     * @return True on success
     */
    bool reconnect();

    /**
     * Get latest error.
     *
     * @return Error string
     */
    const char* error() const;

    /**
     * Get latest error number.
     *
     * @return Error code
     */
    int64_t errornum() const;

    /**
     * Get reference to connection settings. The settings are used when opening a connection.
     *
     * @return Connection settings reference
     */
    ConnectionSettings& connection_settings();

    /**
     * Set proxy header to indicate a non-proxied connection. Useful when connecting to a server which
     * demands a proxy header. Sends a text header (V1).
     */
    void set_local_text_proxy_header();

    /**
     * Sends a binary mode (V2) local proxy header.
     */
    void set_local_bin_proxy_header();

    /**
     * Set a custom proxy header.
     *
     * @param header The proxy protocol header
     */
    void set_custom_proxy_header(std::vector<uint8_t>&& header);

    VersionInfo version_info() const;
    bool        is_open() const;

    /**
     * Get thread ID of this connection
     *
     * The connection must be open when this function is called.
     *
     * @return The thread ID of the connection
     */
    uint32_t thread_id() const;

    /**
     * Set connection timeouts
     *
     * The timeout will affect network reads and writes and reconnections if they take place.
     *
     * @param timeout Timeout in seconds
     */
    void set_timeout(int timeout);

private:
    void clear_errors();
    bool run_query(const std::string& query, const std::function<bool()>& result_handler);
    void update_multiq_result_type();

    MYSQL* m_conn {nullptr};

    ResultType m_current_result_type {ResultType::NONE};
    MYSQL_RES* m_current_result {nullptr};

    std::string m_errormsg;
    int64_t     m_errornum {0};

    ConnectionSettings m_settings;
};

/**
 * QueryResult implementation for the MariaDB-class.
 */
class MariaDBQueryResult : public mxb::QueryResult
{
public:
    MariaDBQueryResult(const MariaDBQueryResult&) = delete;
    MariaDBQueryResult& operator=(const MariaDBQueryResult&) = delete;

    /**
     * Construct a new resultset.
     *
     * @param resultset The results from mysql_query(). Must not be NULL.
     */
    explicit MariaDBQueryResult(MYSQL_RES* resultset);

    ~MariaDBQueryResult() override;

    /**
     * Advance to next row. Affects all result returning functions.
     *
     * @return True if the next row has data, false if the current row was the last one.
     */


    /**
     * How many columns the result set has.
     *
     * @return Column count
     */
    int64_t get_col_count() const override;

    /**
     * How many rows does the result set have?
     *
     * @return The number of rows
     */
    int64_t get_row_count() const override;

    const char* const* rowdata() const;

    struct Field
    {
        enum class Type {STRING, INTEGER, FLOAT, NUL, OTHER};

        std::string name;
        std::string table;
        std::string schema;
        std::string catalog;
        std::string sql_type;
        int64_t     length;
        int64_t     decimals;
        int64_t     flags;
        Type        type;
    };
    using Fields = std::vector<Field>;
    const Fields& fields() const;

    std::string get_field_name(int64_t idx) const override;

private:
    const char* row_elem(int64_t column_ind) const override;
    bool        advance_row() override;
    void        prepare_fields_info();

    static std::vector<std::string> column_names(MYSQL_RES* results);

    MYSQL_RES*         m_resultset {nullptr};   /**< Underlying result set, freed at dtor */
    const char* const* m_rowdata {nullptr};     /**< Data for current row */
    Fields             m_fields_info;           /**< Field names and types */
};

struct MariaDBOkResult
{
    uint64_t insert_id {0};
    uint32_t warnings {0};
    uint64_t affected_rows {0};
};

struct MariaDBErrorResult
{
    uint32_t    error_num {0};
    std::string error_msg;
    std::string sqlstate;
};
}
