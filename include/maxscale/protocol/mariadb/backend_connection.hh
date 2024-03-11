/*
 * Copyright (c) 2019 MariaDB Corporation Ab
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

#include <maxscale/ccdefs.hh>
#include <maxscale/protocol/mariadb/protocol_classes.hh>
#include <maxscale/protocol/mariadb/mysql.hh>

#include <queue>

class MariaDBUserCache;

class MariaDBBackendConnection : public mxs::BackendConnection
{
public:
    using Iter = GWBUF::iterator;

    static std::unique_ptr<MariaDBBackendConnection>
    create(MXS_SESSION* session, mxs::Component* component, SERVER& server);

    ~MariaDBBackendConnection() override;

    void ready_for_reading(DCB* dcb) override;
    void error(DCB* dcb, const char* errmsg) override;

    void     finish_connection() override;
    uint64_t can_reuse(MXS_SESSION* session) const override;
    bool     reuse(MXS_SESSION* session, mxs::Component* upstream, uint64_t reuse_type) override;
    bool     established() override;
    void     set_to_pooled() override;
    void     ping() override;
    bool     can_close() const override;
    bool     is_idle() const override;
    size_t   sizeof_buffers() const override;
    json_t*  diagnostics() const override;

    void              set_dcb(DCB* dcb) override;
    const BackendDCB* dcb() const override;
    BackendDCB*       dcb() override;

    uint64_t        thread_id() const;
    mxs::Component* upstream() const override;

    bool routeQuery(GWBUF&& buffer) override;

private:
    enum class State
    {
        HANDSHAKING,            /**< Handshaking with backend */
        AUTHENTICATING,         /**< Authenticating with backend */
        CONNECTION_INIT,        /**< Sending connection init file contents */
        SEND_DELAYQ,            /**< Sending contents of delay queue */
        ROUTING,                /**< Ready to route queries */
        SEND_CHANGE_USER,       /**< Sending a COM_CHANGE_USER */
        READ_CHANGE_USER,       /**< Reading the response to a COM_CHANGE_USER */
        RESET_CONNECTION,       /**< Reset the connection with a COM_CHANGE_USER */
        RESET_CONNECTION_FAST,  /**< Fast path for connection reset with COM_RESET_CONNECTION */
        PINGING,                /**< Pinging backend server */
        POOLED,                 /**< The connection is in pool and should not route replies */
        SEND_HISTORY,           /**< Sending stored session command history */
        READ_HISTORY,           /**< Reading results of history execution */
        PREPARE_PS,             /**< Executing a COM_STMT_PREPARE */
        FAILED,                 /**< Handshake/authentication failed */
    };

    enum class HandShakeState
    {
        SEND_PROHY_HDR, /**< Send proxy protocol header */
        EXPECT_HS,      /**< Expecting initial server handshake */
        START_SSL,      /**< Send SSLRequest and start SSL */
        SSL_NEG,        /**< Negotiating SSL */
        SEND_HS_RESP,   /**< Send handshake response */
        COMPLETE,       /**< Handshake complete */
        FAIL,           /**< Handshake failed */
    };

    enum class StateMachineRes
    {
        IN_PROGRESS,// The SM should be called again once more data is available.
        DONE,       // The SM is complete for now, the protocol may advance to next state.
        ERROR,      // The SM encountered an error. The connection should be closed.
    };

    enum ReuseType
    {
        CHANGE_USER      = 1,               // Only used if necessary, slower than a COM_RESET_CONNECTION
        RESET_CONNECTION = OPTIMAL_REUSE,   // Faster than COM_CHANGE_USER but still requires a roundtrip
    };

    // Information about executed prepared statements
    struct PSInfo
    {
        uint32_t real_id;                   // The actual ID we use when communicating with the database
        uint16_t n_params {0};              // Number of parameters, used for COM_STMT_EXECUTE
        bool     exec_metadata_sent {false};// Whether COM_STMT_EXECUTE metadata was sent
    };

    State          m_state {State::HANDSHAKING};                /**< Connection state */
    HandShakeState m_hs_state {HandShakeState::SEND_PROHY_HDR}; /**< Handshake state */

    SERVER&                  m_server;          /**< Connected backend server */
    mariadb::SBackendAuth    m_authenticator;   /**< Authentication plugin */
    mariadb::BackendAuthData m_auth_data;       /**< Data shared with auth plugin */

    /**
     * Packets received from router while the connection was busy handshaking/authenticating.
     * Sent to server once connection is ready. */
    std::vector<GWBUF> m_delayed_packets;

    /**
     * Contains information about custom connection initialization queries.
     */
    struct InitQueryStatus
    {
        enum class State
        {
            SENDING,
            RECEIVING,
        };
        State state {State::SENDING};

        int ok_packets_expected {0};    /**< OK packets expected in total */
        int ok_packets_received {0};    /**< OK packets received so far */
    };
    InitQueryStatus m_init_query_status;

    MariaDBBackendConnection(SERVER& server);

    StateMachineRes handshake();
    StateMachineRes authenticate(GWBUF&& buffer);
    StateMachineRes send_connection_init_queries();
    bool            send_delayed_packets();
    void            normal_read();

    void            send_history();
    StateMachineRes read_history_response();
    void            handle_history_mismatch();

    void            send_change_user_to_backend();
    StateMachineRes read_change_user(GWBUF&& buffer);
    void            read_reset_conn_resp(GWBUF&& buffer);

    bool  send_proxy_protocol_header();
    GWBUF create_change_user_packet();
    GWBUF create_reset_connection_packet();
    bool  read_com_ping_response();
    void  do_handle_error(DCB* dcb, const std::string& errmsg,
                          mxs::ErrorType type = mxs::ErrorType::TRANSIENT);
    void prepare_for_write(const GWBUF& buffer);
    void process_stmt_execute(GWBUF& buffer, uint32_t id, PSInfo& info);

    GWBUF track_response(GWBUF& buffer);
    bool  read_backend_handshake(GWBUF&& buffer);
    void  handle_error_response(const GWBUF& buffer);
    bool  session_ok_to_route(DCB* dcb);

    int      gw_decode_mysql_server_handshake(uint8_t* payload);
    GWBUF    create_ssl_request_packet() const;
    GWBUF    create_hs_response_packet(bool with_ssl);
    uint8_t* write_capabilities(uint8_t* buffer) const;
    bool     expecting_reply() const;
    bool     capability_mismatch() const;

    std::string create_response_mismatch_error();

    uint32_t create_capabilities(bool with_ssl) const;
    GWBUF    process_packets(GWBUF& result);
    void     process_one_packet(Iter it, Iter end, uint32_t len);
    void     process_reply_start(Iter it, Iter end);
    void     process_result_start(Iter it, Iter end);
    void     process_ps_response(Iter it, Iter end);
    void     process_ok_packet(Iter it, Iter end);
    void     update_error(Iter it, Iter end);

    const MariaDBUserCache* user_account_cache();

    // Helper for getting the shared session data
    MYSQL_session* mysql_session() const
    {
        return m_auth_data.client_data;
    }

    bool use_deprecate_eof() const
    {
        return mysql_session()->client_capabilities() & GW_MYSQL_CAPABILITIES_DEPRECATE_EOF;
    }

    // Contains the necessary information required to track queries
    struct TrackedQuery
    {
        explicit TrackedQuery(const GWBUF& buffer);

        uint32_t payload_len = 0;
        uint8_t  command = 0;
        bool     opening_cursor = false;
        bool     collect_rows = false;
        uint32_t id = 0;
    };

    void track_query(const TrackedQuery& query);

    /**
     * Set associated client protocol session and upstream. Should be called after creation or when swapping
     * sessions. Also initializes authenticator plugin.
     *
     * @param session The new session to read client data from
     * @param upstream The new upstream to send server replies to
     */
    void assign_session(MXS_SESSION* session, mxs::Component* upstream);

    static std::string to_string(State auth_state);

    uint64_t    m_thread_id {0};                    /**< Backend thread id, received in backend handshake */
    uint64_t    m_capabilities {0};                 /**< Connection capability bits */
    std::string m_account;                          /**< The user@host that last used this connection */
    std::string m_db;                               /**< The database last used with this connection */
    bool        m_collect_result {false};           /**< Collect the next result set as one buffer */
    bool        m_skip_next {false};
    uint64_t    m_num_coldefs {0};
    GWBUF       m_collectq;             /**< Used to collect results when resultset collection is requested */
    int64_t     m_ps_packets {0};
    bool        m_opening_cursor = false;   /**< Whether we are opening a cursor */
    bool        m_large_query = false;
    mxs::Reply  m_reply;

    uint32_t m_mxs_capabilities {0};            /**< Client capabilities sent to server */
    uint32_t m_server_capabilities {0};         /**< Server capabilities */
    uint32_t m_server_extra_capabilities {0};   /**< Extra MariaDB capabilities */

    std::queue<TrackedQuery> m_track_queue;

    // The mapping of COM_STMT_PREPARE IDs we sent upstream to the actual IDs that the backend sent us
    std::unordered_map<uint32_t, PSInfo> m_ps_map;

    // Whether to collect the rows from the next resultset
    bool m_collect_rows {false};

    mxs::Component* m_upstream {nullptr};       /**< Upstream component, typically a router */
    MXS_SESSION*    m_session {nullptr};        /**< Generic session */
    BackendDCB*     m_dcb {nullptr};            /**< Dcb used by this protocol connection */

    std::unique_ptr<mxs::History::Subscriber> m_subscriber;
};
