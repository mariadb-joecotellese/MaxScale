/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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

/**
 * @file dcb.h  The Descriptor Control Block
 */

#include <maxscale/ccdefs.hh>

#include <openssl/ossl_typ.h>
#include <netinet/in.h>

#include <maxbase/json.hh>
#include <maxbase/worker.hh>
#include <maxscale/authenticator.hh>
#include <maxscale/buffer.hh>
#include <maxscale/clock.hh>
#include <maxscale/dcbhandler.hh>
#include <maxscale/modinfo.hh>
#include <maxscale/protocol2.hh>
#include <maxscale/target.hh>

#include <memory>

class MXS_SESSION;
class SERVER;
class SERVICE;

namespace maxscale
{
class ClientConnection;
class BackendConnection;
class SSLContext;
}

/**
 * Descriptor Control Block
 *
 * A wrapper for a socket descriptor within MaxScale. For each client
 * session there will be one ClientDCB and several BackendDCBs.
 */
class DCB : public mxb::Pollable
{
public:
    static const int FD_CLOSED = -1;

    using Handler = DCBHandler;

    class Manager
    {
    public:
        /**
         * Called by DCB when created.
         */
        virtual void add(DCB* dcb) = 0;

        /**
         * Called by DCB when destroyed.
         */
        virtual void remove(DCB* dcb) = 0;

        /**
         * Called by DCB when it needs to be destroyed.
         */
        virtual void destroy(DCB* dcb) = 0;

    protected:
        static void call_destroy(DCB* dcb)
        {
            dcb->destroy();
        }
    };

    enum class Role
    {
        CLIENT,         /*< Serves dedicated client */
        BACKEND,        /*< Serves back end connection */
    };

    enum class State
    {
        CREATED,        /*< Created but not added to the poll instance */
        POLLING,        /*< Added to the poll instance */
        DISCONNECTED,   /*< Socket closed */
        NOPOLLING       /*< Removed from the poll instance */
    };

    enum class Reason
    {
        HIGH_WATER,     /*< Cross high water mark */
        LOW_WATER       /*< Cross low water mark */
    };

    enum class SSLState
    {
        HANDSHAKE_UNKNOWN,  /*< The DCB has unknown SSL status */
        HANDSHAKE_REQUIRED, /*< SSL handshake is needed */
        ESTABLISHED,        /*< The SSL connection is in use */
        HANDSHAKE_FAILED    /*< The SSL handshake failed */
    };

    /**
     * @return The unique identifier of the DCB.
     */
    uint64_t uid() const
    {
        return m_uid;
    }

    /**
     * File descriptor of DCB.
     *
     * Accessing and using the file descriptor directly should only be
     * used as a last resort, as external usage may break the assumptions
     * of the DCB.
     *
     * @return The file descriptor.
     */
    int fd() const
    {
        return m_fd;
    }

    /**
     * The sockaddr struct of the connected peer
     */
    const sockaddr_storage& ip() const
    {
        return m_ip;
    }

    void set_remote_ip_port(const sockaddr_storage& ip, std::string&& ip_str);

    /**
     * @return The remote host of the DCB.
     */
    const std::string& remote() const
    {
        return m_remote;
    }

    virtual std::string whoami() const = 0;

    /**
     * @return The host of the client that created this DCB.
     */
    const std::string& client_remote() const
    {
        return m_client_remote;
    }

    /**
     * @return The role of the DCB.
     */
    Role role() const
    {
        return m_role;
    }

    /**
     * @return The session of the DCB.
     */
    MXS_SESSION* session() const
    {
        return m_session;
    }

    /**
     * @return The event handler of the DCB.
     */
    Handler* handler() const
    {
        return m_handler;
    }

    /**
     * Set the handler of the DCB.
     *
     * @param handler  The new handler of the DCB.
     */
    void set_handler(Handler* handler)
    {
        m_handler = handler;
    }

    /**
     * @return The state of the DCB.
     */
    State state() const
    {
        return m_state;
    }

    /**
     * @return The protocol of the DCB.
     */
    virtual mxs::ProtocolConnection* protocol() const = 0;

    /**
     * Clears the DCB; all queues and callbacks are freed and the session
     * pointer is set to null.
     */
    void clear();

    SERVICE* service() const;

    /**
     * @return True, if SSL has been enabled, false otherwise.
     */
    bool ssl_enabled() const
    {
        return m_encryption.handle != nullptr;
    }

    /**
     * Get current TLS cipher
     *
     * @return Current TLS cipher or empty string if SSL is not in use
     */
    std::string ssl_cipher() const;

    /**
     * @return The current SSL state.
     */
    SSLState ssl_state() const
    {
        return m_encryption.state;
    }

    void set_ssl_state(SSLState ssl_state)
    {
        m_encryption.state = ssl_state;
    }

    /**
     * Perform SSL handshake.
     *
     * @return -1 if an error occurred,
     *          0 if the handshaking is still ongoing and another call to ssl_handshake() is needed, and
     *          1 if the handshaking succeeded
     *
     * @see ClientDCB::ssl_handshake
     * @see BackendDCB::ssl_handshake
     */
    virtual int ssl_handshake() = 0;

    /**
     * Find the number of bytes available on the socket.
     *
     * @return -1 in case of error, otherwise the total number of bytes available.
     */
    int socket_bytes_readable() const;

    /**
     * Read data from socket.
     *
     * @param minbytes Return at least this many bytes.
     * @param maxbytes Return at most this many bytes. More can be read from socket.
     * @return First return value is success, second is data. Data can be empty if not enough bytes were
     * available. Success is false only on socket error.
     */
    std::tuple<bool, GWBUF> read(size_t minbytes, size_t maxbytes);

    /**
     * Same as above, with the difference that at most maxbytes (minus existing readq) is read from socket.
     * Should only be used in special situations. Will not self-trigger reads if more data may be available.
     */
    std::tuple<bool, GWBUF> read_strict(size_t minbytes, size_t maxbytes);

    /**
     * Append data to the write queue.
     *
     * @param data   The data to be appended to the write queue.
     * @return True if the data could be appended, false otherwise.
     */
    bool writeq_append(GWBUF&& data);

    /**
     * Drain the write queue of the DCB.
     *
     * This is called as part of the EPOLLOUT handling of a socket and will try
     * to send any buffered data from the write queue up until the point the
     * write would block.
     */
    void writeq_drain();

    /**
     * @return The current length of the writeq.
     */
    uint64_t writeq_len() const
    {
        return m_writeq.length();
    }

    // TODO: Should probably be made protected.
    virtual void shutdown() = 0;

    /**
     * Adds the DCB to the epoll set of the current worker, which in practice
     * means that the DCB will receive I/O events related to its file descriptor,
     * and that corresponding handlers will be called.
     *
     * NOTE: The current worker *must* be the owner of the DCB.
     *
     * @return True on success, false on error.
     */
    virtual bool enable_events();

    /**
     * Remove the DCB from the epoll set of the current worker, which in practice
     * means that the DCB will no longer receive I/O events related to its file
     * descriptor and that corresponding handlers will no longer be called.
     *
     * NOTE: The current worker *must* be the owner of the DCB.
     *
     * @return True on success, false on error.
     */
    virtual bool disable_events();

    /**
     * Control whether EPOLLIN events are handled for this DCB
     *
     * @param enable Whether EPOLLIN events are listened for
     *
     * @return True if the events were successfully modified
     */
    bool set_reads_enabled(bool val);

    /**
     * Add a callback to the DCB.
     *
     * @reason     When the callback should be called.
     * @cb         The callback.
     * @user_data  The data to provide to the callback when called.
     *
     * @return True, if the callback was added, false otherwise. False will
     *         be returned if the callback could not be added or if the callback
     *         has been added already.
     */
    bool add_callback(Reason reason, int (* cb)(DCB*, Reason, void*), void* user_data);

    /**
     * Remove all callbacks
     */
    void remove_callbacks();

    bool writeq_empty()
    {
        return m_writeq.empty();
    }

    const GWBUF& writeq() const
    {
        return m_writeq;
    }

    /**
     * @brief Returns the read queue of the DCB.
     *
     * @note The read queue remains the property of the DCB.
     *
     * @return A buffer of NULL if there is no read queue.
     */
    bool readq_empty()
    {
        return m_readq.empty();
    }

    const GWBUF& readq() const
    {
        return m_readq;
    }

    /**
     * Copy bytes from the readq without consuming.
     *
     * @param n_bytes Maximum number of bytes to copy
     * @param dst Destination buffer
     * @return Number of bytes actually copied
     */
    size_t readq_peek(size_t n_bytes, uint8_t* dst) const;

    /**
     * Prepend a buffer to the DCB's readqueue. Effectively unreads data so it may be read again.
     * The caller should call 'trigger_read_event' afterwards if the remaining data can be processed right
     * after. Care needs to be taken to avoid an endless loop.
     *
     * @param buffer The buffer to prepend
     */
    void unread(GWBUF&& buffer);

    int64_t last_read() const
    {
        return m_last_read;
    }

    int64_t last_write() const
    {
        return m_last_write;
    }

    std::chrono::milliseconds idle_time() const
    {
        // Only treat the connection as idle if there's no buffered data
        int64_t val = !m_writeq.empty() || !m_readq.empty() ? 0 :
            mxs_clock() - std::max(m_last_read, m_last_write);
        return std::chrono::milliseconds(val * 100);
    }

    int64_t seconds_idle() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(idle_time()).count();
    }

    bool is_open() const
    {
        return m_open;
    }

    bool hanged_up() const
    {
        return m_hanged_up;
    }

    bool is_polling() const
    {
        return m_state == State::POLLING;
    }

    /**
     * Will cause an EPOLL[R]HUP event to be delivered when the current
     * event handling finishes, just before the the control returns
     * back to epoll_wait().
     *
     * @note During one callback, only one event can be triggered.
     *       If there are multiple trigger_...()-calls, only the
     *       last one will be honoured.
     */
    void trigger_hangup_event();

    /**
     * Will cause an EPOLLIN event to be delivered when the current
     * event handling finishes, just before the the control returns
     * back to epoll_wait().
     *
     * @note During one callback, only one event can be triggered.
     *       If there are multiple trigger_...()-calls, only the
     *       last one will be honoured.
     */
    void trigger_read_event();

    /**
     * Will cause an EPOLLOUT event to be delivered when the current
     * event handling finishes, just before the the control returns
     * back to epoll_wait().
     *
     * @note During one callback, only one event can be triggered.
     *       If there are multiple trigger_...()-calls, only the
     *       last one will be honoured.
     */
    void trigger_write_event();

    struct CALLBACK
    {
        Reason           reason;    /*< The reason for the callback */
        int              (* cb)(DCB* dcb, Reason reason, void* userdata);
        void*            userdata;  /*< User data to be sent in the callback */
        struct CALLBACK* next;      /*< Next callback for this DCB */
    };

    static void destroy(DCB* dcb)
    {
        dcb->destroy();
    }

    bool is_fake_event() const
    {
        return m_is_fake_event;
    }

    mxb::Worker* owner() const
    {
        return m_owner;
    }

    /**
     * Sets the owner of the DCB.
     *
     * By default, the owner of a DCB is the routing worker that created it.
     * With this function, the owner of the DCB can be changed. Note that when
     * the owner is changed, the DCB must *not* be in a polling state.
     *
     * @param worker  The new owner of the DCB.
     */
    void set_owner(mxb::Worker* worker)
    {
        mxb_assert(m_state != State::POLLING);
        // Can't be polled, when owner is changed.
        mxb_assert(this->polling_worker() == nullptr);
        m_owner = worker;
    }

    /**
     * Sets the manager of the DCB.
     *
     * The manager of a DCB is set when the DCB is created. With this function
     * it can be changed, which it has to be if the session to which this DCB
     * belongs is moved from one routing worker to another.
     *
     * @param manager  The new manager.
     */
    void set_manager(Manager* manager)
    {
        if (m_manager)
        {
            m_manager->remove(this);
        }

        m_manager = manager;

        if (m_manager)
        {
            m_manager->add(this);
        }
    }

    void silence_errors()
    {
        m_silence_errors = true;
    }

    virtual size_t static_size() const = 0;

    virtual size_t varying_size() const
    {
        size_t rv = 0;

        rv += m_remote.capacity();
        rv += m_client_remote.capacity();

        auto* callback = m_callbacks;

        while (callback)
        {
            rv += sizeof(*callback);
            callback = callback->next;
        }

        rv += m_writeq.varying_size();
        rv += m_readq.varying_size();

        return rv;
    }

    size_t runtime_size() const
    {
        return static_size() + varying_size();
    }

    virtual mxb::Json get_memory_statistics() const;

protected:
    DCB(int fd,
        const sockaddr_storage& ip,
        const std::string& remote,
        Role role,
        MXS_SESSION* session,
        Handler* handler,
        Manager* manager);

    virtual ~DCB();
    void        destroy();
    static void close(DCB* dcb);

    bool create_SSL(const mxs::SSLContext& ssl);

    int  ssl_handshake_check_rval(int ssl_rval);
    bool verify_peer_host();

    /**
     * Release the instance from the associated session.
     *
     * @param session The session to release the DCB from.
     *
     * @return True, if the DCB was released and can be deleted, false otherwise.
     */
    virtual bool release_from(MXS_SESSION* session) = 0;

    struct Encryption
    {
        SSL*     handle = nullptr;                      /**< SSL handle for connection */
        SSLState state = SSLState::HANDSHAKE_UNKNOWN;   /**< Current state of SSL if in use */
        bool     read_want_write = false;
        bool     write_want_read = false;
        bool     verify_host = false;
        int      retry_write_size = 0;
    };

    mxb::Worker*     m_owner {nullptr};
    const uint64_t   m_uid; /**< Unique identifier for this DCB */
    int              m_fd;  /**< The descriptor */
    sockaddr_storage m_ip;  /**< remote IPv4/IPv6 address */

    const Role        m_role;           /**< The role of the DCB */
    std::string       m_remote;         /**< The remote host */
    const std::string m_client_remote;  /**< The host of the client that created this connection */

    MXS_SESSION*   m_session;               /**< The owning session */
    Handler*       m_handler;               /**< The event handler of the DCB */
    Manager*       m_manager;               /**< The DCB manager to use */
    const uint64_t m_high_water;            /**< High water mark of write queue */
    const uint64_t m_low_water;             /**< Low water mark of write queue */
    CALLBACK*      m_callbacks = nullptr;   /**< The list of callbacks for the DCB */

    State      m_state = State::CREATED;/**< Current state */
    int64_t    m_last_read;             /**< Last time the DCB received data */
    int64_t    m_last_write;            /**< Last time the DCB sent data */
    Encryption m_encryption;            /**< Encryption state */
    int        m_old_ssl_io_error {0};

    GWBUF    m_writeq;                  /**< Write Data Queue */
    GWBUF    m_readq;                   /**< Read queue for incomplete reads */
    uint32_t m_triggered_event = 0;     /**< Triggered event to be delivered to handler */
    uint32_t m_triggered_event_old = 0; /**< Triggered event before disabling events */

    bool m_hanged_up = false;       /**< Has the dcb been hanged up? */
    bool m_is_fake_event = false;
    bool m_skip_fast_fake_events = false;
    bool m_silence_errors = false;
    bool m_high_water_reached = false;      /**< High water mark throttle status */
    bool m_reads_enabled = true;

private:
    friend class Manager;

    bool    m_open {true};               /**< Is dcb still open, i.e. close() not called? */
    bool    m_incomplete_read { false }; /**< Was the reading incomplete? */
    int64_t m_read_amount { 0 };         /**< How much has been read in one poll handling callback? */

    enum class ReadLimit
    {
        RES_LEN,    /**< Maxbytes only affects the returned data. Socket can be read for more. */
        STRICT      /**< Exactly the given amount must be read from socket */
    };
    bool socket_read(size_t maxbytes, ReadLimit limit_type);
    bool socket_read_SSL(size_t maxbytes);

    void socket_write_SSL();
    void socket_write();

    std::tuple<uint8_t*, size_t> calc_read_limit_strict(size_t maxbytes);

    std::tuple<bool, GWBUF> read_impl(size_t minbytes, size_t maxbytes, ReadLimit limit_type);

    static void free(DCB* dcb);

    int      poll_fd() const override;
    uint32_t handle_poll_events(mxb::Worker* worker, uint32_t events, Pollable::Context context) override;
    uint32_t event_handler(uint32_t events);
    uint32_t process_events(uint32_t events);

    class FakeEventTask;
    friend class FakeEventTask;

    void call_callback(Reason reason);

    void add_event_via_loop(uint32_t ev);
    void add_event(uint32_t ev);

    void        log_ssl_errors(int ssl_io_error);
    std::string get_one_SSL_error(unsigned long ssl_errno);
};

class ClientDCB : public DCB
{
public:
    ~ClientDCB() override;

    static ClientDCB*
    create(int fd,
           const std::string& remote,
           const sockaddr_storage& ip,
           MXS_SESSION* session,
           std::unique_ptr<mxs::ClientConnection> protocol,
           DCB::Manager* manager);

    /**
     * @brief Return the port number this DCB is connected to
     *
     * @return Port number the DCB is connected to or -1 if information is not available
     */
    int port() const;

    mxs::ClientConnection* protocol() const override;

    /**
     * Initialize ssl-data and and start accepting ssl handshake.
     *
     * @return -1 if an error occurred,
     *          0 if the handshaking is still ongoing and another call to ssl_handshake() is needed, and
     *          1 if the handshaking succeeded
     */
    int ssl_start_accept();
    int ssl_handshake() override;

    void shutdown() override;

    static void close(ClientDCB* dcb);

    size_t static_size() const override
    {
        return sizeof(*this);
    }

    size_t varying_size() const override
    {
        size_t rv = DCB::varying_size();

        // TODO: m_protocol is owned by this, but that's the wrong way around,
        // TODO: so it is consciously ignored.

        return rv;
    }

    std::string whoami() const override;

protected:
    // Only for InternalDCB.
    ClientDCB(int fd,
              const std::string& remote,
              const sockaddr_storage& ip,
              DCB::Role role,
              MXS_SESSION* session,
              std::unique_ptr<mxs::ClientConnection> protocol,
              Manager* manager);

    // Only for Mock DCB.
    ClientDCB(int fd, const std::string& remote, DCB::Role role, MXS_SESSION* session);

private:
    ClientDCB(int fd,
              const std::string& remote,
              const sockaddr_storage& ip,
              MXS_SESSION* session,
              std::unique_ptr<mxs::ClientConnection> protocol,
              DCB::Manager* manager);

    bool release_from(MXS_SESSION* session) override;

    std::unique_ptr<mxs::ClientConnection> m_protocol;          /**< The protocol session */
};

class Session;
class BackendDCB : public DCB
{
public:
    class Manager : public DCB::Manager
    {
    public:
        /**
         * Attempt to move the dcb into the connection pool
         *
         * @param dcb  The dcb to move.
         * @return True, if the dcb was moved to the pool.
         *
         * If @c false is returned, the dcb should in most
         * cases be closed by the caller.
         */
        virtual bool move_to_conn_pool(BackendDCB* dcb) = 0;
    };

    static BackendDCB* connect(SERVER* server, MXS_SESSION* session, DCB::Manager* manager);

    /**
     * Resets the BackendDCB so that it can be reused.
     *
     * @param session  The new session for the DCB.
     */
    void reset(MXS_SESSION* session);

    mxs::BackendConnection* protocol() const override;
    Manager*                manager() const;

    /**
     * Hangup all BackendDCBs connected to a particular server.
     *
     * @param server  BackendDCBs connected to this server should be closed.
     */
    static void generate_hangup(const SERVER* server, const std::string& reason);
    void        shutdown() override;

    SERVER* server() const
    {
        return m_server;
    }

    /**
     * @return True, if the connection should use SSL.
     */
    bool using_ssl() const
    {
        return m_ssl.get();
    }

    /**
     * Initialize ssl-data and and start sending ssl handshake. The remote end should be in a state that
     * expects an SSL handshake.
     *
     * @return -1 if an error occurred,
     *          0 if the handshaking is still ongoing and another call to ssl_handshake() is needed, and
     *          1 if the handshaking succeeded
     */
    int ssl_start_connect();
    int ssl_handshake() override;

    void set_connection(std::unique_ptr<mxs::BackendConnection> conn);

    /**
     * Close the dcb. The dcb is not actually closed, just put to the zombie queue.
     *
     * @param dcb Dcb to close
     */
    static void close(BackendDCB* dcb);

    size_t static_size() const override
    {
        return sizeof(*this);
    }

    size_t varying_size() const override
    {
        size_t rv = DCB::varying_size();

        // TODO: m_protocol is owned by this, but that's the wrong way around,
        // TODO: so it is consciously ignored. m_ssl ignored for now.

        return rv;
    }

    std::string whoami() const override;

private:
    BackendDCB(SERVER* server, int fd, const sockaddr_storage& ip, MXS_SESSION* session,
               DCB::Manager* manager);

    bool release_from(MXS_SESSION* session) override;

    static void hangup_cb(const SERVER* server, const std::string& reason);

    SERVER* const                           m_server;   /**< The associated backend server */
    std::shared_ptr<mxs::SSLContext>        m_ssl;      /**< SSL context for this connection */
    std::unique_ptr<mxs::BackendConnection> m_protocol; /**< The protocol session */
};

namespace maxscale
{

const char* to_string(DCB::Role role);
const char* to_string(DCB::State state);
}

/**
 * Debug printing all DCBs from within a debugger.
 */
void printAllDCBs();

/**
 * Debug printing a DCB from within a debugger.
 *
 * @param dcb   The DCB to print
 */
void printDCB(DCB*);

/**
 * Return DCB counts filtered by role
 *
 * @param role   What kind of DCBs should be counted.
 *
 * @return  Count of DCBs in the specified role.
 */
int dcb_count_by_role(DCB::Role role);

uint64_t dcb_get_session_id(DCB* dcb);

/**
 * @brief Call a function for each connected DCB
 *
 * @deprecated You should not use this function, use dcb_foreach_parallel instead
 *
 * @warning This must only be called from the main thread, otherwise deadlocks occur
 *
 * @param func Function to call. The function should return @c true to continue iteration
 * and @c false to stop iteration earlier. The first parameter is a DCB and the second
 * is the value of @c data that the user provided.
 * @param data User provided data passed as the second parameter to @c func
 * @return True if all DCBs were iterated, false if the callback returned false
 */
bool dcb_foreach(bool (* func)(DCB* dcb, void* data), void* data);

