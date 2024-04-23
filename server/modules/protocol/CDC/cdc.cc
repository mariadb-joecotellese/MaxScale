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
 * @file cdc.c - Change Data Capture Listener protocol module
 *
 * The change data capture protocol module is intended as a mechanism to allow connections
 * into maxscale for the purpose of accessing information within
 * the maxscale with a Change Data Capture API interface (supporting Avro right now)
 * databases.
 *
 * In the first instance it is intended to connect, authenticate and retieve data in the Avro format
 * as requested by compatible clients.
 *
 * @verbatim
 * Revision History
 * Date     Who         Description
 * 11/01/2016   Massimiliano Pinto  Initial implementation
 *
 * @endverbatim
 */

#include <maxscale/protocol/cdc/module_names.hh>
#define MXB_MODULE_NAME MXS_CDC_PROTOCOL_NAME

#include <maxscale/ccdefs.hh>
#include <cstdio>
#include <cstring>
#include <maxscale/protocol/cdc/cdc.hh>
#include <maxscale/modinfo.hh>
#include <maxscale/dcb.hh>
#include <maxscale/buffer.hh>
#include <maxscale/session.hh>
#include "cdc_plain_auth.hh"

namespace
{
mxs::config::Specification s_spec(MXB_MODULE_NAME, mxs::config::Specification::PROTOCOL);

struct CDCProtocolData final : public mxs::ProtocolData
{
    bool will_respond(const GWBUF& buffer) const override
    {
        return false;
    }

    bool can_recover_state() const override
    {
        return false;
    }

    virtual bool is_trx_starting() const override
    {
        return false;
    }

    virtual bool is_trx_active() const override
    {
        return false;
    }

    virtual bool is_trx_read_only() const override
    {
        return false;
    }

    virtual bool is_trx_ending() const override
    {
        return false;
    }

    virtual bool is_autocommit() const override
    {
        return false;
    }

    virtual bool are_multi_statements_allowed() const override
    {
        return false;
    }

    virtual size_t amend_memory_statistics(json_t* memory) const override
    {
        return 0;
    }

    virtual size_t static_size() const override
    {
        return 0;
    }

    virtual size_t varying_size() const override
    {
        return 0;
    }
};
}

class CDCProtocolModule : public mxs::ProtocolModule
{
public:
    ~CDCProtocolModule() override = default;

    static CDCProtocolModule* create(const std::string& name, mxs::Listener*)
    {
        return new CDCProtocolModule(name);
    }

    mxs::config::Configuration& getConfiguration() override final
    {
        return m_config;
    }

    std::unique_ptr<mxs::ClientConnection>
    create_client_protocol(MXS_SESSION* session, mxs::Component* component) override
    {
        session->set_protocol_data(std::make_unique<CDCProtocolData>());
        return std::make_unique<CDCClientConnection>(m_auth_module, component);
    }

    std::string auth_default() const override
    {
        return "CDCPlainAuth";
    }

    std::string name() const override
    {
        return MXB_MODULE_NAME;
    }

    std::string protocol_name() const override
    {
        return MXS_CDC_PROTOCOL_NAME;
    }

    json_t* print_auth_users_json() override
    {
        return m_auth_module.diagnostics();
    }

    GWBUF make_error(int errnum, const std::string& sqlstate, const std::string& message) const override
    {
        mxb_assert(!true);
        return GWBUF{};
    }

    std::string_view get_sql(const GWBUF& packet) const override
    {
        mxb_assert(!true);
        return std::string_view{};
    }

    std::string describe(const GWBUF& packet, int body_max_len) const override
    {
        mxb_assert(!true);
        return std::string{};
    }

private:
    CDCProtocolModule(const std::string& name)
        : m_config(name, &s_spec)
    {
    }

    CDCAuthenticatorModule m_auth_module;

    // This is needed for the getConfiguration entry point
    mxs::config::Configuration m_config;
};

extern "C"
{
/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
MXS_MODULE* MXS_CREATE_MODULE()
{
    static modulecmd_arg_type_t args[] =
    {
        {MODULECMD_ARG_SERVICE, "Service where the user is added"},
        {MODULECMD_ARG_STRING,  "User to add"                    },
        {MODULECMD_ARG_STRING,  "Password of the user"           }
    };

    modulecmd_register_command("cdc", "add_user", MODULECMD_TYPE_ACTIVE, cdc_add_new_user,
                               3, args, "Add a new CDC user");

    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        MXB_MODULE_NAME,
        mxs::ModuleType::PROTOCOL,
        mxs::ModuleStatus::GA,
        MXS_PROTOCOL_VERSION,
        "A Change Data Capture Listener implementation for use in binlog events retrieval",
        "V1.0.0",
        MXS_NO_MODULE_CAPABILITIES,
        &mxs::ProtocolApiGenerator<CDCProtocolModule>::s_api,
        NULL,       /* Process init. */
        NULL,       /* Process finish. */
        NULL,       /* Thread init. */
        NULL,       /* Thread finish. */
        &s_spec
    };

    return &info;
}
}

void CDCClientConnection::ready_for_reading(DCB* event_dcb)
{
    mxb_assert(m_dcb == event_dcb);     // The protocol should only handle its own events.
    auto dcb = m_dcb;

    MXS_SESSION* session = dcb->session();
    CDCClientConnection* protocol = this;
    int auth_val = CDC_STATE_AUTH_FAILED;

    auto [read_ok, head] = m_dcb->read(0, 0);
    if (!head.empty())
    {
        switch (protocol->m_state)
        {
        case CDC_STATE_WAIT_FOR_AUTH:
            /* Fill CDC_session from incoming packet */
            if (m_authenticator.extract(dcb, head))
            {
                /* Call protocol authentication */
                auth_val = m_authenticator.authenticate(dcb);
            }

            if (auth_val == CDC_STATE_AUTH_OK)
            {
                if (session->start())
                {
                    protocol->m_state = CDC_STATE_HANDLE_REQUEST;

                    write_auth_ack();
                }
                else
                {
                    auth_val = CDC_STATE_AUTH_NO_SESSION;
                }
            }

            if (auth_val != CDC_STATE_AUTH_OK)
            {
                protocol->m_state = CDC_STATE_AUTH_ERR;

                write_auth_err();
                /* force the client connection close */
                ClientDCB::close(dcb);
            }
            break;

        case CDC_STATE_HANDLE_REQUEST:
            // handle CLOSE command, it shoudl be routed as well and client connection closed after last
            // transmission
            if (strncmp((char*)head.data(), "CLOSE", head.length()) == 0)
            {
                MXB_INFO("%s: Client [%s] has requested CLOSE action",
                         dcb->service()->name(),
                         dcb->remote().c_str());

                // gwbuf_set_type(head, GWBUF_TYPE_CDC);
                // the router will close the client connection
                // rc = mxs_route_query(session, head);


                /* right now, just force the client connection close */
                ClientDCB::close(dcb);
            }
            else
            {
                MXB_INFO("%s: Client [%s] requested [%.*s] action",
                         dcb->service()->name(),
                         dcb->remote().c_str(),
                         (int)head.length(),
                         (char*)head.data());

                m_downstream->routeQuery(std::move(head));
            }
            break;

        default:
            MXB_INFO("%s: Client [%s] in unknown state %d",
                     dcb->service()->name(),
                     dcb->remote().c_str(),
                     protocol->m_state);
            break;
        }
    }
}

void CDCClientConnection::error(DCB* event_dcb, const char* errmsg)
{
    mxb_assert(m_dcb == event_dcb);
    ClientDCB::close(m_dcb);
}

bool CDCClientConnection::init_connection()
{
    mxb_assert(m_dcb->session());

    /* client protocol state change to CDC_STATE_WAIT_FOR_AUTH */
    m_state = CDC_STATE_WAIT_FOR_AUTH;

    MXB_INFO("%s: new connection from [%s]", m_dcb->service()->name(), m_dcb->remote().c_str());
    return true;
}

void CDCClientConnection::finish_connection()
{
}

CDCClientConnection::CDCClientConnection(CDCAuthenticatorModule& auth_module, mxs::Component* downstream)
    : m_authenticator(auth_module)
    , m_downstream(downstream)
{
}

/**
 * Writes Authentication ACK, success.
 */
void CDCClientConnection::write_auth_ack()
{
    write("OK\n");
}

/**
 * Writes Authentication ERROR.
 */
void CDCClientConnection::write_auth_err()
{
    write("ERROR: Authentication failed\n");
}

bool CDCClientConnection::write(std::string_view msg)
{
    // CDC-protocol messages end in \n. The ending 0-char need not be written.
    GWBUF buf(msg.size() + 1);
    memcpy(buf.data(), msg.data(), msg.size());
    buf.data()[msg.size()] = '\n';
    return clientReply(std::move(buf), mxs::ReplyRoute {}, mxs::Reply {});
}

bool CDCClientConnection::clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    return m_dcb->writeq_append(std::move(buffer));
}

bool CDCClientConnection::safe_to_restart() const
{
    return true;
}

size_t CDCClientConnection::sizeof_buffers() const
{
    return ClientConnectionBase::sizeof_buffers();
}
