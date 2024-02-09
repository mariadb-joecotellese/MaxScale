/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxscale/dcb.hh>
#include <maxscale/protocol.hh>
#include <maxscale/session.hh>

namespace maxscale
{

namespace mock
{

/**
 * The class Dcb provides a mock DCB that can be used when testing.
 */
class Dcb : public ClientDCB
{
    Dcb(const Dcb&);
    Dcb& operator=(const Dcb&);

public:
    class Handler
    {
    public:
        virtual int32_t write(GWBUF&& buffer) = 0;
    };

    /**
     * Constructor
     *
     * @param pSession  The session object of the DCB.
     * @param zHost     The host of the connection.
     * @param pHandler  Optional handler.
     */
    Dcb(MXS_SESSION* pSession,
        const char* zHost,
        Handler* pHandler = NULL);
    ~Dcb();

    /**
     * Get the current handler of the Dcb.
     *
     * @return A Handler or NULL.
     */
    Handler* handler() const;

    /**
     * Set the current handler of the Dcb.
     *
     * @param pHandler  The new handler.
     *
     * @return  The previous handler or NULL.
     */
    Handler* set_handler(Handler* pHandler);

private:
    class Protocol : public mxs::ClientConnection
    {
    public:
        Protocol(Dcb::Handler* pHandler)
            : m_pHandler(pHandler)
        {
        }

        Dcb::Handler* handler() const
        {
            return m_pHandler;
        }

        Dcb::Handler* set_handler(Dcb::Handler* pHandler)
        {
            Dcb::Handler* p = m_pHandler;
            m_pHandler = pHandler;
            return p;
        }

        bool init_connection() override
        {
            mxb_assert(!true);
            return false;
        }

        void finish_connection() override
        {
            mxb_assert(!true);
        }

        void ready_for_reading(DCB*) override
        {
            mxb_assert(!true);
        }

        void error(DCB*, const char* errmsg) override
        {
            mxb_assert(!true);
        }

        json_t* diagnostics() const override
        {
            return nullptr;
        }

        void set_dcb(DCB* dcb) override
        {
            m_dcb = static_cast<Dcb*>(dcb);
        }

        ClientDCB* dcb() override
        {
            return m_dcb;
        }

        const ClientDCB* dcb() const override
        {
            return m_dcb;
        }

        bool in_routing_state() const override
        {
            return true;
        }

        bool safe_to_restart() const override
        {
            return true;
        }

        bool clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply) override
        {
            return write(std::move(buffer));
        }

        size_t sizeof_buffers() const override
        {
            return m_dcb ? m_dcb->runtime_size() : 0;
        }

    private:
        bool write(GWBUF&& buffer);

        Dcb::Handler* m_pHandler;
        Dcb*          m_dcb {nullptr};
    };

public:
    Protocol* protocol() const override
    {
        return &m_protocol;
    }

private:
    mutable Protocol m_protocol;
};
}
}
