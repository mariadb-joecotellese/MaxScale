/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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
#pragma once

#include <maxscale/ccdefs.hh>
#include <map>
#include "routersession.hh"

namespace maxscale
{

namespace mock
{

/**
 * The abstract class Backend represents a backend.
 */
class Backend
{
    Backend(const Backend&);
    Backend& operator=(const Backend&);

public:
    virtual ~Backend();

    /**
     * Create an OK response.
     *
     * @return A GWBUF containing an OK response packet.
     */
    static GWBUF create_ok_response();

    /**
     * Called to handle a statement from a "client".
     *
     * @param pSession    The originating router session.
     * @param pStatement  A buffer containing a statement.
     */
    virtual void handle_statement(RouterSession* pSession, GWBUF&& statement) = 0;

    /**
     * Called when the backend should respond to the client.
     *
     * @param pSession  The router session to respond to.
     *
     * @return True, if the backend has additional responses to the router session.
     */
    virtual bool respond(RouterSession* pSession, const mxs::Reply& reply) = 0;

    /**
     * Whether the backend has a response for some router.
     *
     * @param pSession  A router session.
     *
     * @return True if there are responses for the router session.
     */
    virtual bool idle(const RouterSession* pSession) const = 0;

    /**
     * Discards an available response.
     *
     * @param pSession  A router session.
     *
     * @return True if there are additional responses for the router session.
     */
    virtual bool discard_one_response(const RouterSession* pSession) = 0;

    /**
     * Discards all available responses.
     *
     * @param pSession  A router session.
     */
    virtual void discard_all_responses(const RouterSession* pSession) = 0;

protected:
    Backend();
};

/**
 * The abstract class BufferBackend is a helper class for concrete
 * backend classes.
 */
class BufferBackend : public Backend
{
    BufferBackend(const BufferBackend&);
    BufferBackend& operator=(const BufferBackend&);

public:
    ~BufferBackend();

    virtual bool respond(RouterSession* pSession, const mxs::Reply& reply) override;

    bool idle(const RouterSession* pSession) const override;

    bool discard_one_response(const RouterSession* pSession) override;

    void discard_all_responses(const RouterSession* pSession) override;

protected:
    BufferBackend();

    /**
     * Enqueues a response for a particular router session.
     *
     * @param pSession   The session to enqueue the response for.
     * @param response   The response.
     */
    void enqueue_response(const RouterSession* pSession, GWBUF&& response);

private:
    GWBUF dequeue_response(const RouterSession* pSession, bool* pEmpty);

private:
    typedef std::deque<GWBUF>                         Responses;
    typedef std::map<const RouterSession*, Responses> SessionResponses;

    SessionResponses m_session_responses;
};

/**
 * The OkBackend is a concrete backend class that response with an
 * OK packet to all statements.
 */
class OkBackend : public BufferBackend
{
    OkBackend(const OkBackend&);
    OkBackend& operator=(const OkBackend&);

public:
    OkBackend();

    void handle_statement(RouterSession* pSession, GWBUF&& statement) override;
};

/**
 * The ResultsetBackend
 */
class ResultSetBackend : public BufferBackend
{
    ResultSetBackend(const ResultSetBackend&);
    ResultSetBackend& operator=(const ResultSetBackend&);

public:
    ResultSetBackend();

    void reset()
    {
        m_created = false;
    }

    bool respond(RouterSession* pSession, const mxs::Reply& reply) override final;
    void handle_statement(RouterSession* pSession, GWBUF&& statement) override;

    int  m_counter;
    bool m_created;
};
}
}
