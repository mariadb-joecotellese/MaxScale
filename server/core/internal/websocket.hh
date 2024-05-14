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

#include <maxscale/ccdefs.hh>

#include <microhttpd.h>

#include <functional>
#include <vector>

#include <maxbase/worker.hh>

// A class that handles the framing and sending of WebSocket messages.
// The WebSocket protocol can be found here: https://tools.ietf.org/html/rfc6455#section-5.2
class WebSocket : public mxb::Pollable
                , public mxb::Worker::Callable
{
public:
    WebSocket(const WebSocket&) = delete;
    WebSocket& operator=(const WebSocket&) = delete;

    using Handler = std::function<std::string ()>;

    /**
     * Create a new WebSocket connection
     *
     * This should only be called from the callback passed to MHD_create_response_for_upgrade.
     *
     * @param fd  The network socket
     * @param urh Opaque handler used to close the connection
     * @param cb  Callback used to generate values that are sent to the client. If no data is currently
     *            available, the callback should return an empty string.
     */
    static void create(int fd, MHD_UpgradeResponseHandle* urh, Handler cb);

    /**
     * Close all open connections
     */
    static void shutdown();

    ~WebSocket();

private:
    int                        m_fd;
    MHD_UpgradeResponseHandle* m_urh;
    Handler                    m_cb;
    std::vector<uint8_t>       m_buffer;
    mxb::Worker::DCId          m_dcid = 0;

    enum Result
    {
        FULL,
        ERROR,
        MORE
    };

    WebSocket(int fd, MHD_UpgradeResponseHandle* urh, std::function<std::string ()> cb);

    int         poll_fd() const override;
    uint32_t    handle_poll_events(mxb::Worker* worker, uint32_t events, Pollable::Context context) override;
    static void close(WebSocket* ws);

    bool send();
    bool delayed_send();

    void   enqueue_frame(const std::string& data);
    Result drain();
};
