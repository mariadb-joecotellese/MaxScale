/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

#include <vector>
#include <maxtest/testconnections.hh>
#include <maxtest/tcp_connection.hh>

int main(int argc, char* argv[])
{
    TestConnections test(argc, argv);
    test.reset_timeout();

    uint32_t caps = 1 | 8 | 512;
    uint32_t max_packet = 65535;
    uint8_t charset = 8;
    std::string username = "username";
    uint8_t token_len = 20;     // SHA1 hash size
    std::string database = "database";

    // Capabilities, max packet size and client charset
    std::vector<uint8_t> wbuf;
    auto it = std::back_inserter(wbuf);

    for (auto a : {
        (uint8_t)(caps), (uint8_t)(caps >> 8), (uint8_t)(caps >> 16), (uint8_t)(caps >> 24),
        (uint8_t)(max_packet), (uint8_t)(max_packet >> 8), (uint8_t)(max_packet >> 16),
        (uint8_t)(max_packet >> 24),
        charset
    })
    {
        *it++ = a;
    }

    // Reserved filler space
    std::fill_n(it, 23, 0);

    // Username without terminating null character
    for (auto a : username)
    {
        *it++ = (uint8_t)a;
    }

    // Auth token length and the token itself
    *it++ = token_len;
    std::fill_n(it, token_len, 123);

    // Database without terminating null character
    for (auto a : database)
    {
        *it++ = (uint8_t)a;
    }

    // Payload length and sequence number
    uint8_t bufsize = wbuf.size();
    wbuf.insert(wbuf.begin(), {(uint8_t)(bufsize), (uint8_t)(bufsize >> 8), (uint8_t)(bufsize >> 16), 1});


    tcp::Connection conn;
    conn.connect(test.maxscale->ip(), test.maxscale->rwsplit_port);

    // Read the handshake
    uint8_t buf[512] = {};
    conn.read(buf, sizeof(buf));

    // Send the handshake response
    conn.write(&wbuf[0], wbuf.size());

    // Read MaxScale's response
    conn.read(buf, sizeof(buf));

    const char response[] = "Bad handshake";
    test.add_result(memmem(buf, sizeof(buf), response, sizeof(response) - 1) == NULL,
                    "MaxScale should respond with 'Bad handshake'");

    return test.global_result;
}
