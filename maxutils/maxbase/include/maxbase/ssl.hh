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

#include <maxbase/ccdefs.hh>
#include <string>

namespace maxbase
{

namespace ssl_version
{
enum Version
{
    TLS10,
    TLS11,
    TLS12,
    TLS13,
    SSL_TLS_MAX,
    SSL_UNKNOWN
};

/**
 * Returns the enum value as string.
 *
 * @param version SSL version
 * @return Version as a string
 */
const char* to_string(Version version);

Version from_string(const char* str);
}

// TLS key extended usage. This tells what kind of purpose the key should be used for. If the expected bit
// (clientAuth for CLIENT and serverAuth for SERVER) is not present, then it is assumed that the other bit is
// not present either. This is essentially a XNOR of the clientAuth and serverAuth bits.
enum class KeyUsage
{
    CLIENT,     // Used with outbound connection where MaxScale acts as a client
    SERVER,     // Used with inbound connections where MaxScale is the server
};

// SSL configuration
struct SSLConfig
{
    bool enabled = false;   /** Whether SSL should be used */

    std::string key;    /**< SSL private key */
    std::string cert;   /**< SSL certificate */
    std::string ca;     /**< SSL CA certificate */

    ssl_version::Version version {ssl_version::SSL_TLS_MAX};    /**< Which TLS version to use */

    bool verify_peer {false};   /**< Enable peer certificate verification */
    bool verify_host {false};   /**< Enable peer host verification */

    std::string crl;                /** SSL certificate revocation list*/
    int         verify_depth = 9;   /**< SSL certificate verification depth */
    std::string cipher;             /**< Selected TLS cipher */

    // Convert to human readable string representation
    std::string to_string() const;
};
}
