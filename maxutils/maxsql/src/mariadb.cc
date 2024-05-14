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
#include <maxsql/mariadb.hh>
#include <string.h>
#include <errmsg.h>
#include <thread>
#include <chrono>
#include <maxbase/alloc.hh>
#include <maxbase/assert.hh>
#include <maxbase/format.hh>

using std::string;

namespace
{
struct THIS_UNIT
{
    bool log_statements;    // Should all statements sent to server be logged?
};

static THIS_UNIT this_unit =
{
    false
};
}

namespace maxsql
{

int mysql_query_ex(MYSQL* conn, const std::string& query, int query_retries, time_t query_retry_timeout)
{
    const char* query_cstr = query.c_str();
    time_t start = time(NULL);
    int rc = mysql_query(conn, query_cstr);

    for (int n = 0; rc != 0 && n < query_retries && mysql_is_net_error(mysql_errno(conn))
         && time(NULL) - start < query_retry_timeout; n++)
    {
        if (n > 0)
        {
            // The first reconnection didn't work, wait for one second before attempting again. This
            // should reduce the likelihood of transient problems causing state changes due to too many
            // reconnection attemps in a short period of time.
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        rc = mysql_query(conn, query_cstr);
    }

    log_statement(rc, conn, query);

    return rc;
}

void log_statement(int rc, MYSQL* conn, const std::string& query)
{
    if (this_unit.log_statements)
    {
        const char* host = "0.0.0.0";
        unsigned int port = 0;
        MXB_AT_DEBUG(int rc1 = ) mariadb_get_info(conn, MARIADB_CONNECTION_HOST, &host);
        MXB_AT_DEBUG(int rc2 = ) mariadb_get_info(conn, MARIADB_CONNECTION_PORT, &port);
        mxb_assert(!rc1 && !rc2);
        MXB_NOTICE("SQL([%s]:%u): %d, \"%s\"", host, port, rc, query.c_str());
    }
}

bool mysql_is_net_error(unsigned int errcode)
{
    switch (errcode)
    {
    case CR_SOCKET_CREATE_ERROR:
    case CR_CONNECTION_ERROR:
    case CR_CONN_HOST_ERROR:
    case CR_IPSOCK_ERROR:
    case CR_SERVER_GONE_ERROR:
    case CR_TCP_CONNECTION:
    case CR_SERVER_LOST:
        return true;

    default:
        return false;
    }
}

void mysql_set_log_statements(bool enable)
{
    this_unit.log_statements = enable;
}

bool mysql_get_log_statements()
{
    return this_unit.log_statements;
}

/**
 * @brief Calculate the length of a length-encoded integer in bytes
 *
 * @param ptr Start of the length encoded value
 * @return Number of bytes before the actual value
 */
size_t leint_bytes(const uint8_t* ptr)
{
    uint8_t val = *ptr;
    if (val <= 0xfb)
    {
        return 1;
    }
    else if (val == 0xfc)
    {
        return 3;
    }
    else if (val == 0xfd)
    {
        return 4;
    }
    else
    {
        return 9;
    }
}

/**
 * @brief Converts a length-encoded integer to @c uint64_t
 *
 * @see https://dev.mysql.com/doc/internals/en/integer.html
 * @param c Pointer to the first byte of a length-encoded integer
 * @return The value converted to a standard unsigned integer
 */
uint64_t leint_value(const uint8_t* c)
{
    uint64_t sz = 0;
    if (*c < 0xfb)
    {
        sz = *c;
    }
    else if (*c == 0xfc)
    {
        memcpy(&sz, c + 1, 2);
    }
    else if (*c == 0xfd)
    {
        memcpy(&sz, c + 1, 3);
    }
    else if (*c == 0xfe)
    {
        memcpy(&sz, c + 1, 8);
    }
    else if (*c != 0xfb) // 0xfb is NULL -> return 0
    {
        mxb_assert(*c == 0xff);
        MXB_ERROR("Unexpected length encoding '%x' encountered when reading length-encoded integer.", *c);
    }
    return sz;
}

/**
 * Converts a length-encoded integer into a standard unsigned integer
 * and advances the pointer to the next unrelated byte.
 *
 * @param c Pointer to the first byte of a length-encoded integer
 */
uint64_t leint_consume(uint8_t** c)
{
    uint64_t rval = leint_value(*c);
    *c += leint_bytes(*c);
    return rval;
}

/**
 * @brief Consume and duplicate a length-encoded string
 *
 * Converts a length-encoded string to a C string and advances the pointer to
 * the first byte after the string. The caller is responsible for freeing
 * the returned string.
 * @param c Pointer to the first byte of a valid packet.
 * @return The newly allocated string or NULL if memory allocation failed
 */
char* lestr_consume_dup(uint8_t** c)
{
    uint64_t slen = leint_consume(c);
    char* str = (char*)MXB_MALLOC((slen + 1) * sizeof(char));

    if (str)
    {
        memcpy(str, *c, slen);
        str[slen] = '\0';
        *c += slen;
    }

    return str;
}

/**
 * @brief Consume a length-encoded string
 *
 * Converts length-encoded strings to character strings and advanced
 * the pointer to the next unrelated byte.
 * @param c Pointer to the start of the length-encoded string
 * @param size Pointer to a variable where the size of the string is stored
 * @return Pointer to the start of the string
 */
char* lestr_consume(uint8_t** c, size_t* size)
{
    uint64_t slen = leint_consume(c);
    *size = slen;
    char* start = (char*) *c;
    *c += slen;
    return start;
}

uint64_t mysql_get_server_capabilities(MYSQL* conn)
{
    uint64_t base_caps = 0;
    uint64_t extra_caps = 0;
    mariadb_get_infov(conn, MARIADB_CONNECTION_SERVER_CAPABILITIES, &base_caps);
    mariadb_get_infov(conn, MARIADB_CONNECTION_EXTENDED_SERVER_CAPABILITIES, &extra_caps);
    return base_caps | (extra_caps << 32);
}

void set_proxy_header(MYSQL* conn)
{
    // Older versions of connector-c assume that the buffer is static. Luckily this doesn't matter as
    // we have to use a static one anyways as we don't know the IP or the port we're connecting from.
    static const char fake_header[] = "PROXY UNKNOWN\r\n";
    mysql_optionsv(conn, MARIADB_OPT_PROXY_HEADER, fake_header, sizeof(fake_header) - 1);
}

const char* lestr_consume_safe(const uint8_t** c, const uint8_t* end, size_t* size)
{
    const char* rval = nullptr;
    const uint8_t* ptr = *c;
    size_t int_len = leint_bytes(ptr);

    if (ptr + int_len < end)
    {
        size_t int_value = leint_value(ptr);

        if (ptr + int_len + int_value <= end)
        {
            rval = (char*)ptr + int_len;
            *size = int_value;
            *c += int_len + int_value;
        }
    }

    return rval;
}
}
