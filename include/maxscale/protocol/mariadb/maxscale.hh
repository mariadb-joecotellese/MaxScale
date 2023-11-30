/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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

#include <maxscale/ccdefs.hh>
#include <stdlib.h>
#include <stdint.h>
#include <mysql.h>
#include <maxsql/mariadb.hh>
#include <maxsql/mariadb_connector.hh>
#include <maxscale/protocol/mariadb/mysql.hh>
#include <maxscale/server.hh>

/**
 * Creates a new database connection.
 *
 * @param con     The initialized connection
 * @param address The host to connect to
 * @param port    The port to connect to
 * @param user    Username used for the connection
 * @param passwd  The password for the user
 * @param ssl     The SSL configuration to use
 * @param flags   Connection flags for Connector-C
 *
 * @return New connection or NULL on error
 */
MYSQL* mxs_mysql_real_connect(MYSQL* con, const char* address, int port,
                              const char* user, const char* passwd,
                              const mxb::SSLConfig& ssl, int flags = 0);

/**
 * Creates a database connection to a server.
 *
 * @param con    A valid MYSQL structure.
 * @param server The server on which the MySQL engine is running.
 * @param port   The port to connect to
 * @param user   The MySQL login ID.
 * @param passwd The password for the user.
 *
 * @return New connection or NULL on error
 */
MYSQL* mxs_mysql_real_connect(MYSQL* con, SERVER* server, int port, const char* user, const char* passwd);

/**
 * Execute a query using global query retry settings.
 *
 * @param conn  MySQL connection
 * @param query Query to execute
 *
 * @return return value of mysql_query
 */
int mxs_mysql_query(MYSQL* conn, const char* query);

typedef enum mxs_pcre_quote_approach
{
    MXS_PCRE_QUOTE_VERBATIM,    /*<! Quote all PCRE characters. */
    MXS_PCRE_QUOTE_WILDCARD     /*<! Quote all PCRE characters, except % that is converted into .*. */
} mxs_pcre_quote_approach_t;

typedef enum mxs_mysql_name_kind
{
    MXS_MYSQL_NAME_WITH_WILDCARD,   /*<! The input string contains a %. */
    MXS_MYSQL_NAME_WITHOUT_WILDCARD /*<! The input string does not contain a %. */
} mxs_mysql_name_kind_t;

/**
 * Convert a MySQL/MariaDB name string to a pcre compatible one.
 *
 * Note that the string is expected to be a user name or a host name,
 * but not a full account name. Further, if converting a user name,
 * then the approach should be @c MXS_PCRE_QUOTE_VERBATIM and if converting
 * a host name, the approach should be @c MXS_PCRE_QUOTE_WILDCARD.
 *
 * Note also that this function will not trim surrounding quotes.
 *
 * In principle:
 *   - Quote all characters that have a special meaning in a PCRE context.
 *   - Optionally convert "%" into ".*".
 *
 * @param pcre     The string to which the conversion should be copied.
 *                 To be on the safe size, the buffer should be twice the
 *                 size of 'mysql'.
 * @param mysql    The mysql user or host string.
 * @param approach Whether % should be converted or not.
 *
 * @return Whether or not the name contains a wildcard.
 */
mxs_mysql_name_kind_t mxs_mysql_name_to_pcre(char* pcre,
                                             const char* mysql,
                                             mxs_pcre_quote_approach_t approach);

/**
 * Get server information from connector, store it to server object. This does not query
 * the server as the data has been read while connecting.
 *
 * @param dest     Server object to write
 * @param source   MySQL handle from which information is read
 */
void mxs_mysql_update_server_version(SERVER* dest, MYSQL* source);

/**
 * Update default server character set from @@global.character_set_server
 *
 * @param mysql  The connection handle
 * @param server The server to modify
 */
void mxs_update_server_charset(MYSQL* mysql, SERVER* server);

namespace maxscale
{

/**
 * Execute a query which returns data. The results are returned as a unique pointer to a QueryResult
 * object. The column names of the results are assumed unique.
 *
 * @param conn Server connection
 * @param query The query
 * @param errmsg_out Where to store an error message if query fails. Can be null.
 * @param errno_out Error code output. Can be null.
 * @return Pointer to query results, or an empty pointer on failure
 */
std::unique_ptr<mxb::QueryResult> execute_query(MYSQL* conn, const std::string& query,
                                                std::string* errmsg_out = nullptr,
                                                unsigned int* errno_out = nullptr);
}
