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

#include "mariadbmon_common.hh"

/** Default gtid domain */
const int64_t GTID_DOMAIN_UNKNOWN = -1;
/** Default port */
const int PORT_UNKNOWN = 0;
/** Server lock names */
const char* SERVER_LOCK_NAME = "maxscale_mariadbmonitor";
const char* MASTER_LOCK_NAME = "maxscale_mariadbmonitor_master";

using std::string;

DelimitedPrinter::DelimitedPrinter(string separator)
    : m_separator(std::move(separator))
{
}

void DelimitedPrinter::cat(string& target, const string& addition)
{
    target += m_current_separator + addition;
    m_current_separator = m_separator;
}

void DelimitedPrinter::cat(const std::string& addition)
{
    cat(m_message, addition);
    m_current_separator = m_separator;
}

std::string DelimitedPrinter::message() const
{
    return m_message;
}
