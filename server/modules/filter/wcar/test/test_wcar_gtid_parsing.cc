/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "../capstorage.hh"
#include <maxbase/log.hh>
#include <iostream>

int main(int argc, char** argv)
{
    mxb::Log logger(MXB_LOG_TARGET_STDOUT);
    int errors = 0;

    auto check = [&](std::string_view str, uint32_t domain = 0, uint32_t srv_id = 0, uint64_t seq){
        Gtid gtid = Gtid::from_string(str);

        if (gtid.domain_id != domain || gtid.server_id != srv_id || gtid.sequence_nr != seq)
        {
            std::cout << "Parsing error for: " << str << std::endl;
            errors++;
        }
    };

    check("0-1-2", 0, 1, 2);
    check("foobar", 0, 0, 0);
    check("", 0, 0, 0);

    return errors;
}
