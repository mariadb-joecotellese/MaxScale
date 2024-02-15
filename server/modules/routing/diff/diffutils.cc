/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */

#include "diffutils.hh"
#include <string>
#include <maxsql/mariadb_connector.hh>
#include <maxscale/secrets.hh>
#include <maxscale/server.hh>
#include <maxscale/service.hh>

std::optional<ReplicationInfo> get_replication_info(const SERVER& server,
                                                    const std::string& user,
                                                    const std::string& password)
{
    std::optional<ReplicationInfo> rv;

    mxq::MariaDB mdb;
    mxq::MariaDB::ConnectionSettings& settings = mdb.connection_settings();

    settings.user = user;
    settings.password = password;
    settings.ssl = server.ssl_config();

    if (mdb.open(server.address(), server.port()))
    {
        auto sResult = mdb.query("SHOW SLAVE STATUS");

        if (sResult)
        {
            ReplicationInfo rinfo;
            rinfo.pServer = &server;
            if (sResult->get_col_count() != 0 && sResult->next_row())
            {
                rinfo.master_host = sResult->get_string("Master_Host");
                rinfo.master_port = sResult->get_int("Master_Port");
                rinfo.slave_io_state = sResult->get_string("Slave_IO_State");
            }

            rv = rinfo;
        }
        else
        {
            MXB_ERROR("Got no result for SHOW SLAVE STATUS from server '%s' at %s:%d: %s",
                      server.name(), server.address(), server.port(), mdb.error());
        }
    }
    else
    {
        MXB_ERROR("Could not connect to server at %s:%d: %s",
                  server.address(), server.port(), mdb.error());
    }

    return rv;
}

ReplicationStatus get_replication_status(const SERVICE& service,
                                         const SERVER& main,
                                         const SERVER& other)
{
    ReplicationStatus rv = ReplicationStatus::ERROR;

    const auto& sConfig = service.config();
    auto user = sConfig->user;
    auto password = mxs::decrypt_password(sConfig->password);

    std::optional<ReplicationInfo> ri_other = get_replication_info(other, user, password);

    if (ri_other)
    {
        if (ri_other->will_replicate_from(main))
        {
            MXB_INFO("Other '%s' is configured to replicate from main '%s'. "
                     "A read-write setup.", other.name(), main.name());

            if (ri_other->is_currently_replicating())
            {
                rv = ReplicationStatus::OTHER_REPLICATES_FROM_MAIN;
            }
            else
            {
                MXB_ERROR("Other server '%s' is configured to replicate from "
                          "main server '%s' at %s:%d, but is currently not replicating.",
                          other.name(), main.name(),
                          ri_other->master_host.c_str(), ri_other->master_port);
            }
        }
        else
        {
            std::optional<ReplicationInfo> ri_main = get_replication_info(main, user, password);

            if (ri_main)
            {
                if (ri_main->will_replicate_from(other))
                {
                    MXB_ERROR("Main '%s' is configured to replicate from other '%s'.",
                              main.name(), other.name());

                    rv = ReplicationStatus::MAIN_REPLICATES_FROM_OTHER;
                }
                else
                {
                    if (ri_main->has_same_master(*ri_other))
                    {
                        MXB_INFO("Main '%s' and other '%s' are configured to replicate from %s:%d. "
                                 "A read-only setup.",
                                 main.name(), other.name(),
                                 ri_other->master_host.c_str(), ri_other->master_port);

                        if (ri_other->slave_io_state == ri_main->slave_io_state)
                        {
                            // Both are replicating or neither is. Either way, we don't care.
                            rv = ReplicationStatus::BOTH_REPLICATES_FROM_THIRD;
                        }
                        else
                        {
                            MXB_ERROR("Main '%s' and other '%s' are configured to replicate from %s:%d, "
                                      "but main is %s and other is %s.",
                                      main.name(), other.name(),
                                      ri_other->master_host.c_str(), ri_other->master_port,
                                      ri_main->slave_io_state.empty() ? "replicating" : "not replicating",
                                      ri_other->slave_io_state.empty() ? "replicating" : "not replicating");
                        }
                    }
                    else
                    {
                        MXB_ERROR("Main '%s' is configured to replicate from %s:%d and "
                                  "other '%s' is configured to replicate from %s:%d. There "
                                  "is no relation between them.",
                                  main.name(), ri_main->master_host.c_str(), ri_main->master_port,
                                  other.name(), ri_other->master_host.c_str(), ri_other->master_port);
                        rv = ReplicationStatus::NO_RELATION;
                    }
                }
            }
        }
    }

    return rv;
}

