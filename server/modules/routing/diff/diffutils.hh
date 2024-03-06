/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"
#include <optional>
#include <maxscale/server.hh>

enum class ReplicationStatus
{
    MAIN_REPLICATES_FROM_OTHER,
    OTHER_REPLICATES_FROM_MAIN,
    BOTH_REPLICATES_FROM_THIRD,
    NO_RELATION,
    ERROR
};

struct ReplicationInfo
{
    const SERVER* pServer { nullptr };
    std::string   master_host;
    int           master_port { 0 };
    bool          slave_io_running { false };
    bool          slave_sql_running { false };

    bool will_replicate_from(const SERVER& server) const
    {
        return this->master_host == server.address() && this->master_port == server.port();
    }

    bool will_replicate_from(const ReplicationInfo& ri) const
    {
        mxb_assert(ri.pServer);
        return will_replicate_from(*ri.pServer);
    }

    bool has_same_master(const ReplicationInfo& ri) const
    {
        return this->master_host == ri.master_host && this->master_port == ri.master_port;
    }

    bool is_currently_replicating() const
    {
        return this->slave_io_running && this->slave_sql_running;
    }
};

class SERVICE;

std::optional<ReplicationInfo> get_replication_info(const SERVER& server,
                                                    const std::string& user,
                                                    const std::string& password);

ReplicationStatus get_replication_status(const SERVICE& service,
                                         const SERVER& main,
                                         const SERVER& other);
