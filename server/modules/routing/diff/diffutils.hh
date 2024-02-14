/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "diffdefs.hh"

enum class ReplicationStatus
{
    MAIN_REPLICATES_FROM_OTHER,
    OTHER_REPLICATES_FROM_MAIN,
    BOTH_REPLICATES_FROM_THIRD,
    NO_RELATION,
    ERROR
};

class SERVICE;
class SERVER;

ReplicationStatus get_replication_status(const SERVICE& service,
                                         const SERVER& main,
                                         const SERVER& other);
