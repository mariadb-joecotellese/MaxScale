/*
 * Copyright (c) 2020 MariaDB Corporation Ab
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

#include "find_gtid.hh"
#include "inventory.hh"
#include "pinloki.hh"
#include "rpl_event.hh"
#include <maxbase/log.hh>
#include <maxscale/routingworker.hh>
#include <fstream>
#include <iostream>
#include <iomanip>

namespace pinloki
{
inline bool operator<(const GtidPosition& lhs, const GtidPosition& rhs)
{
    if (lhs.file_name.empty())
    {
        return true;
    }
    else if (rhs.file_name.empty())
    {
        return false;
    }

    auto lhs_pos = lhs.file_name.find_last_of(".");
    auto rhs_pos = lhs.file_name.find_last_of(".");

    auto lhs_num = std::atoi(&lhs.file_name[lhs_pos + 1]);
    auto rhs_num = std::atoi(&rhs.file_name[rhs_pos + 1]);

    return lhs_num < rhs_num || (lhs_num == rhs_num && lhs.file_pos < rhs.file_pos);
}

bool search_file(const std::string& file_name,
                 const maxsql::Gtid& gtid,
                 GtidPosition* pos,
                 bool search_in_file);

std::vector<GtidPosition> find_gtid_position(const std::vector<maxsql::Gtid>& gtids,
                                             const InventoryReader& inv)
{
    mxb::WatchdogNotifier::Workaround workaround(mxs::RoutingWorker::get_current());

    std::vector<GtidPosition> ret;
    // Simple linear search. If there can be a lot of files, make this a binary search, or
    // if it really becomes slow, create an index
    const auto& file_names = inv.file_names();

    // Search in reverse because the gtid is likely be one of the latest files, and
    // the search can stop as soon as the gtid is greater than the gtid list in the file,
    // uh, expect for the first file which doesn't have a GTID_LIST_EVENT.

    // TODO, don't do one gtid at a time, modify the search to do all in one go.
    for (const auto& gtid : gtids)
    {
        GtidPosition pos {gtid};
        auto last_one = rend(file_names) - 1;   // which is the first, oldest file
        for (auto ite = rbegin(file_names); ite != rend(file_names); ++ite)
        {
            if (search_file(*ite, gtid, &pos, ite == last_one))
            {
                break;
            }
        }

        ret.push_back(pos);
    }

    sort(begin(ret), end(ret));

    return ret;
}

/**
 * @brief search_gtid_in_file
 * @param file
 * @param from_pos
 * @return position, or 0 if not found
 */
long search_gtid_in_file(std::ifstream& file, long file_pos, const maxsql::Gtid& gtid)
{
    long found_pos = 0;

    while (!found_pos)
    {
        auto this_pos = file_pos;

        maxsql::RplEvent rpl = maxsql::RplEvent::read_header_only(file, &file_pos);
        if (rpl.is_empty())
        {
            break;
        }

        if (rpl.event_type() != GTID_EVENT)
        {
            file_pos = rpl.next_event_pos();
        }
        else
        {
            rpl.read_body(file, &file_pos);
            if (rpl.is_empty())
            {
                break;
            }

            maxsql::GtidEvent event = rpl.gtid_event();
            if (event.gtid.domain_id() == gtid.domain_id()
                && event.gtid.sequence_nr() == gtid.sequence_nr())
            {
                found_pos = this_pos;
            }
        }
    }

    return found_pos;
}

bool search_file(const std::string& file_name,
                 const maxsql::Gtid& gtid,
                 GtidPosition* ret_pos,
                 bool first_file)
{
    std::ifstream file {file_name, std::ios_base::in | std::ios_base::binary};

    if (!file.good())
    {
        MXB_SERROR("Could not open binlog file " << file_name);
        return false;
    }

    enum GtidListResult {NotFound, GtidInThisFile, GtidInPriorFile};
    GtidListResult result = NotFound;
    long file_pos = PINLOKI_MAGIC.size();

    while (result == NotFound)
    {
        maxsql::RplEvent rpl = maxsql::RplEvent::read_header_only(file, &file_pos);

        if (rpl.is_empty())
        {
            break;
        }

        if (rpl.event_type() != GTID_LIST_EVENT)
        {
            file_pos = rpl.next_event_pos();
        }
        else
        {
            rpl.read_body(file, &file_pos);
            if (rpl.is_empty())
            {
                break;
            }

            maxsql::GtidListEvent event = rpl.gtid_list();

            uint64_t highest_seq = 0;
            bool domain_in_list = false;

            for (const auto& tid : event.gtid_list.gtids())
            {
                if (tid.domain_id() == gtid.domain_id())
                {
                    domain_in_list = true;
                    highest_seq = std::max(highest_seq, tid.sequence_nr());
                }
            }

            if (!domain_in_list || (domain_in_list && highest_seq < gtid.sequence_nr()))
            {
                result = GtidInThisFile;
            }
            else if (highest_seq == gtid.sequence_nr())
            {
                result = GtidInPriorFile;
            }
            else
            {
                break;
            }
        }
    }

    bool success = false;

    // The first file does not necessarily have a GtidList
    if ((result == NotFound && first_file) || result == GtidInThisFile)
    {
        if (result == NotFound)
        {
            file_pos = PINLOKI_MAGIC.size();
        }

        file.clear();
        file_pos = search_gtid_in_file(file, file_pos, gtid);
        if (file_pos)
        {
            success = true;
            ret_pos->file_name = file_name;
            ret_pos->file_pos = file_pos;
        }
    }
    else if (result == GtidInPriorFile)
    {
        // The gtid is in a prior log file, and the caller already has it.
        // file_pos points one past the gtid list, but to be sure the whole file
        // is always sent, let the reader handle positioning.
        success = true;
        ret_pos->file_name = file_name;
        ret_pos->file_pos = PINLOKI_MAGIC.size();
    }

    return success;
}


maxsql::GtidList find_last_gtid_list(const InventoryWriter& inv)
{
    maxsql::GtidList ret;
    if (inv.file_names().empty())
    {
        return ret;
    }

    auto file_name = inv.file_names().back();
    std::ifstream file {file_name, std::ios_base::in | std::ios_base::binary};
    long file_pos = PINLOKI_MAGIC.size();
    long prev_pos = file_pos;
    long truncate_to = 0;
    bool in_trx = false;
    mxq::Gtid last_gtid;
    uint8_t flags = 0;

    while (auto rpl = mxq::RplEvent::read_event(file, &file_pos))
    {
        switch (rpl.event_type())
        {
        case GTID_LIST_EVENT:
            {
                auto event = rpl.gtid_list();

                for (const auto& gtid : event.gtid_list.gtids())
                {
                    ret.replace(gtid);
                }
            }
            break;

        case GTID_EVENT:
            {
                auto event = rpl.gtid_event();
                in_trx = true;
                truncate_to = prev_pos;
                flags = event.flags;
                last_gtid = event.gtid;
            }
            break;

        case XID_EVENT:
            in_trx = false;
            ret.replace(last_gtid);
            break;

        case QUERY_EVENT:
            // This was a DDL event that commits the previous transaction. If the F_STANDALONE flag is not
            // set, an XID_EVENT will follow that commits the transaction.
            if (flags & mxq::F_STANDALONE)
            {
                in_trx = false;
                ret.replace(last_gtid);
            }
            break;

        case STOP_EVENT:
        case ROTATE_EVENT:
            // End of the binlog, return the latest GTID we found. We can assume that only complete
            // transactions are stored in the file if we get this far.
            return ret;

        default:
            MXB_SDEBUG("GTID search: " << rpl);
            break;
        }

        if (prev_pos < rpl.next_event_pos())
        {
            file_pos = rpl.next_event_pos();
        }
        else
        {
            // If the binlog file is over 4GiB, we cannot rely on the next event offset anymore.
            file_pos = prev_pos + rpl.buffer_size();
            mxb_assert(file_pos >= std::numeric_limits<uint32_t>::max());
        }

        prev_pos = file_pos;
    }

    if (in_trx)
    {
        MXB_WARNING("Partial transaction '%s' in '%s'. Truncating the file to "
                    "the last known good event at %ld.",
                    last_gtid.to_string().c_str(), file_name.c_str(), truncate_to);

        // NOTE: If the binlog file is ever read by multiple independent readers in parallel, file truncation
        // cannot be done. Instead of truncating the file, a separate temporary file that holds the
        // partially replicated transactions needs to be used.
        if (truncate(file_name.c_str(), truncate_to) != 0)
        {
            MXB_ERROR("Failed to truncate '%s': %d, %s", file_name.c_str(), errno, mxb_strerror(errno));
        }
    }

    return ret;
}
}
