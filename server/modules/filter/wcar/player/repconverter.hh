/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "../capdefs.hh"
#include "repconfig.hh"
#include "../capbooststorage.hh"
#include "repbooststorage.hh"

class RepConverter
{
public:
    RepConverter(const RepConfig& config)
    {
        fs::path path(config.file_name);
        auto output = config.build_rep_storage();

        if (path.extension() == ".rx")
        {
            for (auto ev : RepBoostStorage(config.file_name, RepBoostStorage::READ_ONLY))
            {
                output->add_rep_event(std::move(ev));
            }
        }
        else if (path.extension() == ".cx" || path.extension() == ".ex")
        {
            for (auto ev : CapBoostStorage(config.file_name, ReadWrite::READ_ONLY))
            {
                if (is_real_event(ev))
                {
                    output->add_rep_event(as_rep_event(ev));
                }
            }
        }
        else
        {
            MXB_THROW(WcarError, "Cannot convert files of type " << path.extension());
        }
    }

private:
    RepEvent as_rep_event(const QueryEvent& qe)
    {
        RepEvent re;
        re.can_id = qe.can_id;
        re.start_time = qe.start_time;
        re.end_time = qe.end_time;
        re.event_id = qe.event_id;
        re.num_rows = 0;
        return re;
    }
};
