/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "repbooststorage.hh"

RepBoostStorage::RepBoostStorage(const fs::path& path, Access access)
    : m_path(path)
{
    if (access == Access::READ_ONLY)
    {
        m_sRep_event_in = std::make_unique<BoostIFile>(m_path);
    }
    else
    {
        m_sRep_event_out = std::make_unique<BoostOFile>(m_path);
    }
}

void RepBoostStorage::add_rep_event(RepEvent&& revent)
{
    int64_t start_time_dur = revent.start_time.time_since_epoch().count();
    int64_t end_time_dur = revent.end_time.time_since_epoch().count();

    (**m_sRep_event_out) & revent.event_id;
    (**m_sRep_event_out) & start_time_dur;
    (**m_sRep_event_out) & end_time_dur;
    (**m_sRep_event_out) & revent.can_id;
    (**m_sRep_event_out) & revent.num_rows;
    (**m_sRep_event_out) & revent.rows_read;
    (**m_sRep_event_out) & revent.error;
}

void RepBoostStorage::add_rep_event(std::vector<RepEvent>& revents)
{
    for (auto&& ev : revents)
    {
        add_rep_event(std::move(ev));
    }
}

RepStorage::Iterator RepBoostStorage::begin()
{
    return RepStorage::Iterator(this, next_rep_event());
}

RepStorage::Iterator RepBoostStorage::end() const
{
    return RepStorage::Iterator(nullptr, RepEvent {});
}

RepEvent RepBoostStorage::next_rep_event()
{
    if (m_sRep_event_in->at_end_of_stream())
    {
        return RepEvent{};
    }

    int64_t event_id;
    int64_t start_time;
    int64_t end_time;
    int64_t can_id;
    int32_t num_rows;
    int32_t rows_read;
    uint16_t error;

    (**m_sRep_event_in) & event_id;
    (**m_sRep_event_in) & start_time;
    (**m_sRep_event_in) & end_time;
    (**m_sRep_event_in) & can_id;
    (**m_sRep_event_in) & num_rows;
    (**m_sRep_event_in) & rows_read;
    (**m_sRep_event_in) & error;

    RepEvent ev;
    ev.event_id = event_id;
    ev.start_time = mxb::TimePoint{mxb::Duration{start_time}};
    ev.end_time = mxb::TimePoint{mxb::Duration{end_time}};
    ev.can_id = can_id;
    ev.num_rows = num_rows;
    ev.rows_read = rows_read;
    ev.error = error;
    return ev;
}
