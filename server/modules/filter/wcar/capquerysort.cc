/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capquerysort.hh"
#include <execution>

QuerySort::QuerySort(CapBoostStorage& storage,
                     BoostOFile& qevent_out,
                     BoostOFile& tevent_out,
                     CapBoostStorage::SortCallback sort_cb)
    : m_storage(storage)
    , m_qevent_out(qevent_out)
    , m_sort_cb(sort_cb)
{
    // Sort by gtid, which can lead to out of order end_time. The number of
    // gtids is small relative to query events and fit in memory (TODO document).
    std::sort(std::execution::par, storage.m_tevents.begin(), storage.m_tevents.end(), []
              (const auto& lhs, const auto& rhs){
        if (lhs.gtid.domain_id == lhs.gtid.domain_id)
        {
            return lhs.gtid.sequence_nr < rhs.gtid.sequence_nr;
        }
        else
        {
            return lhs.end_time < rhs.end_time;
        }
    });

    for (auto&& e : storage.m_tevents)
    {
        m_storage.save_trx_event(tevent_out, e);
    }
}

void QuerySort::add_query_event(std::deque<QueryEvent>& qevents)
{
    m_num_events += qevents.size();
    std::move(qevents.begin(), qevents.end(), std::back_inserter(m_qevents));
}

void QuerySort::finalize()
{
    std::sort(std::execution::par, m_qevents.begin(), m_qevents.end(),
              [](const auto& lhs, const auto& rhs){
        return lhs.start_time < rhs.start_time;
    });

    m_capture_duration = m_qevents.back().end_time - m_qevents.front().start_time;

    for (auto&& qevent : m_qevents)
    {
        m_sort_cb(qevent);
        m_storage.save_query_event(m_qevent_out, qevent);
    }
}

int64_t QuerySort::num_events()
{
    return m_num_events;
}

mxb::Duration QuerySort::capture_duration()
{
    return m_capture_duration;
}
