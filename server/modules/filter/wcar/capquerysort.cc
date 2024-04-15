/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capquerysort.hh"
#include <execution>

QuerySort::QuerySort(fs::path file_path,
                     CapBoostStorage::SortCallback sort_cb)
    : m_file_path(file_path)
    , m_sort_cb(sort_cb)
{
    auto trx_path = file_path.replace_extension("gx");
    BoostIFile trx_in{trx_path.string()};
    std::vector<TrxEvent> tevents;

    while (!trx_in.at_end_of_stream())
    {
        tevents.push_back(CapBoostStorage::load_trx_event(trx_in));
    }

    // Sort by gtid, which can lead to out of order end_time. The number of
    // gtids is small relative to query events and fit in memory (TODO document).
    std::sort(std::execution::par, m_tevents.begin(), m_tevents.end(), []
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

    BoostOFile trx_out{trx_path.string()};
    for (auto&& e : tevents)
    {
        CapBoostStorage::save_trx_event(trx_out, e);
    }

    auto qevent_path = file_path.replace_extension("ex");
    BoostIFile qevent_in{qevent_path.string()};
    while (!qevent_in.at_end_of_stream())
    {
        m_qevents.push_back(CapBoostStorage::load_query_event(qevent_in));
    }
}

void QuerySort::finalize()
{
    std::sort(std::execution::par, m_qevents.begin(), m_qevents.end(),
              [](const auto& lhs, const auto& rhs){
        return lhs.start_time < rhs.start_time;
    });

    m_capture_duration = m_qevents.back().end_time - m_qevents.front().start_time;

    BoostOFile qevent_out{m_file_path.replace_extension("ex")};
    for (auto&& qevent : m_qevents)
    {
        m_sort_cb(qevent);
        CapBoostStorage::save_query_event(qevent_out, qevent);
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
