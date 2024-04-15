/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capquerysort.hh"
#include <execution>

QuerySort::QuerySort(fs::path file_path,
                     SortCallback sort_cb)
    : m_file_path(file_path)
    , m_sort_cb(sort_cb)
{
    auto trx_path = file_path.replace_extension("gx");
    BoostIFile trx_in{trx_path.string()};

    while (!trx_in.at_end_of_stream())
    {
        m_tevents.push_back(CapBoostStorage::load_trx_event(trx_in));
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
    for (auto& e : m_tevents)
    {
        CapBoostStorage::save_trx_event(trx_out, e);
    }

    auto qevent_path = file_path.replace_extension("ex");
    BoostIFile qevent_in{qevent_path.string()};
    while (!qevent_in.at_end_of_stream())
    {
        m_qevents.push_back(CapBoostStorage::load_query_event(qevent_in));
    }

    std::sort(std::execution::par, m_qevents.begin(), m_qevents.end(),
              [](const auto& lhs, const auto& rhs){
        return lhs.start_time < rhs.start_time;
    });

    int64_t num_events;
    BoostOFile qevent_out{m_file_path.replace_extension("ex")};
    for (auto&& qevent : m_qevents)
    {
        ++num_events;
        m_sort_cb(qevent);
        CapBoostStorage::save_query_event(qevent_out, qevent);
    }

    m_report.capture_duration = m_qevents.back().end_time - m_qevents.front().start_time;
    m_report.events = num_events;
}

std::vector<TrxEvent> QuerySort::release_trx_events()
{
    return std::move(m_tevents);
}

SortReport QuerySort::report()
{
    return m_report;
}
