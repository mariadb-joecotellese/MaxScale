/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capquerysort.hh"
#include <execution>

static inline bool operator<(const SortKey& lhs, const SortKey& rhs)
{
    return lhs.start_time < rhs.start_time
           || (lhs.start_time == rhs.start_time && lhs.event_id < rhs.event_id);
}

static inline bool operator==(const SortKey& lhs, const SortKey& rhs)
{
    return lhs.event_id == rhs.event_id;
}

QuerySort::QuerySort(fs::path file_path,
                     SortCallback sort_cb)
    : m_file_path(file_path)
    , m_sort_cb(sort_cb)
{
    load_sort_keys();
    sort_query_events();
    sort_trx_events();
}

std::vector<TrxEvent> QuerySort::release_trx_events()
{
    return std::move(m_tevents);
}

SortReport QuerySort::report()
{
    return m_report;
}

void QuerySort::load_sort_keys()
{
    auto qevent_path = m_file_path;
    qevent_path.replace_extension("ex");
    BoostIFile query_in{qevent_path.string()};
    while (!query_in.at_end_of_stream())
    {
        auto qevent = CapBoostStorage::load_query_event(query_in);
        m_keys.emplace_back(qevent.start_time, qevent.event_id);
    }

    std::sort(std::execution::par, m_keys.begin(), m_keys.end());
}

void QuerySort::sort_query_events()
{
    auto qevent_path = m_file_path.replace_extension("ex");
    BoostIFile qevent_in{qevent_path.string()};
    while (!qevent_in.at_end_of_stream())
    {
        m_qevents.push_back(CapBoostStorage::load_query_event(qevent_in));
    }

    std::sort(std::execution::par, m_qevents.begin(), m_qevents.end(),
              [](const auto& lhs, const auto& rhs){
        return lhs.start_time < rhs.start_time;
    });

    int64_t num_events = 0;
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

void QuerySort::sort_trx_events()
{
    auto trx_path = m_file_path.replace_extension("gx");
    BoostIFile trx_in{trx_path.string()};
    m_tevents = CapBoostStorage::load_trx_events(trx_in);

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
}
