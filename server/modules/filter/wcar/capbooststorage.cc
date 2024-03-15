/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capbooststorage.hh"
#include <maxbase/assert.hh>
#include <algorithm>
#include <type_traits>
#include <execution>

constexpr int64_t MAX_QUERY_EVENTS = 10'000;

CapBoostStorage::CapBoostStorage(const fs::path& base_path, ReadWrite access)
    : m_base_path(base_path)
    , m_canonical_path(base_path)
    , m_query_event_path(base_path)
    , m_gtid_path(base_path)
    , m_access(access)
{
    m_canonical_path.replace_extension("cx");
    m_query_event_path.replace_extension("ex");
    m_gtid_path.replace_extension("gx");

    if (m_access == ReadWrite::READ_ONLY)
    {
        m_sCanonical_in = std::make_unique<BoostIFile>(m_canonical_path);
        m_sQuery_event_in = std::make_unique<BoostIFile>(m_query_event_path);
        m_sGtid_in = std::make_unique<BoostIFile>(m_gtid_path);
        read_canonicals();
        preload_query_events(MAX_QUERY_EVENTS);
    }
    else
    {
        m_sCanonical_out = std::make_unique<BoostOFile>(m_canonical_path);
        m_sQuery_event_out = std::make_unique<BoostOFile>(m_query_event_path);
        m_sGtid_out = std::make_unique<BoostOFile>(m_gtid_path);
    }
}

void CapBoostStorage::add_query_event(QueryEvent&& qevent)
{
    int64_t hash{static_cast<int64_t>(std::hash<std::string> {}(*qevent.sCanonical))};
    auto canon_ite = m_canonicals.find(hash);

    if (canon_ite != std::end(m_canonicals))
    {
        qevent.can_id = canon_ite->second.can_id;
        qevent.sCanonical = canon_ite->second.sCanonical;
    }
    else
    {
        qevent.can_id = next_can_id();
        save_canonical(*m_sCanonical_out, qevent.can_id, *qevent.sCanonical);
        m_canonicals.emplace(hash, CanonicalEntry {qevent.can_id, qevent.sCanonical});
    }

    save_query_event(*m_sQuery_event_out, qevent);
    if (qevent.gtid.is_valid())
    {
        GtidEvent gevent{qevent.event_id, qevent.end_time, qevent.gtid};
        save_gtid_event(*m_sGtid_out, gevent);
    }
}

void CapBoostStorage::add_query_event(std::vector<QueryEvent>& qevents)
{
    for (auto& event : qevents)
    {
        add_query_event(std::move(event));
    }
}

Storage::Iterator CapBoostStorage::begin()
{
    return Storage::Iterator(this, next_event());
}

Storage::Iterator CapBoostStorage::end() const
{
    return Storage::Iterator(nullptr, QueryEvent {});
}

int64_t CapBoostStorage::size()
{
    if (m_access == ReadWrite::WRITE_ONLY)
    {
        return m_sCanonical_out->tell() + m_sQuery_event_out->tell();
    }
    else
    {
        return m_sCanonical_in->tell() + m_sQuery_event_in->tell();
    }
}

QueryEvent CapBoostStorage::next_event()
{
    if (m_query_events.empty())
    {
        preload_query_events(MAX_QUERY_EVENTS);
    }

    if (!m_query_events.empty())
    {
        QueryEvent ret = std::move(m_query_events.front());
        m_query_events.pop_front();
        return ret;
    }
    else
    {
        return QueryEvent{};
    }
}

void CapBoostStorage::save_canonical(BoostOFile& bof, int64_t can_id, const std::string& canonical)
{
    *bof & can_id;
    *bof & canonical;
}

void CapBoostStorage::save_query_event(BoostOFile& bof, const QueryEvent& qevent)
{
    *bof & qevent.can_id;
    *bof & qevent.event_id;
    *bof & qevent.session_id;
    *bof & qevent.flags;

    int nargs = qevent.canonical_args.size();
    *bof & nargs;
    for (const auto& a : qevent.canonical_args)
    {
        *bof & a.pos;
        *bof & a.value;
    }

    mxb::Duration start_time_dur = qevent.start_time.time_since_epoch();
    mxb::Duration end_time_dur = qevent.end_time.time_since_epoch();
    *bof & *reinterpret_cast<const int64_t*>(&start_time_dur);
    *bof & *reinterpret_cast<const int64_t*>(&end_time_dur);
}

void CapBoostStorage::save_gtid_event(BoostOFile& bof, const GtidEvent& qevent)
{
    Gtid gtid;
    int64_t end_time_cnt = qevent.end_time.time_since_epoch().count();

    *bof & qevent.event_id;
    *bof & end_time_cnt;
    *bof & qevent.gtid.domain_id;
    *bof & qevent.gtid.server_id;
    *bof & qevent.gtid.sequence_nr;
}

CapBoostStorage::GtidEvent CapBoostStorage::load_gtid_event()
{
    GtidEvent gevent;
    int64_t end_time_cnt;

    (**m_sGtid_in) & gevent.event_id;
    (**m_sGtid_in) & end_time_cnt;
    (**m_sGtid_in) & gevent.gtid.domain_id;
    (**m_sGtid_in) & gevent.gtid.server_id;
    (**m_sGtid_in) & gevent.gtid.sequence_nr;

    gevent.end_time = mxb::TimePoint(mxb::Duration(end_time_cnt));

    return gevent;
}

void CapBoostStorage::read_canonicals()
{
    int64_t can_id;
    std::string canonical;
    while (!m_sCanonical_in->at_end_of_stream())
    {
        (**m_sCanonical_in) & can_id;
        (**m_sCanonical_in) & canonical;
        auto hash = std::hash<std::string> {}(canonical);
        auto shared = std::make_shared<std::string>(canonical);
        m_canonicals.emplace(hash, CanonicalEntry {can_id, shared});
    }
}

std::vector<CapBoostStorage::GtidEvent> CapBoostStorage::load_gtid_events()
{
    std::vector<GtidEvent> gevents;
    while (!m_sGtid_in->at_end_of_stream())
    {
        gevents.push_back(load_gtid_event());
    }

    return gevents;
}

void CapBoostStorage::preload_query_events(int64_t max_in_container)
{
    int64_t nfetch = max_in_container - m_query_events.size();
    while (!m_sQuery_event_in->at_end_of_stream() && nfetch--)
    {
        QueryEvent qevent;

        (**m_sQuery_event_in) & qevent.can_id;
        (**m_sQuery_event_in) & qevent.event_id;
        (**m_sQuery_event_in) & qevent.session_id;
        (**m_sQuery_event_in) & qevent.flags;

        int nargs;
        (**m_sQuery_event_in) & nargs;
        for (int i = 0; i < nargs; ++i)
        {
            int32_t pos;
            std::string value;
            (**m_sQuery_event_in) & pos;
            (**m_sQuery_event_in) & value;
            qevent.canonical_args.emplace_back(pos, std::move(value));
        }

        int64_t start_time_int;
        int64_t end_time_int;
        (**m_sQuery_event_in) & start_time_int;
        (**m_sQuery_event_in) & end_time_int;
        qevent.start_time = mxb::TimePoint(mxb::Duration(start_time_int));
        qevent.end_time = mxb::TimePoint(mxb::Duration(end_time_int));

        qevent.sCanonical = find_canonical(qevent.can_id);

        m_query_events.push_back(std::move(qevent));
    }
}

CapBoostStorage::SortReport CapBoostStorage::sort_query_event_file()
{
    mxb::StopWatch sw;
    // First version - read the entire file into memory, sort and write back.
    preload_query_events(std::numeric_limits<int64_t>::max());
    m_sQuery_event_in.reset();

    SortReport report;
    report.read = sw.lap();

    std::sort(std::execution::par, m_query_events.begin(), m_query_events.end(),
              [](const auto& lhs, const auto& rhs){
        return lhs.start_time < rhs.start_time;
    });

    auto final_event = std::max_element(std::execution::par, m_query_events.begin(), m_query_events.end(),
                                        [](const auto& lhs, const auto& rhs){
        return lhs.end_time < rhs.end_time;
    });

    report.sort = sw.lap();
    report.events = m_query_events.size();

    if (report.events)
    {
        report.capture_duration = final_event->end_time - m_query_events.front().start_time;
    }

    auto new_path = m_query_event_path;
    new_path.replace_extension("ex");
    fs::rename(m_query_event_path, new_path);
    m_query_event_path = new_path;

    m_sQuery_event_out = std::make_unique<BoostOFile>(m_query_event_path);

    for (auto&& qevent : m_query_events)
    {
        add_query_event(std::move(qevent));
    }

    m_sQuery_event_out.reset();
    m_query_events.clear();

    m_sQuery_event_in = std::make_unique<BoostIFile>(m_query_event_path);

    report.write = sw.lap();
    report.total = sw.split();

    return report;
}

std::shared_ptr<std::string> CapBoostStorage::find_canonical(int64_t can_id)
{
    // Linear search isn't that bad - there aren't that many canonicals,
    // and this is only called when loading events. If it becomes are
    // problem, create an index.
    // The other purpose of this is to be able to reload sql if it has been dropped.
    auto ite = std::find_if(m_canonicals.begin(), m_canonicals.end(), [can_id](const auto& e){
        return e.second.can_id == can_id;
    });

    if (ite == m_canonicals.end())
    {
        MXB_THROW(WcarError, "Bug, canonical should have been found.");
    }

    return ite->second.sCanonical;
}
