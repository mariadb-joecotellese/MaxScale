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

class QuerySort
{
public:
    QuerySort(CapBoostStorage& storage,
              BoostOFile& qevent_out,
              BoostOFile& gevent_out);

    void add_query_event(std::deque<QueryEvent>& qevents);
    void finalize();

    int64_t       num_events();
    mxb::Duration capture_duration();

private:
    using GtidEvents = std::deque<CapBoostStorage::TrxEvent>;
    CapBoostStorage&        m_storage;
    BoostOFile&             m_qevent_out;
    BoostOFile&             m_gevent_out;
    std::vector<QueryEvent> m_qevents;
    int64_t                 m_num_events = 0;
    mxb::Duration           m_capture_duration;

    std::unordered_map<int64_t, mxb::TimePoint> m_adjusted_end_time;
};

QuerySort::QuerySort(CapBoostStorage& storage, BoostOFile& qevent_out, BoostOFile& gevent_out)
    : m_storage(storage)
    , m_qevent_out(qevent_out)
    , m_gevent_out(gevent_out)
{
    std::vector<CapBoostStorage::TrxEvent> qevents = m_storage.load_gtid_events();

    // Sort by gtid, which can lead to out of order end_time. The number of
    // gtids is small relative to query events and fit in memory (TODO document).
    std::sort(std::execution::par, qevents.begin(), qevents.end(), []
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

    // Make a note of gtids that are out of order by end_time.
    if (qevents.size() > 1)
    {
        auto prev = qevents.begin();
        m_storage.save_gtid_event(m_gevent_out, *prev);
        for (auto next = prev + 1; next != qevents.end(); prev = next, ++next)
        {
            if (prev->gtid.domain_id == next->gtid.domain_id
                && prev->end_time > next->end_time)
            {
                m_adjusted_end_time.insert(std::make_pair(next->event_id, prev->end_time));
            }

            m_storage.save_gtid_event(m_gevent_out, *next);
        }
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
        auto adjusted = m_adjusted_end_time.find(qevent.event_id);
        if (adjusted != m_adjusted_end_time.end())
        {
            qevent.end_time = adjusted->second;
        }
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
        if (*qevent.sCanonical != *canon_ite->second.sCanonical)
        {
            MXB_WARNING("Hash collision found. Queries '%s' and '%s' hash to the same value.",
                        qevent.sCanonical->c_str(), canon_ite->second.sCanonical->c_str());
        }

        qevent.can_id = canon_ite->second.can_id;
        qevent.sCanonical = canon_ite->second.sCanonical;
    }
    else
    {
        qevent.can_id = hash;
        save_canonical(*m_sCanonical_out, qevent.can_id, *qevent.sCanonical);
        m_canonicals.emplace(hash, CanonicalEntry {qevent.can_id, qevent.sCanonical});
    }

    save_query_event(*m_sQuery_event_out, qevent);
    if (qevent.sTrx)
    {
        TrxEvent gevent{qevent.event_id, qevent.end_time, qevent.sTrx->gtid};
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

void CapBoostStorage::save_gtid_event(BoostOFile& bof, const TrxEvent& qevent)
{
    Gtid gtid;
    int64_t end_time_cnt = qevent.end_time.time_since_epoch().count();

    *bof & qevent.event_id;
    *bof & end_time_cnt;
    *bof & qevent.gtid.domain_id;
    *bof & qevent.gtid.server_id;
    *bof & qevent.gtid.sequence_nr;
}

CapBoostStorage::TrxEvent CapBoostStorage::load_gtid_event()
{
    TrxEvent gevent;
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

std::vector<CapBoostStorage::TrxEvent> CapBoostStorage::load_gtid_events()
{
    std::vector<TrxEvent> gevents;
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
    SortReport report;
    mxb::StopWatch sw;
    mxb::IntervalTimer read_interval;   /// the first preload_query_events() not counted

    auto qevent_out = std::make_unique<BoostOFile>(m_query_event_path);
    auto gevent_out = std::make_unique<BoostOFile>(m_gtid_path);
    QuerySort sorter(*this, *qevent_out, *gevent_out);

    while (!m_query_events.empty())
    {
        sorter.add_query_event(m_query_events);
        m_query_events.clear();

        read_interval.start_interval();
        preload_query_events(MAX_QUERY_EVENTS);
        read_interval.end_interval();
    }

    sorter.finalize();

    // TODO: read, sort and write are now interleaved.

    report.read = read_interval.total();
    report.sort = sw.lap() - report.read;
    report.events = sorter.num_events();
    report.capture_duration = sorter.capture_duration();
    report.write = sw.lap();
    report.total = sw.split();

    m_sQuery_event_out.reset();
    m_query_events.clear();
    m_sQuery_event_in = std::make_unique<BoostIFile>(m_query_event_path);

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

std::map<int64_t, std::shared_ptr<std::string>>  CapBoostStorage::canonicals() const
{
    std::map<int64_t, std::shared_ptr<std::string>> canonicals_by_id;

    for (const auto& [k, v] : m_canonicals)
    {
        canonicals_by_id.emplace(v.can_id, v.sCanonical);
    }

    return canonicals_by_id;
}

void CapBoostStorage::events_to_sql(fs::path path)
{
    std::ofstream out(path);

    if (!out)
    {
        MXB_THROW(WcarError, "Could not open file " << path << ": " << mxb_strerror(errno));
    }

    for (const auto& qevent : *this)
    {
        if (is_session_close(qevent))
        {
            out << "/** Session: " << qevent.session_id << " quit */;\n";
        }
        else
        {
            out << "/**"
                << " Session: " << qevent.session_id
                << " Event: " << qevent.event_id
                << " Duration: " << mxb::to_string(qevent.end_time - qevent.start_time)
                << " */ "
                << maxsimd::canonical_args_to_sql(*qevent.sCanonical, qevent.canonical_args)
                << ";\n";
        }
    }
}
