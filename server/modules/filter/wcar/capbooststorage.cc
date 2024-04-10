/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capbooststorage.hh"
#include <maxbase/assert.hh>
#include <algorithm>
#include <type_traits>

#if HAVE_STD_EXECUTION
#include <execution>
#define sort_par(...) std::sort(std::execution::par, __VA_ARGS__)
#else
#define sort_par(...) std::sort(__VA_ARGS__)
#endif

constexpr int64_t MAX_QUERY_EVENTS = 10'000;

class QuerySort
{
public:
    QuerySort(CapBoostStorage& storage,
              BoostOFile& qevent_out,
              BoostOFile& tevent_out,
              CapBoostStorage::SortCallback sort_cb);

    void add_query_event(std::deque<QueryEvent>& qevents);
    void finalize();

    int64_t       num_events();
    mxb::Duration capture_duration();

private:
    CapBoostStorage&              m_storage;
    BoostOFile&                   m_qevent_out;
    CapBoostStorage::SortCallback m_sort_cb;
    std::vector<QueryEvent>       m_qevents;
    int64_t                       m_num_events = 0;
    mxb::Duration                 m_capture_duration;
};

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
    sort_par(storage.m_tevents.begin(), storage.m_tevents.end(), [](const auto& lhs, const auto& rhs){
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
    sort_par(m_qevents.begin(), m_qevents.end(), [](const auto& lhs, const auto& rhs){
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

CapBoostStorage::CapBoostStorage(const fs::path& base_path, ReadWrite access)
    : m_base_path(base_path)
    , m_canonical_path(base_path)
    , m_query_event_path(base_path)
    , m_trx_path(base_path)
    , m_access(access)
{
    m_canonical_path.replace_extension("cx");
    m_query_event_path.replace_extension("ex");
    m_trx_path.replace_extension("gx");

    if (m_access == ReadWrite::READ_ONLY)
    {
        m_sCanonical_in = std::make_unique<BoostIFile>(m_canonical_path);
        m_sQuery_event_in = std::make_unique<BoostIFile>(m_query_event_path);
        m_sTrx_in = std::make_unique<BoostIFile>(m_trx_path);
        read_canonicals();
        load_gtrx_events();
        preload_query_events(MAX_QUERY_EVENTS);
    }
    else
    {
        m_sCanonical_out = std::make_unique<BoostOFile>(m_canonical_path);
        m_sQuery_event_out = std::make_unique<BoostOFile>(m_query_event_path);
        m_sTrx_out = std::make_unique<BoostOFile>(m_trx_path);
    }
}

void CapBoostStorage::add_query_event(QueryEvent&& qevent)
{
    auto canon_ite = m_canonicals.find(*qevent.sCanonical);

    if (canon_ite != std::end(m_canonicals))
    {
        qevent.can_id = canon_ite->second.can_id;
        qevent.sCanonical = canon_ite->second.sCanonical;
    }
    else
    {
        qevent.can_id = next_can_id();
        save_canonical(*m_sCanonical_out, qevent.can_id, *qevent.sCanonical);
        m_canonicals.emplace(std::string_view {*qevent.sCanonical}, CanonicalEntry {qevent.can_id,
                                                                                    qevent.sCanonical});
    }

    save_query_event(*m_sQuery_event_out, qevent);
    if (qevent.sTrx)
    {
        TrxEvent gevent{qevent.session_id,
                        qevent.sTrx->start_event_id,
                        qevent.event_id,
                        qevent.end_time,
                        qevent.sTrx->gtid};
        save_trx_event(*m_sTrx_out, gevent);
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

    *bof& qevent.start_time.time_since_epoch().count();
    *bof& qevent.end_time.time_since_epoch().count();
}

void CapBoostStorage::save_trx_event(BoostOFile& bof, const TrxEvent& tevent)
{
    *bof & tevent.session_id;
    *bof & tevent.start_event_id;
    *bof & tevent.end_event_id;
    *bof& tevent.end_time.time_since_epoch().count();
    *bof & tevent.gtid.domain_id;
    *bof & tevent.gtid.server_id;
    *bof & tevent.gtid.sequence_nr;
}

TrxEvent CapBoostStorage::load_trx_event()
{
    TrxEvent tevent;
    int64_t end_time_cnt;

    (**m_sTrx_in) & tevent.session_id;
    (**m_sTrx_in) & tevent.start_event_id;
    (**m_sTrx_in) & tevent.end_event_id;
    (**m_sTrx_in) & end_time_cnt;
    (**m_sTrx_in) & tevent.gtid.domain_id;
    (**m_sTrx_in) & tevent.gtid.server_id;
    (**m_sTrx_in) & tevent.gtid.sequence_nr;

    tevent.end_time = wall_time::TimePoint(mxb::Duration(end_time_cnt));

    return tevent;
}

void CapBoostStorage::read_canonicals()
{
    int64_t can_id;
    std::string canonical;
    while (!m_sCanonical_in->at_end_of_stream())
    {
        (**m_sCanonical_in) & can_id;
        (**m_sCanonical_in) & canonical;
        auto shared = std::make_shared<std::string>(canonical);
        m_canonicals.emplace(std::string_view {*shared}, CanonicalEntry {can_id, shared});
    }
}

void CapBoostStorage::load_gtrx_events()
{
    m_tevents.clear();
    while (!m_sTrx_in->at_end_of_stream())
    {
        m_tevents.push_back(load_trx_event());
    }
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
        qevent.start_time = wall_time::TimePoint(mxb::Duration(start_time_int));
        qevent.end_time = wall_time::TimePoint(mxb::Duration(end_time_int));

        qevent.sCanonical = find_canonical(qevent.can_id);

        m_query_events.push_back(std::move(qevent));
    }
}

CapBoostStorage::SortReport CapBoostStorage::sort_query_event_file(const SortCallback& sort_cb)
{
    SortReport report;
    mxb::StopWatch sw;
    mxb::IntervalTimer read_interval;   /// the first preload_query_events() not counted

    auto qevent_out = std::make_unique<BoostOFile>(m_query_event_path);
    auto tevent_out = std::make_unique<BoostOFile>(m_trx_path);
    QuerySort sorter(*this, *qevent_out, *tevent_out, sort_cb);

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

void CapBoostStorage::events_to_sql(std::ostream& out)
{
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
