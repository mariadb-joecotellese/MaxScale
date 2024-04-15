/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capbooststorage.hh"
#include "capquerysort.hh"
#include <maxbase/assert.hh>
#include <algorithm>
#include <type_traits>

#if HAVE_STD_EXECUTION
#include <execution>
#define sort_par(...) std::sort(std::execution::par, __VA_ARGS__)
#else
#define sort_par(...) std::sort(__VA_ARGS__)
#endif

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

TrxEvent CapBoostStorage::load_trx_event(BoostIFile& bif)
{
    TrxEvent tevent;
    int64_t end_time_cnt;

    *bif & tevent.session_id;
    *bif & tevent.start_event_id;
    *bif & tevent.end_event_id;
    *bif & end_time_cnt;
    *bif & tevent.gtid.domain_id;
    *bif & tevent.gtid.server_id;
    *bif & tevent.gtid.sequence_nr;

    tevent.end_time = wall_time::TimePoint(mxb::Duration(end_time_cnt));

    return tevent;
}

void CapBoostStorage::load_canonicals()
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
        m_tevents.push_back(load_trx_event(*m_sTrx_in));
    }
}

QueryEvent CapBoostStorage::load_query_event(BoostIFile& bif)
{
    QueryEvent qevent;

    *bif & qevent.can_id;
    *bif & qevent.event_id;
    *bif & qevent.session_id;
    *bif & qevent.flags;

    int nargs;
    *bif & nargs;
    for (int i = 0; i < nargs; ++i)
    {
        int32_t pos;
        std::string value;
        *bif & pos;
        *bif & value;
        qevent.canonical_args.emplace_back(pos, std::move(value));
    }

    int64_t start_time_int;
    int64_t end_time_int;
    *bif & start_time_int;
    *bif & end_time_int;
    qevent.start_time = wall_time::TimePoint(mxb::Duration(start_time_int));
    qevent.end_time = wall_time::TimePoint(mxb::Duration(end_time_int));

    return qevent;
}

void CapBoostStorage::preload_query_events(int64_t max_in_container)
{
    int64_t nfetch = max_in_container - m_query_events.size();
    while (!m_sQuery_event_in->at_end_of_stream() && nfetch--)
    {
        auto qevent = load_query_event(*m_sQuery_event_in);
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
    if (m_canonicals.empty())
    {
        load_canonicals();
    }

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

std::map<int64_t, std::shared_ptr<std::string>>  CapBoostStorage::canonicals()
{
    if (m_canonicals.empty())
    {
        load_canonicals();
    }

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
