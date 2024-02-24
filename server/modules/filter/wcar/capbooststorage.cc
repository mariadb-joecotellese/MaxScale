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

template<typename BoostArchive>
BoostFile<BoostArchive>::BoostFile(const fs::path& path)
try
    : m_path(path)
    , m_fs(path.string())
{
    if (!m_fs.is_open())
    {
        m_fs.open(path, std::ios_base::out);
    }
}
catch (std::exception& ex)
{
    MXB_THROW(WcarError, "Could not open file " << path << ' ' << mxb_strerror(errno));
}

template<typename BoostArchive>
void BoostFile<BoostArchive>::open()
{
    if (m_sArchive)
    {
        return;
    }

    try
    {
        m_sArchive = std::make_unique<BoostArchive>(m_fs);
    }
    catch (std::exception& ex)
    {
        MXB_THROW(WcarError, "Could open boost archive " << m_path << ' ' << mxb_strerror(errno));
    }
}

template<typename BoostArchive>
BoostArchive& BoostFile<BoostArchive>::operator*()
{
    open();

    return *m_sArchive;
}

template<typename BoostArchive>
bool BoostFile<BoostArchive>::at_end_of_stream()
{
    open();

    if constexpr (std::is_same_v<boost::archive::binary_iarchive, BoostArchive> )
    {
        return m_fs.peek() == EOF;
    }
    else if constexpr (std::is_same_v<boost::archive::text_iarchive, BoostArchive> )
    {
        return m_fs.peek() == '\n';
    }
    else
    {
        static_assert(false, "at_end_of_stream() only for input");
    }
}

template<typename BoostArchive>
void BoostFile<BoostArchive>::rewind()
{
    m_fs.seekg(0);
    m_fs.seekp(0);
}

CapBoostStorage::CapBoostStorage(const fs::path& base_path, ReadWrite access)
    : m_base_path(base_path)
    , m_canonical_path(base_path)
    , m_query_event_path(base_path)
    , m_rep_event_path(base_path)
    , m_access(access)
{
    m_canonical_path.replace_extension("cx");
    m_query_event_path.replace_extension("ex");
    m_rep_event_path.replace_extension("rx");

    if (m_access == ReadWrite::READ_ONLY)
    {
        m_sCanonical_in = std::make_unique<BoostIFile>(m_canonical_path);
        m_sQuery_event_in = std::make_unique<BoostIFile>(m_query_event_path);
        m_sRep_event_in = std::make_unique<BoostIFile>(m_rep_event_path);
        read_canonicals();
        preload_query_events(MAX_QUERY_EVENTS);
    }
    else
    {
        m_sCanonical_out = std::make_unique<BoostOFile>(m_canonical_path);
        m_sQuery_event_out = std::make_unique<BoostOFile>(m_query_event_path);
        m_sRep_event_out = std::make_unique<BoostOFile>(m_rep_event_path);
    }
}

void CapBoostStorage::add_query_event(QueryEvent&& qevent)
{
    int64_t hash{static_cast<int64_t>(std::hash<std::string> {}(*qevent.sCanonical))};
    auto canon_ite = m_canonicals.find(hash);
    auto can_id = next_can_id();

    if (canon_ite != std::end(m_canonicals))
    {
        can_id = canon_ite->second.can_id;
        qevent.sCanonical = canon_ite->second.sCanonical;
    }
    else
    {
        save_canonical(can_id, *qevent.sCanonical);
        m_canonicals.emplace(hash, CanonicalEntry {can_id, qevent.sCanonical});
    }

    save_query_event(can_id, qevent);
}

void CapBoostStorage::add_query_event(std::vector<QueryEvent>& qevents)
{
    for (auto& event : qevents)
    {
        add_query_event(std::move(event));
    }
}

void CapBoostStorage::add_rep_event(RepEvent&& revent)
{
    mxb::Duration start_time_dur = revent.start_time.time_since_epoch();
    mxb::Duration end_time_dur = revent.end_time.time_since_epoch();

    (**m_sRep_event_out) & revent.event_id;
    (**m_sRep_event_out) & *reinterpret_cast<const int64_t*>(&start_time_dur);
    (**m_sRep_event_out) & *reinterpret_cast<const int64_t*>(&end_time_dur);
    (**m_sRep_event_out) & revent.num_rows;
}

void CapBoostStorage::add_rep_event(std::vector<RepEvent>& revents)
{
    for (auto& revent : revents)
    {
        add_rep_event(std::move(revent));
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

void CapBoostStorage::save_canonical(int64_t can_id, const std::string& canonical)
{
    (**m_sCanonical_out) & can_id;
    (**m_sCanonical_out) & canonical;
}

void CapBoostStorage::save_query_event(int64_t can_id, const QueryEvent& qevent)
{
    (**m_sQuery_event_out) & can_id;
    (**m_sQuery_event_out) & qevent.event_id;
    (**m_sQuery_event_out) & qevent.session_id;
    (**m_sQuery_event_out) & qevent.flags;

    int nargs = qevent.canonical_args.size();
    (**m_sQuery_event_out) & nargs;
    for (const auto& a : qevent.canonical_args)
    {
        (**m_sQuery_event_out) & a.pos;
        (**m_sQuery_event_out) & a.value;
    }

    mxb::Duration start_time_dur = qevent.start_time.time_since_epoch();
    mxb::Duration end_time_dur = qevent.end_time.time_since_epoch();
    (**m_sQuery_event_out) & *reinterpret_cast<const int64_t*>(&start_time_dur);
    (**m_sQuery_event_out) & *reinterpret_cast<const int64_t*>(&end_time_dur);
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

void CapBoostStorage::preload_query_events(int64_t max_in_container)
{
    int64_t nfetch = max_in_container - m_query_events.size();
    while (!m_sQuery_event_in->at_end_of_stream() && nfetch--)
    {
        int64_t can_id;
        QueryEvent qevent;

        (**m_sQuery_event_in) & can_id;
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

        qevent.sCanonical = find_canonical(can_id);

        m_query_events.push_back(std::move(qevent));
    }
}

void CapBoostStorage::sort_query_event_file()
{
    // First version - read the entire file into memory, sort and write back.
    preload_query_events(std::numeric_limits<int64_t>::max());
    m_sQuery_event_in.reset();

    std::sort(std::execution::par, m_query_events.begin(), m_query_events.end(),
              [](const auto& lhs, const auto& rhs){
        return lhs.start_time < rhs.start_time;
    });

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
