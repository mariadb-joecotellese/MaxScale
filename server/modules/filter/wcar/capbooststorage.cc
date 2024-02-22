/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capbooststorage.hh"
#include <maxbase/assert.hh>
#include <algorithm>

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

    m_canonical_fs = open_file(m_canonical_path);
    m_query_event_fs = open_file(m_query_event_path);
    m_rep_event_fs = open_file(m_rep_event_path);

    if (m_access == ReadWrite::READ_ONLY)
    {
        m_sQuery_event_ia = std::make_unique<BoostIArchive>(m_query_event_fs);
        m_sCanonical_ia = std::make_unique<BoostIArchive>(m_canonical_fs);
        m_sRep_event_ia = std::make_unique<BoostIArchive>(m_rep_event_fs);
        read_canonicals();
        preload_more_query_events();
    }
    else
    {
        m_sCanonical_oa = std::make_unique<BoostOArchive>(m_canonical_fs);
        m_sQuery_event_oa = std::make_unique<BoostOArchive>(m_query_event_fs);
        m_sRep_event_oa = std::make_unique<BoostOArchive>(m_rep_event_fs);
    }
}

std::fstream CapBoostStorage::open_file(const fs::path& path)
{
    std::fstream stream;
    if (m_access == ReadWrite::READ_ONLY)
    {
        if (fs::exists(path))
        {
            stream.open(path, std::ios_base::in);
        }
        else
        {
            MXB_THROW(WcarError, "Capture file '" << m_canonical_path << "' not found.");
        }
    }
    else if (fs::exists(path))
    {
        MXB_THROW(WcarError, "Capture file '"
                  << path << "' already exists."
                  << " Appending to existing capture is not allowed.");
    }
    else
    {
        stream.open(path, std::ios_base::out);
        if (!stream)
        {
            MXB_THROW(WcarError, "Could not open '" << path << "' for writing: "
                                                    << mxb_strerror(errno));
        }
    }

    return stream;
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

    (*m_sRep_event_oa) & revent.event_id;
    (*m_sRep_event_oa) & *reinterpret_cast<const int64_t*>(&start_time_dur);
    (*m_sRep_event_oa) & *reinterpret_cast<const int64_t*>(&end_time_dur);
    (*m_sRep_event_oa) & revent.num_rows;
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
        preload_more_query_events();
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
    (*m_sCanonical_oa) & can_id;
    (*m_sCanonical_oa) & canonical;
}

void CapBoostStorage::save_query_event(int64_t can_id, const QueryEvent& qevent)
{
    (*m_sQuery_event_oa) & can_id;
    (*m_sQuery_event_oa) & qevent.event_id;
    (*m_sQuery_event_oa) & qevent.session_id;
    (*m_sQuery_event_oa) & qevent.flags;

    int nargs = qevent.canonical_args.size();
    (*m_sQuery_event_oa) & nargs;
    for (const auto& a : qevent.canonical_args)
    {
        (*m_sQuery_event_oa) & a.pos;
        (*m_sQuery_event_oa) & a.value;
    }

    mxb::Duration start_time_dur = qevent.start_time.time_since_epoch();
    mxb::Duration end_time_dur = qevent.end_time.time_since_epoch();
    (*m_sQuery_event_oa) & *reinterpret_cast<const int64_t*>(&start_time_dur);
    (*m_sQuery_event_oa) & *reinterpret_cast<const int64_t*>(&end_time_dur);
}

void CapBoostStorage::read_canonicals()
{
    int64_t can_id;
    std::string canonical;
    for (;;)
    {
        try
        {
            (*m_sCanonical_ia) & can_id;
            (*m_sCanonical_ia) & canonical;
            auto hash = std::hash<std::string> {}(canonical);
            auto shared = std::make_shared<std::string>(canonical);
            m_canonicals.emplace(hash, CanonicalEntry {can_id, shared});
        }
        catch (std::exception& ex)
        {
            if (!m_canonical_fs.good())     // presumambly the stream was read to the end
            {
                break;
            }
            else
            {
                throw ex;
            }
        }
    }
}

void CapBoostStorage::preload_more_query_events()
{
    // This will become something that needs to consider memory usage
    // rather than number of events.
    int64_t nfetch = 1000 - m_query_events.size();
    while (nfetch--)
    {
        try
        {
            int64_t can_id;
            QueryEvent qevent;

            (*m_sQuery_event_ia) & can_id;
            (*m_sQuery_event_ia) & qevent.event_id;
            (*m_sQuery_event_ia) & qevent.session_id;
            (*m_sQuery_event_ia) & qevent.flags;

            int nargs;
            (*m_sQuery_event_ia) & nargs;
            for (int i = 0; i < nargs; ++i)
            {
                int32_t pos;
                std::string value;
                (*m_sQuery_event_ia) & pos;
                (*m_sQuery_event_ia) & value;
                qevent.canonical_args.emplace_back(pos, std::move(value));
            }

            int64_t start_time_int;
            int64_t end_time_int;
            (*m_sQuery_event_ia) & start_time_int;
            (*m_sQuery_event_ia) & end_time_int;
            qevent.start_time = mxb::TimePoint(mxb::Duration(start_time_int));
            qevent.end_time = mxb::TimePoint(mxb::Duration(end_time_int));

            qevent.sCanonical = find_canonical(can_id);

            m_query_events.push_back(std::move(qevent));
        }
        catch (std::exception& ex)
        {
            if (!m_canonical_fs.good())     // presumambly the stream was read to the end
            {
                break;
            }
            else
            {
                throw ex;
            }
        }
    }
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
