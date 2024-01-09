/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "wcarbooststorage.hh"
#include <maxbase/assert.hh>

BoostStorage::BoostStorage(const fs::path& base_path, ReadWrite access)
    : m_base_path(base_path)
    , m_canonical_path(base_path)
    , m_event_path(base_path)
    , m_access(access)
{
    m_canonical_path.replace_extension("cx");
    m_event_path.replace_extension("ex");

    m_canonical_fs = open_file(m_canonical_path);
    m_event_fs = open_file(m_event_path);

    if (m_access == ReadWrite::READ_ONLY)
    {
        m_sCanonical_ia = std::make_unique<BoostIArchive>(m_canonical_fs);
        m_sEvent_ia = std::make_unique<BoostIArchive>(m_event_fs);
        read_canonicals();
        preload_more_events();
    }
    else
    {
        m_sEvent_oa = std::make_unique<BoostOArchive>(m_event_fs);
        m_sCanonical_oa = std::make_unique<BoostOArchive>(m_canonical_fs);
    }
}

std::fstream BoostStorage::open_file(const fs::path& path)
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

void BoostStorage::add_query_event(QueryEvent&& qevent)
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

    qevent.event_id = next_event_id();
    save_event(can_id, qevent);
}

void BoostStorage::add_query_event(std::vector<QueryEvent>& qevents)
{
    for (auto& event : qevents)
    {
        add_query_event(std::move(event));
    }
}

Storage::Iterator BoostStorage::begin()
{
    return Storage::Iterator(this, next_event());
}

Storage::Iterator BoostStorage::end() const
{
    return Storage::Iterator(nullptr, QueryEvent {});
}

int64_t BoostStorage::num_unread() const
{
    return 42;      // TODO, num_unread only makes sense for InmemoryStorage
}

QueryEvent BoostStorage::next_event()
{
    return QueryEvent{};
}

void BoostStorage::save_canonical(int64_t can_id, const std::string& canonical)
{
    (*m_sCanonical_oa) & can_id;
    (*m_sCanonical_oa) & canonical;
}

void BoostStorage::save_event(int64_t can_id, const QueryEvent& qevent)
{
    (*m_sEvent_oa) & can_id;
    (*m_sEvent_oa) & qevent.event_id;

    int nargs = qevent.canonical_args.size();
    (*m_sEvent_oa) & nargs;
    for (const auto& a : qevent.canonical_args)
    {
        (*m_sEvent_oa) & a.pos;
        (*m_sEvent_oa) & a.value;
    }
    (*m_sEvent_oa) & qevent.session_id;

    mxb::Duration start_time_dur = qevent.start_time.time_since_epoch();
    mxb::Duration end_time_dur = qevent.end_time.time_since_epoch();
    (*m_sEvent_oa) & *reinterpret_cast<const int64_t*>(&start_time_dur);
    (*m_sEvent_oa) & *reinterpret_cast<const int64_t*>(&end_time_dur);
}

void BoostStorage::read_canonicals()
{
}

void BoostStorage::preload_more_events()
{
}
