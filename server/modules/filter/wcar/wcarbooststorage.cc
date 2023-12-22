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
}

void BoostStorage::add_query_event(std::vector<QueryEvent>& qevents)
{
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
}

void BoostStorage::save_event(int64_t can_id, const QueryEvent& qevent)
{
}

void BoostStorage::read_canonicals()
{
}

void BoostStorage::preload_more_events()
{
}
