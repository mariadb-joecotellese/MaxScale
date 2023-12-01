/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "wcarinmemorystorage.hh"
#include <maxbase/assert.hh>
#include <sstream>

void InmemoryStorage::add_query_event(QueryEvent&& qevent)
{
    auto hash = std::hash<std::string> {}(qevent.canonical);
    auto can_id = next_can_id();
    CanonicalEntry entry{can_id, std::move(qevent.canonical)};

    auto p = m_canonicals.try_emplace(hash, std::move(entry));
    if (p.second == false)
    {
        if (p.first->second.canonical != entry.canonical)
        {
            // TODO potentially serious, although very unlikely error. Decide what to do.
            MXB_SERROR("Hash collision. In storage:'" << p.first->first << "' attempt to insert '"
                                                      << entry.canonical);
        }
    }

    m_canonical_lookup.emplace(can_id, p.first);
    m_events.emplace_back(next_event_id(), can_id, std::move(qevent.canonical_args));
}

void InmemoryStorage::add_query_event(std::vector<QueryEvent>& qevents)
{
    for (auto& event : qevents)
    {
        add_query_event(std::move(event));
    }
}

Storage::Iterator InmemoryStorage::begin()
{
    return Storage::Iterator(this, next_event());
}

Storage::Iterator InmemoryStorage::end() const
{
    return Storage::Iterator(nullptr, QueryEvent {});
}

int64_t InmemoryStorage::num_unread() const
{
    return m_events.size();
}

QueryEvent InmemoryStorage::next_event()
{
    if (m_events.empty())
    {
        return QueryEvent{};
    }

    auto& cap_event = m_events.front();
    auto lookup_ite = m_canonical_lookup.find(cap_event.can_id);
    auto& canon_ite = lookup_ite->second;

    QueryEvent ret{std::move(canon_ite->second.canonical), std::move(cap_event.args), m_read_event_idx};

    ++m_read_event_idx;
    m_canonical_lookup.erase(lookup_ite);
    m_canonicals.erase(canon_ite);
    m_events.pop_front();

    return ret;
}
