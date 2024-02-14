/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capinmemorystorage.hh"
#include <maxbase/assert.hh>

void CapInmemoryStorage::add_query_event(QueryEvent&& qevent)
{
    int64_t hash{static_cast<int64_t>(std::hash<std::string> {}(*qevent.sCanonical))};
    auto canon_ite = m_canonicals.find(hash);

    if (canon_ite != std::end(m_canonicals))
    {
        if (std::shared_ptr<std::string> sCanonical = canon_ite->second.lock())
        {
            // cached
            qevent.sCanonical = std::move(sCanonical);
        }
        else
        {
            // cache entry expired
            m_canonicals.erase(canon_ite);
            m_canonicals.emplace(hash, qevent.sCanonical);
        }
    }
    else
    {
        // insert a weak_ptr
        m_canonicals.emplace(hash, qevent.sCanonical);
    }

    m_events.emplace_back(std::move(qevent));
}

void CapInmemoryStorage::add_query_event(std::vector<QueryEvent>& qevents)
{
    for (auto& event : qevents)
    {
        add_query_event(std::move(event));
    }
}

void CapInmemoryStorage::add_rep_event(RepEvent&& revent)
{
    throw std::runtime_error("CapInmemoryStorage::add_rep_event not implemented yet");
}

void CapInmemoryStorage::add_rep_event(std::vector<RepEvent>& revents)
{
    for (auto& revent : revents)
    {
        add_rep_event(std::move(revent));
    }
}

Storage::Iterator CapInmemoryStorage::begin()
{
    return Storage::Iterator(this, next_event());
}

Storage::Iterator CapInmemoryStorage::end() const
{
    return Storage::Iterator(nullptr, QueryEvent {});
}

int64_t CapInmemoryStorage::num_unread() const
{
    return m_events.size();
}

QueryEvent CapInmemoryStorage::next_event()
{
    if (m_events.empty())
    {
        return QueryEvent{};
    }

    QueryEvent ret{std::move(m_events.front())};
    m_events.pop_front();

    if (m_events.empty())
    {
        m_canonicals.clear();
    }

    return ret;
}
