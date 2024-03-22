/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxbase/ccdefs.hh>
#include <maxbase/stopwatch.hh>
#include <maxsimd/canonical.hh>
#include <memory>
#include <iostream>

struct Gtid
{
    uint32_t domain_id = 0;
    uint32_t server_id = 0;
    uint64_t sequence_nr = 0;
    bool is_valid() const
    {
        return server_id != 0;
    }
};

struct Trx
{
    int64_t start_event_id;
    Gtid    gtid;

    Trx(int64_t id, const Gtid& gtid)
        : start_event_id{id}
        , gtid{gtid}
    {
    }
};

inline std::ostream& operator<<(std::ostream& os, const Gtid& gtid)
{
    os << gtid.domain_id << '-' << gtid.server_id << '-' << gtid.sequence_nr;
    return os;
}

// Flags used by capture. Upper 32 bits of QueryEvent::flags.
constexpr uint64_t CAP_SESSION_CLOSE = 1ul << 32;
constexpr uint64_t CAP_ARTIFICIAL = 1ul << 33;

struct QueryEvent
{
    /* shared_ptr at this level because every kind of storage benefits
     * from the shared_ptr for caching.
     * The flags member has type_mask in lower 32 bits, upper 32 bits are for future use
     */
    std::shared_ptr<std::string> sCanonical;
    maxsimd::CanonicalArgs       canonical_args;
    int64_t                      can_id;
    int64_t                      session_id;
    uint64_t                     flags;
    mxb::TimePoint               start_time;
    mxb::TimePoint               end_time;
    int64_t                      event_id;
    std::unique_ptr<Trx>         sTrx;  // not populated when created from storage
};

inline bool is_session_close(const QueryEvent& qevent)
{
    return qevent.flags & CAP_SESSION_CLOSE;
}

inline bool is_real_event(const QueryEvent& qevent)
{
    return (qevent.flags & (CAP_ARTIFICIAL | CAP_SESSION_CLOSE)) == 0;
}

/** Abstract Storage for QueryEvents.
 *
 *  The Storage class is also a container with input iterators. As the iterators
 *  are input iterators, iterating will move data from storage to where ever
 *  it is going (into another kind of storage, or replay). The storage being moved
 *  from is still valid and events can be added which the next begin() will
 *  pick up.
 *
 *  Single thread only.
 */
class Storage
{
public:
    // Acts like a constant input iterator. The difference_type is meaningless.
    class Iterator
    {
    public:
        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = QueryEvent;
        using pointer = QueryEvent*;
        using reference = QueryEvent&;

        // non-const. The returned QueryEvent can be moved by the client
        QueryEvent& operator*();
        QueryEvent* operator->();

        Iterator& operator++();

        friend bool operator==(const Iterator& lhs, const Iterator& rhs);
        friend bool operator!=(const Iterator& lhs, const Iterator& rhs);

        Iterator(Storage* pStorage, QueryEvent&& event);

    private:
        Storage*   m_pStorage;
        QueryEvent m_event;
    };

    Storage() = default;
    virtual ~Storage() = default;
    void move_values_from(Storage& other);

    virtual void add_query_event(QueryEvent&& qevent) = 0;
    virtual void add_query_event(std::vector<QueryEvent>& qevents) = 0;

    virtual Iterator begin() = 0;
    virtual Iterator end() const = 0;

    // Return a meaningful size for a Storage. Currently there only
    // is boost storage, but it is not impossible that in-memory
    // storage is revived.
    virtual int64_t size() = 0;

protected:
    virtual QueryEvent next_event() = 0;
};

inline void Storage::move_values_from(Storage& other)
{
    constexpr int CHUNK = 10000;

    std::vector<QueryEvent> buffer;
    buffer.reserve(CHUNK);

    for (auto& event : other)
    {
        buffer.push_back(std::move(event));

        if (buffer.size() >= CHUNK)
        {
            add_query_event(buffer);
            buffer.clear();
        }
    }

    if (!buffer.empty())
    {
        add_query_event(buffer);
    }
}

inline Storage::Iterator::Iterator(Storage* pStorage, QueryEvent&& event)
    : m_pStorage(pStorage)
    , m_event(std::move(event))
{
}

inline QueryEvent& Storage::Iterator::operator*()
{
    return m_event;
}

inline QueryEvent* Storage::Iterator::operator->()
{
    return &m_event;
}

inline Storage::Iterator& Storage::Iterator::operator++()
{
    m_event = m_pStorage->next_event();
    return *this;
}

inline bool operator==(const Storage::Iterator& lhs, const Storage::Iterator& rhs)
{
    return lhs.m_event.event_id == rhs.m_event.event_id;
}

inline bool operator!=(const Storage::Iterator& lhs, const Storage::Iterator& rhs)
{
    return !(lhs == rhs);
}
