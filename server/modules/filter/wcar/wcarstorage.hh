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

struct QueryEvent
{
    /* shared_ptr at this level because every kind of storage benefits
     * from the shared_ptr for caching.
     */
    std::shared_ptr<std::string> sCanonical;
    maxsimd::CanonicalArgs       canonical_args;
    mxb::TimePoint               start_time;
    mxb::TimePoint               end_time;
    int64_t                      event_id = -1; // managed by storage
};

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
    void move_values_from(Storage& other);

    virtual void add_query_event(QueryEvent&& qevent) = 0;
    virtual void add_query_event(std::vector<QueryEvent>& qevents) = 0;

    virtual Iterator begin() = 0;
    virtual Iterator end() const = 0;

    // Can be used to conditionally write to external storage in batches.
    virtual int64_t num_unread() const = 0;

protected:
    int64_t            next_can_id();
    int64_t            next_event_id();
    virtual QueryEvent next_event() = 0;

private:
    int64_t m_can_id_generator{0};
    int64_t m_event_id_generator{0};
};

inline void Storage::move_values_from(Storage& other)
{
    for (auto& event : other)
    {
        add_query_event(std::move(event));
    }
}

inline int64_t Storage::next_can_id()
{
    return ++m_can_id_generator;
}

inline int64_t Storage::next_event_id()
{
    return ++m_event_id_generator;
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
