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
     * The flags member has type_mask in lower 32 bits, upper 32 bits are for future use
     */
    std::shared_ptr<std::string> sCanonical;
    maxsimd::CanonicalArgs       canonical_args;
    int64_t                      session_id;
    uint64_t                     flags;
    mxb::TimePoint               start_time;
    mxb::TimePoint               end_time;
    int64_t                      event_id;

    bool is_session_close() const
    {
        return start_time == end_time;
    }
};

struct RepEvent
{
    int64_t        event_id;
    mxb::TimePoint start_time;
    mxb::TimePoint end_time;
    int32_t        num_rows;
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
    template<class T>
    class Iterator
    {
    public:
        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = T;
        using pointer = T*;
        using reference = T&;

        // non-const. The returned value can be moved by the client
        T& operator*();
        T* operator->();

        Iterator<T>& operator++();

        template<class V>
        friend bool operator==(const Iterator<V>& lhs, const Iterator<V>& rhs);

        template<class V>
        friend bool operator!=(const Iterator<V>& lhs, const Iterator<V>& rhs);

        Iterator(Storage* pStorage, T&& event);

    private:
        Storage* m_pStorage;
        T        m_event;
    };

    Storage() = default;
    virtual ~Storage() = default;
    void move_values_from(Storage& other);

    virtual void add_query_event(QueryEvent&& qevent) = 0;
    virtual void add_query_event(std::vector<QueryEvent>& qevents) = 0;
    virtual void add_rep_event(RepEvent&& revent) = 0;
    virtual void add_rep_event(std::vector<RepEvent>& revents) = 0;

    virtual Iterator<QueryEvent> begin() = 0;
    virtual Iterator<QueryEvent> end() const = 0;

protected:
    int64_t            next_can_id();
    virtual QueryEvent next_event() = 0;

private:
    int64_t m_can_id_generator{0};
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

inline int64_t Storage::next_can_id()
{
    return ++m_can_id_generator;
}

template<class T>
inline Storage::Iterator<T>::Iterator(Storage* pStorage, T&& event)
    : m_pStorage(pStorage)
    , m_event(std::move(event))
{
}

template<class T>
inline T& Storage::Iterator<T>::operator*()
{
    return m_event;
}

template<class T>
inline T* Storage::Iterator<T>::operator->()
{
    return &m_event;
}

template<class T>
inline Storage::Iterator<T>& Storage::Iterator<T>::operator++()
{
    if constexpr (std::is_same_v<T, QueryEvent> )
    {
        m_event = m_pStorage->next_event();
    }
    else if constexpr (std::is_same_v<T, RepEvent> )
    {
        static_assert(!true, "Not implemented yet");
    }
    else
    {
        static_assert(!true, "Unknown type");
    }

    return *this;
}

template<class V>
inline bool operator==(const Storage::Iterator<V>& lhs, const Storage::Iterator<V>& rhs)
{
    return lhs.m_event.event_id == rhs.m_event.event_id;
}

template<class V>
inline bool operator!=(const Storage::Iterator<V>& lhs, const Storage::Iterator<V>& rhs)
{
    return !(lhs == rhs);
}
