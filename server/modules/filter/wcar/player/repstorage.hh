/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include <maxbase/ccdefs.hh>
#include <maxbase/stopwatch.hh>
#include <vector>

struct RepEvent
{
    int64_t        event_id {0};
    mxb::TimePoint start_time {0s};
    mxb::TimePoint end_time {0s};
    int64_t        can_id {0};
    int32_t        num_rows {0};
};

class RepStorage
{
public:
    class Iterator
    {
    public:
        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = RepEvent;
        using pointer = RepEvent*;
        using reference = RepEvent&;

        const RepEvent& operator*() const;
        const RepEvent* operator->() const;

        Iterator& operator++();

        friend bool operator==(const Iterator& lhs, const Iterator& rhs);
        friend bool operator!=(const Iterator& lhs, const Iterator& rhs);

        Iterator(RepStorage* pStorage, RepEvent&& event);
    private:
        RepStorage* m_pStorage;
        RepEvent    m_event;
    };

    RepStorage() = default;
    virtual ~RepStorage() = default;

    virtual void add_rep_event(RepEvent&& qevent) = 0;
    virtual void add_rep_event(std::vector<RepEvent>& qevents) = 0;

    virtual Iterator begin() = 0;
    virtual Iterator end() const = 0;

protected:
    virtual RepEvent next_rep_event() = 0;
};

inline RepStorage::Iterator::Iterator(RepStorage* pStorage, RepEvent&& event)
    : m_pStorage(pStorage)
    , m_event(std::move(event))
{
}

inline const RepEvent& RepStorage::Iterator::operator*() const
{
    return m_event;
}

inline const RepEvent* RepStorage::Iterator::operator->() const
{
    return &m_event;
}

inline RepStorage::Iterator& RepStorage::Iterator::operator++()
{
    m_event = m_pStorage->next_rep_event();
    return *this;
}

inline bool operator==(const RepStorage::Iterator& lhs, const RepStorage::Iterator& rhs)
{
    return lhs.m_event.event_id == rhs.m_event.event_id;
}

inline bool operator!=(const RepStorage::Iterator& lhs, const RepStorage::Iterator& rhs)
{
    return !(lhs == rhs);
}
