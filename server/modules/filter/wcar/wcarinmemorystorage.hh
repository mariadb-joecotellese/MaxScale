/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarstorage.hh"
#include <map>
#include <unordered_map>
#include <deque>

class InmemoryStorage final : public Storage
{
public:
    void add_query_event(QueryEvent&& qevent) override;
    void add_query_event(std::vector<QueryEvent>& qevents) override;

    Iterator begin() override;
    Iterator end() const override;
    int64_t  num_unread() const override;

private:
    QueryEvent next_event() override;

    struct CanonicalEntry
    {
        int64_t     can_id;
        std::string canonical;
        CanonicalEntry(int64_t can_id, std::string&& canonical)
            : can_id(can_id)
            , canonical(std::move(canonical))
        {
        }
    };

    // <canonical hash, CanonicalEntry>
    // The reason for using a map is so that the canonical can be moved in and out
    // from the container. Could use an unordered_set: given an iterator, break the
    // rules and const_cast the element, extract canonical and erase using the iterator.
    using Canonicals = std::map<int64_t, CanonicalEntry>;

    // Reverse lookup into above map using can_id. Not const_iterator because
    // the element is moved from, before it is erased.
    using CanonicalLookup = std::unordered_map<int64_t, Canonicals::iterator>;

    struct CaptureEvent
    {
        int64_t event_id;
        int64_t can_id;

        maxsimd::CanonicalArgs args;

        CaptureEvent(int64_t event_id, int64_t can_id, maxsimd::CanonicalArgs&& args)
            : event_id(event_id)
            , can_id(can_id)
            , args(std::move(args))
        {
        }
    };

    // capture events
    using CaptureEvents = std::deque<CaptureEvent>;

    Canonicals      m_canonicals;
    CanonicalLookup m_canonical_lookup;
    CaptureEvents   m_events;
    int64_t         m_read_event_idx = 0;
};
