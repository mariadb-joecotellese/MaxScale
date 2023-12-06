/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarstorage.hh"
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

    // <canonical_hash, weak_canonical>
    // weak_ptr here, shared_ptr in QueryEvent.
    using Canonicals = std::unordered_map<int64_t, std::weak_ptr<std::string>>;
    Canonicals             m_canonicals;
    std::deque<QueryEvent> m_events;
};
