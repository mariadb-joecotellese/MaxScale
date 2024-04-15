/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capbooststorage.hh"

class QuerySort
{
public:
    QuerySort(CapBoostStorage& storage,
              BoostOFile& qevent_out,
              BoostOFile& tevent_out,
              CapBoostStorage::SortCallback sort_cb);

    void add_query_event(std::deque<QueryEvent>& qevents);
    void finalize();

    int64_t       num_events();
    mxb::Duration capture_duration();

private:
    CapBoostStorage&              m_storage;
    BoostOFile&                   m_qevent_out;
    CapBoostStorage::SortCallback m_sort_cb;
    std::vector<QueryEvent>       m_qevents;
    int64_t                       m_num_events = 0;
    mxb::Duration                 m_capture_duration;
};
