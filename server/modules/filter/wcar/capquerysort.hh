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
    QuerySort(fs::path file_path, CapBoostStorage::SortCallback sort_cb);

    void finalize();

    int64_t       num_events();
    mxb::Duration capture_duration();

private:
    fs::path                      m_file_path;
    CapBoostStorage::SortCallback m_sort_cb;
    std::vector<QueryEvent>       m_qevents;
    std::vector<TrxEvent>         m_tevents;
    int64_t                       m_num_events = 0;
    mxb::Duration                 m_capture_duration;
};
