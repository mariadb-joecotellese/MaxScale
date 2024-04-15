/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capbooststorage.hh"

using SortCallback = std::function<void (const QueryEvent&)>;

struct SortReport
{
    // Statistics about the sorting
    mxb::Duration total {0};
    mxb::Duration read {0};
    mxb::Duration sort {0};
    mxb::Duration write {0};

    // Statistics about the capture itself
    int64_t       events {0};
    mxb::Duration capture_duration {0};
};

class QuerySort
{
public:
    QuerySort(fs::path file_path, SortCallback sort_cb);

    std::vector<TrxEvent> release_trx_events();
    SortReport            report();
private:
    fs::path                m_file_path;
    SortCallback            m_sort_cb;
    std::vector<QueryEvent> m_qevents;
    std::vector<TrxEvent>   m_tevents;
    SortReport              m_report;
};
