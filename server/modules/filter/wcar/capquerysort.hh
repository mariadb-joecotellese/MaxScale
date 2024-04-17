/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capbooststorage.hh"
#include <iomanip>

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

struct SortKey
{
    SortKey(wall_time::TimePoint start_time, int64_t event_id)
        : start_time(start_time)
        , event_id(event_id)
    {
    }
    wall_time::TimePoint start_time;
    int64_t              event_id;
};

struct QueryKey : public SortKey
{
    explicit QueryKey(std::unique_ptr<QueryEvent> sQuery_event)
        : SortKey{sQuery_event->start_time, sQuery_event->event_id}
        , sQuery_event(std::move(sQuery_event))
    {
    }

    QueryKey(QueryKey&&) = default;
    QueryKey& operator=(QueryKey&&) = default;

    std::unique_ptr<QueryEvent> sQuery_event;
};

/* A chunk of QueryKeys used for implementing
 * merge-sort of QueryEvents
 */
class WorkChunk
{
public:
    explicit WorkChunk(std::deque<QueryKey>&& qevents);
    WorkChunk() = default;
    WorkChunk(WorkChunk&&) = default;
    WorkChunk& operator=(WorkChunk&&) = default;

    [[nodiscard]] bool empty() const;
    size_t             size() const;

    const QueryKey& front() const;
    const QueryKey& back() const;

    void  push_back(QueryKey&& qkey);
    void  sort();
    void  pop_front();
    void  append(WorkChunk&& rhs);
    void  merge(WorkChunk&& rhs);
    WorkChunk split();

private:
    std::deque<QueryKey> m_qkeys;
};

class ExternalChunks
{
public:
    void save(WorkChunk&& chunk)
    {
        std::ostringstream os;
        os << chunk_file_base_name << std::setfill('0') << std::setw(4) << m_chunk_ctr++;
        m_chunk_files.emplace(os.str(), std::move(chunk));
    }

    std::vector<WorkChunk> load()
    {
        std::vector<WorkChunk> ret;
        for (auto& p : m_chunk_files)
        {
            ret.push_back(std::move(p.second));
        }
        return ret;
    }

private:
    static inline const std::string chunk_file_base_name = "/tmp/chunk-";
    int                             m_chunk_ctr = 0;
    // These would be read and written from boost
    std::map<std::string, WorkChunk> m_chunk_files;
};

class QuerySort
{
public:
    QuerySort(fs::path file_path, SortCallback sort_cb);

    std::vector<TrxEvent> release_trx_events();
    SortReport            report();
private:
    void load_sort_keys();
    void sort_query_events();
    void sort_trx_events();
    // Returns true when query_in has been all read
    bool fill_chunk(WorkChunk& chunk, BoostIFile& query_in);

    fs::path                m_file_path;
    SortCallback            m_sort_cb;
    std::vector<SortKey>    m_keys;
    ExternalChunks          m_external_chunks;
    std::vector<QueryEvent> m_qevents;
    std::vector<TrxEvent>   m_tevents;
    SortReport              m_report;
};
