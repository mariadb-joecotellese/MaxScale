/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capbooststorage.hh"
#include <maxbase/temp_file.hh>
#include <iomanip>

using SortCallback = std::function<void (const QueryEvent&)>;

struct SortReport
{
    // Statistics about the sorting
    mxb::Duration total_duration {0};
    mxb::Duration read_duration {0};
    mxb::Duration sort_duration {0};
    mxb::Duration merge_duration {0};
    int           events_direct_to_output {0};
    int           merge_files {0};

    // Statistics about the capture itself
    int64_t       events {0};
    mxb::Duration capture_duration {0};
};

struct SortKey
{
    SortKey() = default;
    SortKey(wall_time::TimePoint start_time, int64_t event_id)
        : start_time(start_time)
        , event_id(event_id)
    {
    }
    wall_time::TimePoint start_time {wall_time::Duration{0}};
    int64_t              event_id {0};
};

struct QueryKey : public SortKey
{
    QueryKey() = default;
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

    void      push_back(QueryKey&& qkey);
    void      sort();
    void      pop_front();
    void      append(WorkChunk&& rhs);
    void      merge(WorkChunk&& rhs);
    WorkChunk split();

    void save(const std::string& file_name);
private:
    friend class StreamChunk;
    std::deque<QueryKey> m_qkeys;
};

/** A chunk of QueryKeys streamed from a file, or alternatively
 *  created from a WorkChunk.
 */
class StreamChunk
{
public:
    explicit StreamChunk(WorkChunk&& work_chunk);
    explicit StreamChunk(const std::string& file_name);
    StreamChunk(StreamChunk&&) = default;
    StreamChunk& operator=(StreamChunk&&) = default;

    [[nodiscard]] bool empty();
    const QueryKey&    front();
    void               pop_front();
private:
    void read_more();
    std::deque<QueryKey>        m_qkeys;
    std::unique_ptr<BoostIFile> m_infile;
};

class ExternalChunks
{
public:
    ExternalChunks();
    void                     save(WorkChunk&& chunk);
    std::vector<StreamChunk> load();

private:
    static inline const std::string m_dir_name = "/tmp/query-chunks";
    static inline const std::string m_file_base_name = "chunk-";
    int                             m_chunk_ctr = 0;
    mxb::TempDirectory              m_chunk_dir;
    std::vector<std::string>        m_file_names;
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
    bool fill_chunk(WorkChunk& chunk, BoostIFile& query_in, int64_t more);

    fs::path                m_file_path;
    SortCallback            m_sort_cb;
    std::vector<SortKey>    m_keys;
    ExternalChunks          m_external_chunks;
    std::vector<QueryEvent> m_qevents;
    std::vector<TrxEvent>   m_tevents;
    SortReport              m_report;
    mxb::IntervalTimer      m_read_time;
    mxb::IntervalTimer      m_sort_time;
    mxb::IntervalTimer      m_merge_time;
};
