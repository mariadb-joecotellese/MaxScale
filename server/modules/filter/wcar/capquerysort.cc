/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capquerysort.hh"

#if HAVE_STD_EXECUTION
#include <execution>
#define sort_par(...)  std::sort(std::execution::par, __VA_ARGS__)
#define merge_par(...) std::merge(std::execution::par, __VA_ARGS__)
#else
#define sort_par(...)  std::sort(__VA_ARGS__)
#define merge_par(...) std::merge(__VA_ARGS__)
#endif

// This should not really be a constant, but rather dynamic
// based on available memory as a QueryEvent has a vector
// of canonical arguments.

constexpr size_t MAX_CHUNK_SIZE = 1'000'000;

static inline bool operator<(const SortKey& lhs, const SortKey& rhs)
{
    return lhs.start_time < rhs.start_time
           || (lhs.start_time == rhs.start_time && lhs.event_id < rhs.event_id);
}

static inline bool operator==(const SortKey& lhs, const SortKey& rhs)
{
    return lhs.event_id == rhs.event_id;
}

// WorkChunk implementation
inline WorkChunk::WorkChunk(std::deque<QueryKey>&& qevents)
    : m_qkeys{std::move(qevents)}
{
}

inline bool WorkChunk::empty() const
{
    return m_qkeys.empty();
}

inline size_t WorkChunk::size() const
{
    return m_qkeys.size();
}

inline const QueryKey& WorkChunk::front() const
{
    mxb_assert(!m_qkeys.empty());
    return m_qkeys.front();
}

inline const QueryKey& WorkChunk::back() const
{
    mxb_assert(!m_qkeys.empty());
    return m_qkeys.back();
}

inline void WorkChunk::push_back(QueryKey&& qkey)
{
    m_qkeys.push_back(std::move(qkey));
}

inline void WorkChunk::sort()
{
    sort_par(m_qkeys.begin(), m_qkeys.end());
}

inline void WorkChunk::pop_front()
{
    mxb_assert(!m_qkeys.empty());
    m_qkeys.pop_front();
}

inline void WorkChunk::append(WorkChunk&& rhs)
{
    m_qkeys.insert(end(m_qkeys),
                   std::make_move_iterator(begin(rhs.m_qkeys)),
                   std::make_move_iterator(end(rhs.m_qkeys)));
}

inline WorkChunk WorkChunk::split()
{
    auto middle = m_qkeys.begin() + m_qkeys.size() / 2;
    std::deque<QueryKey> split_qkeys {std::make_move_iterator(middle),
                                      std::make_move_iterator(m_qkeys.end())};
    m_qkeys.erase(middle, m_qkeys.end());

    return WorkChunk{std::move(split_qkeys)};
}

void WorkChunk::save(const std::string& file_name)
{
    BoostOFile query_out{file_name};

    for (const auto& qkey : m_qkeys)
    {
        CapBoostStorage::save_query_event(query_out, *qkey.sQuery_event);
    }
}

inline void WorkChunk::merge(WorkChunk&& rhs)
{
    std::deque<QueryKey> res;
    merge_par(std::make_move_iterator(m_qkeys.begin()), std::make_move_iterator(m_qkeys.end()),
              std::make_move_iterator(rhs.m_qkeys.begin()), std::make_move_iterator(rhs.m_qkeys.end()),
              std::back_inserter(res));
    m_qkeys = std::move(res);
}


QuerySort::QuerySort(fs::path file_path,
                     SortCallback sort_cb)
    : m_file_path(file_path)
    , m_sort_cb(sort_cb)
{
    mxb::StopWatch total_time;

    load_sort_keys();
    sort_query_events();
    sort_trx_events();

    m_report.read_duration = m_read_time.total();
    m_report.sort_duration = m_sort_time.total();
    m_report.merge_duration = m_merge_time.total();
    m_report.total_duration = total_time.split();
}

std::vector<TrxEvent> QuerySort::release_trx_events()
{
    return std::move(m_tevents);
}

SortReport QuerySort::report()
{
    return m_report;
}

void QuerySort::load_sort_keys()
{
    m_read_time.start_interval();
    auto qevent_path = m_file_path;
    qevent_path.replace_extension("ex");
    BoostIFile query_in{qevent_path.string()};
    wall_time::TimePoint end_time;

    while (!query_in.at_end_of_stream())
    {
        auto qevent = CapBoostStorage::load_query_event(query_in);
        end_time = qevent.end_time;
        m_keys.emplace_back(qevent.start_time, qevent.event_id);
    }
    m_read_time.end_interval();

    m_sort_time.start_interval();
    sort_par(m_keys.begin(), m_keys.end());
    m_sort_time.end_interval();

    m_report.events = m_keys.size();
    m_report.capture_duration = end_time - m_keys.front().start_time;
}

void QuerySort::sort_query_events()
{
    auto key_ite = m_keys.begin();

    auto qevent_path = m_file_path;
    qevent_path.replace_extension("ex");
    BoostIFile query_in{qevent_path.string()};
    BoostOFile query_out{qevent_path.string()};

    m_sort_time.start_interval();

    WorkChunk work_chunk;
    fill_chunk(work_chunk, query_in);   // also manipulates m_sort_time

    while (key_ite != m_keys.end())
    {

        while (!work_chunk.empty() && work_chunk.front() == *key_ite)
        {
            ++key_ite;
            ++m_report.events_direct_to_output;
            m_sort_cb(*work_chunk.front().sQuery_event);
            CapBoostStorage::save_query_event(query_out, std::move(*work_chunk.front().sQuery_event));
            work_chunk.pop_front();
        }

        if (work_chunk.size() == MAX_CHUNK_SIZE)
        {
            m_external_chunks.save(work_chunk.split());
        }

        if (fill_chunk(work_chunk, query_in))
        {
            break;
        }
    }

    m_sort_time.end_interval();
    m_merge_time.start_interval();

    auto merge_chunks = m_external_chunks.load();
    m_report.merge_files = merge_chunks.size();

    if (!work_chunk.empty())
    {
        merge_chunks.push_back(StreamChunk(std::move(work_chunk)));
    }

    // merge the chunks.
    while (!merge_chunks.empty())
    {
        auto from_ite = merge_chunks.end();

        for (auto ite = begin(merge_chunks); ite != end(merge_chunks); ++ite)
        {
            if (ite->front() == *key_ite)
            {
                from_ite = ite;
                break;
            }
        }
        mxb_assert(from_ite != merge_chunks.end());

        while (from_ite->front() == *key_ite)
        {
            m_sort_cb(*from_ite->front().sQuery_event);
            CapBoostStorage::save_query_event(query_out, std::move(*from_ite->front().sQuery_event));
            from_ite->pop_front();
            ++key_ite;
            if (from_ite->empty())
            {
                merge_chunks.erase(from_ite);
                break;
            }
        }
    }
    m_merge_time.end_interval();
}

void QuerySort::sort_trx_events()
{
    auto trx_path = m_file_path.replace_extension("gx");
    BoostIFile trx_in{trx_path.string()};
    m_tevents = CapBoostStorage::load_trx_events(trx_in);

    // Sort by gtid, which can lead to out of order end_time. The number of
    // gtids is small relative to query events and fit in memory (TODO document).
    sort_par(m_tevents.begin(), m_tevents.end(), [](const auto& lhs, const auto& rhs){
        if (lhs.gtid.domain_id == lhs.gtid.domain_id)
        {
            return lhs.gtid.sequence_nr < rhs.gtid.sequence_nr;
        }
        else
        {
            return lhs.end_time < rhs.end_time;
        }
    });

    BoostOFile trx_out{trx_path.string()};
    for (auto& e : m_tevents)
    {
        CapBoostStorage::save_trx_event(trx_out, e);
    }
}

bool QuerySort::fill_chunk(WorkChunk& chunk, BoostIFile& query_in)
{
    m_sort_time.end_interval();
    m_read_time.start_interval();
    auto n_existing_events = chunk.size();
    WorkChunk new_chunk;
    while (!query_in.at_end_of_stream() && new_chunk.size() + n_existing_events < MAX_CHUNK_SIZE)
    {
        auto qevent = CapBoostStorage::load_query_event(query_in);
        new_chunk.push_back(QueryKey(std::make_unique<QueryEvent>(std::move(qevent))));
    }

    m_read_time.end_interval();

    if (new_chunk.empty())
    {
        return true;
    }

    m_sort_time.start_interval();
    new_chunk.sort();
    m_sort_time.end_interval();

    if (chunk.empty())
    {
        chunk = std::move(new_chunk);
    }
    else if (chunk.back() < new_chunk.front())
    {
        chunk.append(std::move(new_chunk));
    }
    else
    {
        chunk.merge(std::move(new_chunk));
    }

    m_sort_time.start_interval();

    return false;
}

StreamChunk::StreamChunk(WorkChunk&& work_chunk)
    : m_qkeys{std::move(work_chunk.m_qkeys)}
{
}

StreamChunk::StreamChunk(const std::string& file_name)
    : m_infile{std::make_unique<BoostIFile>(file_name)}
{
}

inline bool StreamChunk::empty()
{
    if (m_qkeys.empty() && m_infile)
    {
        read_more();
    }
    return m_qkeys.empty();
}

inline const QueryKey& StreamChunk::front()
{
    if (m_qkeys.empty() && m_infile)
    {
        read_more();
    }
    mxb_assert(!m_qkeys.empty());
    return m_qkeys.front();
}

void StreamChunk::pop_front()
{
    mxb_assert(!m_qkeys.empty());
    m_qkeys.pop_front();
}

void StreamChunk::read_more()
{
    mxb_assert(m_infile);

    while (m_qkeys.size() < 1000 && !m_infile->at_end_of_stream())
    {
        m_qkeys.emplace_back(std::make_unique<QueryEvent>(CapBoostStorage::load_query_event(*m_infile)));
    }
}

ExternalChunks::ExternalChunks()
    : m_chunk_dir(m_dir_name)
{
}

void ExternalChunks::save(WorkChunk&& chunk)
{
    std::ostringstream os;
    os << m_dir_name << '/' << m_file_base_name << std::setfill('0') << std::setw(4) << m_chunk_ctr++;
    m_file_names.push_back(os.str());
    chunk.save(os.str());
}

std::vector<StreamChunk> ExternalChunks::load()
{
    std::vector<StreamChunk> ret;
    for (auto& file_name : m_file_names)
    {
        ret.push_back(StreamChunk {file_name});
    }
    return ret;
}
