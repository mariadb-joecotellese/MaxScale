/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "capquerysort.hh"
#include <execution>

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

// Chunk implementation
inline Chunk::Chunk(std::deque<QueryKey>&& qevents)
    : m_qkeys{std::move(qevents)}
{
}

inline bool Chunk::empty() const
{
    return m_qkeys.empty();
}

inline size_t Chunk::size() const
{
    return m_qkeys.size();
}

inline const QueryKey& Chunk::front() const
{
    mxb_assert(!m_qkeys.empty());
    return m_qkeys.front();
}

inline const QueryKey& Chunk::back() const
{
    mxb_assert(!m_qkeys.empty());
    return m_qkeys.back();
}

inline void Chunk::push_back(QueryKey&& qkey)
{
    m_qkeys.push_back(std::move(qkey));
}

inline void Chunk::sort()
{
    std::sort(std::execution::par, m_qkeys.begin(), m_qkeys.end());
}

inline void Chunk::pop_front()
{
    mxb_assert(!m_qkeys.empty());
    m_qkeys.pop_front();
}

inline void Chunk::append(Chunk&& rhs)
{
    m_qkeys.insert(end(m_qkeys),
                   std::make_move_iterator(begin(rhs.m_qkeys)),
                   std::make_move_iterator(end(rhs.m_qkeys)));
}

inline Chunk Chunk::split()
{
    auto middle = m_qkeys.begin() + m_qkeys.size() / 2;
    std::deque<QueryKey> split_qkeys {std::make_move_iterator(middle),
                                      std::make_move_iterator(m_qkeys.end())};
    m_qkeys.erase(middle, m_qkeys.end());

    return Chunk{std::move(split_qkeys)};
}

inline void Chunk::merge(Chunk&& rhs)
{
    std::deque<QueryKey> res;
    std::merge(std::execution::par,
               std::make_move_iterator(m_qkeys.begin()), std::make_move_iterator(m_qkeys.end()),
               std::make_move_iterator(rhs.m_qkeys.begin()), std::make_move_iterator(rhs.m_qkeys.end()),
               std::back_inserter(res));
    m_qkeys = std::move(res);
}


QuerySort::QuerySort(fs::path file_path,
                     SortCallback sort_cb)
    : m_file_path(file_path)
    , m_sort_cb(sort_cb)
{
    load_sort_keys();
    sort_query_events();
    sort_trx_events();
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
    auto qevent_path = m_file_path;
    qevent_path.replace_extension("ex");
    BoostIFile query_in{qevent_path.string()};
    while (!query_in.at_end_of_stream())
    {
        auto qevent = CapBoostStorage::load_query_event(query_in);
        m_keys.emplace_back(qevent.start_time, qevent.event_id);
    }

    std::sort(std::execution::par, m_keys.begin(), m_keys.end());
}

void QuerySort::sort_query_events()
{
    auto key_ite = m_keys.begin();

    auto qevent_path = m_file_path;
    qevent_path.replace_extension("ex");
    BoostIFile query_in{qevent_path.string()};
    BoostOFile query_out{qevent_path.string()};

    Chunk work_chunk;
    fill_chunk(work_chunk, query_in);

    int directly_out = 0;
    while (key_ite != m_keys.end())
    {
        while (!work_chunk.empty() && work_chunk.front() == *key_ite)
        {
            ++key_ite;
            ++directly_out;
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

    auto merge_chunks = m_external_chunks.load();
    if (!work_chunk.empty())
    {
        merge_chunks.push_back(std::move(work_chunk));
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
}

void QuerySort::sort_trx_events()
{
    auto trx_path = m_file_path.replace_extension("gx");
    BoostIFile trx_in{trx_path.string()};
    m_tevents = CapBoostStorage::load_trx_events(trx_in);

    // Sort by gtid, which can lead to out of order end_time. The number of
    // gtids is small relative to query events and fit in memory (TODO document).
    std::sort(std::execution::par, m_tevents.begin(), m_tevents.end(), []
              (const auto& lhs, const auto& rhs){
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

bool QuerySort::fill_chunk(Chunk& chunk, BoostIFile& query_in)
{
    auto n_existing_events = chunk.size();
    Chunk new_chunk;
    while (!query_in.at_end_of_stream() && new_chunk.size() + n_existing_events < MAX_CHUNK_SIZE)
    {
        auto qevent = CapBoostStorage::load_query_event(query_in);
        new_chunk.push_back(QueryKey(std::make_unique<QueryEvent>(std::move(qevent))));
    }

    if (new_chunk.empty())
    {
        return true;
    }

    new_chunk.sort();

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

    return false;
}
