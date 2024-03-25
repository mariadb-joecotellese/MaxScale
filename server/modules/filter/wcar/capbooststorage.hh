/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capstorage.hh"
#include "capconfig.hh"
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <deque>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

using BoostOArchive = boost::archive::binary_oarchive;
using BoostIArchive = boost::archive::binary_iarchive;

namespace fs = std::filesystem;

enum class ReadWrite
{
    READ_ONLY,
    WRITE_ONLY
};

struct TrxEvent
{
    int64_t        session_id;
    int64_t        start_event_id;
    int64_t        end_event_id;
    mxb::TimePoint end_time;
    Gtid           gtid;
    bool           completed = false;
};

template<typename BoostArchive>
class BoostFile
{
public:
    using BoostArchiveType = BoostArchive;

    // Boost archive does not support appending, archives are
    // opened on demand. Less safe, but avoids flags.
    // If a file for output already exists the write pointer is
    // moved to the beginning for overwriting.
    BoostFile(const fs::path& path);
    BoostArchive& operator*();
    bool          at_end_of_stream();
    // Rewind to read and (over) write from the beginning of the file.
    void rewind();
    // Return read pos for input and write pos for output
    int64_t tell();
private:
    void open();
    // Could be ifstream or ofstream based on a template arg, but an
    // ofstream cannot be opened for writing and
    fs::path                      m_path;
    std::fstream                  m_fs;
    std::unique_ptr<BoostArchive> m_sArchive;
};

using BoostOFile = BoostFile<BoostOArchive>;
using BoostIFile = BoostFile<BoostIArchive>;

class CapBoostStorage final : public Storage
{
public:
    CapBoostStorage(const fs::path& base_path, ReadWrite access);

    void add_query_event(QueryEvent&& qevent) override;
    void add_query_event(std::vector<QueryEvent>& qevents) override;

    Iterator begin() override;
    Iterator end() const override;
    // Return file size
    int64_t size() override;

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

    /**
     * Sort query event file, write back and re-open.
     * Call sort_cb() for each event in sorted order.
     * @return A report on the sorting and capture statistics
     */
    using SortCallback = std::function<void (const QueryEvent&)>;
    SortReport sort_query_event_file(const SortCallback& sort_cb);

    /**
     * Get all of the canonicals mapped to their IDs
     *
     * @return The canonical form of the query strings mapped to their IDs
     */
    std::map<int64_t, std::shared_ptr<std::string>> canonicals() const;

    /**
     * Convert the events to SQL and write them into a file
     *
     * @param path The path to the file where the output is written
     */
    void events_to_sql(fs::path path);

    std::vector<TrxEvent> release_trx_events()
    {
        return std::move(m_tevents);
    }

private:
    friend class QuerySort;

    QueryEvent next_event() override;
    // Save an event to m_canonical_path
    void save_query_event(BoostOFile& bof, const QueryEvent& qevent);
    // Save a canonical to m_canonical_path
    void save_canonical(BoostOFile& bof, int64_t can_id, const std::string& canonical);
    // Save gtid event, along with identifiers
    void save_gtid_event(BoostOFile& bof, const TrxEvent& qevent);
    // Read all canonicals into memory
    void read_canonicals();
    // Load and return all gtid events.
    TrxEvent load_gtid_event();
    // Read all gtid events to memory. Unlike canonicals,
    // these should always fit in memory.
    void load_gtid_events();
    // Preload QueryEvents.
    void preload_query_events(int64_t max_loaded);

    std::shared_ptr<std::string> find_canonical(int64_t can_id);

    // For now, assume that the canonicals always fit in memory.
    // Later, the strategy can be to keep the std::unordered_map entries,
    // but reset shared_ptrs to regain memory. The entries must be kept if an
    // add_query_event() is called with a canonical that hashes to an
    // existing entry, in which case only the can_id is needed as the sql
    // has already been written to disk.
    struct CanonicalEntry
    {
        int64_t                      can_id;
        std::shared_ptr<std::string> sCanonical;
    };

    using Canonicals = std::unordered_map<int64_t, CanonicalEntry>;
    Canonicals m_canonicals;

    using QueryEvents = std::deque<QueryEvent>;
    QueryEvents m_query_events;

    using TrxEvents = std::vector<TrxEvent>;
    TrxEvents m_tevents;

    fs::path  m_base_path;
    fs::path  m_canonical_path;
    fs::path  m_query_event_path;
    fs::path  m_gtid_path;
    ReadWrite m_access;

    std::unique_ptr<BoostOFile> m_sCanonical_out;
    std::unique_ptr<BoostIFile> m_sCanonical_in;

    std::unique_ptr<BoostOFile> m_sQuery_event_out;
    std::unique_ptr<BoostIFile> m_sQuery_event_in;

    std::unique_ptr<BoostOFile> m_sGtid_out;
    std::unique_ptr<BoostIFile> m_sGtid_in;
};

// Inline definitions

template<typename BoostArchive>
BoostFile<BoostArchive>::BoostFile(const fs::path& path)
try
    : m_path(path)
    , m_fs(path.string())
{
    if (!m_fs.is_open())
    {
        m_fs.open(path, std::ios_base::out);
    }
}
catch (std::exception& ex)
{
    MXB_THROW(WcarError, "Could not open file " << path << ' ' << mxb_strerror(errno));
}

template<typename BoostArchive>
void BoostFile<BoostArchive>::open()
{
    if (m_sArchive)
    {
        return;
    }

    if constexpr (std::is_same_v<BoostIArchive, BoostArchive> )
    {
        if (m_fs.peek() == EOF)
        {   // The input file file is empty. boost would throw an exception
            return;
        }
    }

    try
    {
        m_sArchive = std::make_unique<BoostArchive>(m_fs);
    }
    catch (std::exception& ex)
    {
        MXB_THROW(WcarError, "Could open boost archive " << m_path << ' ' << mxb_strerror(errno));
    }
}

template<typename BoostArchive>
BoostArchive& BoostFile<BoostArchive>::operator*()
{
    open();

    return *m_sArchive;
}

template<typename BoostArchive>
bool BoostFile<BoostArchive>::at_end_of_stream()
{
    open();

    if constexpr (std::is_same_v<boost::archive::binary_iarchive, BoostArchive> )
    {
        return m_fs.peek() == EOF;
    }
    else if constexpr (std::is_same_v<boost::archive::text_iarchive, BoostArchive> )
    {
        return m_fs.peek() == '\n';
    }
    else
    {
        static_assert(mxb::always_false_v<BoostArchive>, "at_end_of_stream() only for input");
    }
}

template<typename BoostArchive>
int64_t BoostFile<BoostArchive>::tell()
{
    if (!m_sArchive)
    {
        return 0;
    }

    if constexpr (std::is_same_v<BoostOArchive, BoostArchive> )
    {
        return m_fs.tellg();
    }
    else if constexpr (std::is_same_v<BoostIArchive, BoostArchive> )
    {
        return m_fs.tellp();
    }
    else
    {
        static_assert(mxb::always_false_v<BoostArchive>, "Unknown type");
    }
}

template<typename BoostArchive>
void BoostFile<BoostArchive>::rewind()
{
    m_fs.seekg(0);
    m_fs.seekp(0);
}
