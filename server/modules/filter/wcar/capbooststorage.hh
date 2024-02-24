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
    void add_rep_event(RepEvent&& revent) override;
    void add_rep_event(std::vector<RepEvent>& revents) override;

    Iterator begin() override;
    Iterator end() const override;

    // Sort query event file, write back and re-open.
    void sort_query_event_file();

private:
    QueryEvent next_event() override;
    // Save a canonical to m_canonical_path
    void save_canonical(int64_t can_id, const std::string& canonical);
    // Save an event to m_canonical_path
    void save_query_event(int64_t can_id, const QueryEvent& qevent);
    // Read all canonicals into memory
    void read_canonicals();
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

    fs::path  m_base_path;
    fs::path  m_canonical_path;
    fs::path  m_query_event_path;
    fs::path  m_rep_event_path;
    ReadWrite m_access;

    std::unique_ptr<BoostOFile> m_sCanonical_out;
    std::unique_ptr<BoostIFile> m_sCanonical_in;

    std::unique_ptr<BoostOFile> m_sQuery_event_out;
    std::unique_ptr<BoostIFile> m_sQuery_event_in;

    std::unique_ptr<BoostOFile> m_sRep_event_out;
    std::unique_ptr<BoostIFile> m_sRep_event_in;
};
