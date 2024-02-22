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

using BoostOArchive = boost::archive::text_oarchive;
using BoostIArchive = boost::archive::text_iarchive;

namespace fs = std::filesystem;

enum class ReadWrite
{
    READ_ONLY,
    WRITE_ONLY
};

class CapBoostStorage final : public Storage
{
public:
    CapBoostStorage(const fs::path& base_path, ReadWrite access = ReadWrite::WRITE_ONLY);

    void add_query_event(QueryEvent&& qevent) override;
    void add_query_event(std::vector<QueryEvent>& qevents) override;
    void add_rep_event(RepEvent&& revent) override;
    void add_rep_event(std::vector<RepEvent>& revents) override;

    Iterator begin() override;
    Iterator end() const override;

private:
    std::fstream open_file(const fs::path& path);
    void         open_capture_file(std::string_view path);
    QueryEvent   next_event() override;
    /// Save a canonical to m_canonical_path
    void save_canonical(int64_t can_id, const std::string& canonical);
    /// Save an event to m_canonical_path
    void save_query_event(int64_t can_id, const QueryEvent& qevent);
    /// Read all canonicals into memory
    void read_canonicals();
    /// While serving events, preload a number of events using
    /// some as of yet unspecified strategy.
    void preload_more_query_events();

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

    std::fstream                   m_canonical_fs;
    std::unique_ptr<BoostOArchive> m_sCanonical_oa;
    std::unique_ptr<BoostIArchive> m_sCanonical_ia;

    std::fstream                   m_query_event_fs;
    std::unique_ptr<BoostOArchive> m_sQuery_event_oa;
    std::unique_ptr<BoostIArchive> m_sQuery_event_ia;

    std::fstream                   m_rep_event_fs;
    std::unique_ptr<BoostOArchive> m_sRep_event_oa;
    std::unique_ptr<BoostIArchive> m_sRep_event_ia;
};
