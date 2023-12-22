/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarstorage.hh"
#include "wcarconfig.hh"
#include <unordered_map>
#include <filesystem>
#include <fstream>
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

class BoostStorage final : public Storage
{
public:
    BoostStorage(const fs::path& base_path, ReadWrite access = ReadWrite::WRITE_ONLY);

    void add_query_event(QueryEvent&& qevent) override;
    void add_query_event(std::vector<QueryEvent>& qevents) override;

    Iterator begin() override;
    Iterator end() const override;
    int64_t  num_unread() const override;

private:
    std::fstream open_file(const fs::path& path);
    void         open_capture_file(std::string_view path);
    QueryEvent   next_event() override;
    /// Save a canonical to m_canonical_path
    void save_canonical(int64_t can_id, const std::string& canonical);
    /// Save an event to m_canonical_path
    void save_event(int64_t can_id, const QueryEvent& qevent);
    /// Read all canonicals into memory
    void read_canonicals();
    /// While serving events, preload a number of events using
    /// some as of yet unspecified strategy.
    void preload_more_events();

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

    fs::path  m_base_path;
    fs::path  m_canonical_path;
    fs::path  m_event_path;
    ReadWrite m_access;

    std::fstream                   m_canonical_fs;
    std::fstream                   m_event_fs;
    std::unique_ptr<BoostOArchive> m_sCanonical_oa;
    std::unique_ptr<BoostIArchive> m_sCanonical_ia;
    std::unique_ptr<BoostOArchive> m_sEvent_oa;
    std::unique_ptr<BoostIArchive> m_sEvent_ia;
};
