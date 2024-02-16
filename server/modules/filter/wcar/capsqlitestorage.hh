/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "capstorage.hh"
#include <filesystem>
#include <sqlite3.h>

namespace fs = std::filesystem;

enum class Access
{
    READ_ONLY,
    READ_WRITE
};

class CapSqliteStorage final : public Storage
{
public:
    CapSqliteStorage(const fs::path& path, Access access = Access::READ_WRITE);
    ~CapSqliteStorage();

    void add_query_event(QueryEvent&& qevent) override;
    void add_query_event(std::vector<QueryEvent>& qevents) override;
    void add_rep_event(RepEvent&& revent) override;
    void add_rep_event(std::vector<RepEvent>& revents) override;

    Iterator begin() override;
    Iterator end() const override;

    // this will have to become virtual if other storages are used for rep_events
    void truncate_rep_events() const;

    void set_sort_by_start_time()
    {
        m_sort_by_start_time = true;
    }

private:
    QueryEvent next_event() override;

    void                   sqlite_execute(const std::string& sql);
    void                   sqlite_prepare(const std::string& sql, sqlite3_stmt** ppStmt);
    void                   insert_canonical(int64_t hash, int64_t id, const std::string& canonical);
    void                   insert_query_event(const QueryEvent& qevent, int64_t can_id);
    void                   insert_canonical_args(int64_t event_id, const maxsimd::CanonicalArgs& args);
    void                   insert_rep_event(const RepEvent& qevent);
    std::string            select_canonical(int64_t can_id);
    maxsimd::CanonicalArgs select_canonical_args(int64_t event_id);

    struct SelectCanIdRes
    {
        bool    exists = false;
        int64_t can_id;
    };

    SelectCanIdRes select_can_id(int64_t hash);


    Access        m_access;
    fs::path      m_path;
    sqlite3*      m_pDb = nullptr;
    sqlite3_stmt* m_pCanonical_insert_stmt = nullptr;
    sqlite3_stmt* m_pQuery_event_insert_stmt = nullptr;
    sqlite3_stmt* m_pArg_insert_stmt = nullptr;
    sqlite3_stmt* m_pRep_event_insert_stmt = nullptr;
    sqlite3_stmt* m_pQuery_event_read_stmt = nullptr;
    bool          m_sort_by_start_time = false;
};
