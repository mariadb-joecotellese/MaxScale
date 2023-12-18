/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#pragma once

#include "wcarstorage.hh"
#include <filesystem>
#include <sqlite3.h>

namespace fs = std::filesystem;

enum class Access
{
    READ_ONLY,
    READ_WRITE
};

class SqliteStorage final : public Storage
{
public:
    SqliteStorage(const fs::path& path, Access access = Access::READ_WRITE);
    ~SqliteStorage();

    void add_query_event(QueryEvent&& qevent) override;
    void add_query_event(std::vector<QueryEvent>& qevents) override;

    Iterator begin() override;
    Iterator end() const override;
    int64_t  num_unread() const override;

private:
    QueryEvent next_event() override;

    void                   sqlite_execute(const std::string& sql);
    void                   sqlite_prepare(const std::string& sql, sqlite3_stmt** ppStmt);
    void                   insert_canonical(int64_t hash, int64_t id, const std::string& canonical);
    void                   insert_event(const QueryEvent& qevent, int64_t can_id);
    void                   insert_canonical_args(int64_t event_id, const maxsimd::CanonicalArgs& args);
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
    int64_t       m_last_event_read = -1;
    sqlite3_stmt* m_pCanonical_insert_stmt = nullptr;
    sqlite3_stmt* m_pEvent_insert_stmt = nullptr;
    sqlite3_stmt* m_pArg_insert_stmt = nullptr;
    sqlite3_stmt* m_pEvent_read_stmt = nullptr;
};
