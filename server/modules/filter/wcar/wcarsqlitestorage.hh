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

    void     add_query_event(QueryEvent&& qevent) override;
    Iterator begin() override;
    Iterator end() const override;
    size_t   num_unread() const override;

private:
    QueryEvent next_event() override;
    void       sqlite_execute(const std::string& sql);

    struct SelectCanIdRes
    {
        bool   exists = false;
        size_t can_id;
    };

    SelectCanIdRes select_can_id(size_t hash);
    std::string    select_canonical(ssize_t can_id);

    maxsimd::CanonicalArgs select_canonical_args(ssize_t event_id);

    Access        m_access;
    fs::path      m_path;
    sqlite3*      m_pDb = nullptr;
    ssize_t       m_last_event_read = -1;
    sqlite3_stmt* m_pEvent_stmt = nullptr;
};
