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

class SqliteStorage final : public Storage
{
public:
    SqliteStorage(const fs::path& path);
    ~SqliteStorage();

    void     add_query_event(QueryEvent&& qevent) override;
    Iterator begin() override;
    Iterator end() const override;
    size_t   num_unread() const override;

private:
    QueryEvent next_event(const QueryEvent& event) override;
    void       sqlite_execute(const std::string& sql);

    struct SelectCanIdRes
    {
        bool   exists = false;
        size_t can_id;
    };

    SelectCanIdRes select_can_id(size_t hash);

    fs::path m_path;
    sqlite3* m_pDb = nullptr;
};
