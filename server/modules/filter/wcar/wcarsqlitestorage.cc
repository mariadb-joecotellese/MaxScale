/*
 * Copyright (c) 2024 MariaDB plc
 *
 * This is UNPUBLISHED PROPRIETARY SOURCE CODE of MariaDB plc
 */
#include "wcarsqlitestorage.hh"
#include "wcarconfig.hh"
#include <maxbase/assert.hh>

using namespace std;

static const char SQL_CREATE_CANONICAL_TBL[] =
    "create table canonical ("
    "hash int primary key"
    ", can_id int"
    ", canonical text"
    ")";

static const char SQL_CREATE_CANONICAL_INDEX[] =
    "create index can_index on canonical(can_id)";

static const char SQL_CREATE_EVENT_TBL[] =
    "create table event ("
    "event_id int primary key"
    ", can_id int references canonical(can_id)"
    ")";

static const char SQL_CREATE_ARGUMENT_TBL[] =
    "create table argument ("
    "event_id int references event(event_id)"
    ", pos int"
    ", value text"
    ")";

static const char SQL_CREATE_ARGUMENT_INDEX[] =
    "create index arg_index on argument(event_id)";

static auto CREATE_TABLES_SQL =
{
    SQL_CREATE_CANONICAL_TBL,
    SQL_CREATE_CANONICAL_INDEX,
    SQL_CREATE_EVENT_TBL,
    SQL_CREATE_ARGUMENT_TBL,
    SQL_CREATE_ARGUMENT_INDEX
};

SqliteStorage::SqliteStorage(const fs::path& path)
    : m_path(path)
{
    if (fs::exists(m_path))
    {
        MXB_THROW(WcarError, "sqlite3 database '" << m_path << "' already exists");
    }

    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_CREATE;
    if (auto rv = sqlite3_open_v2(m_path.c_str(), &m_pDb, flags, nullptr); rv != SQLITE_OK)
    {
        std::string error_msg = "Could not create sqlite3 database '" + m_path.string() + '\'';
        if (m_pDb)
        {
            error_msg += " error: "s + sqlite3_errmsg(m_pDb);
        }

        MXB_THROW(WcarError, error_msg);
    }

    for (const auto& create : CREATE_TABLES_SQL)
    {
        sqlite_execute(create);
    }
}

SqliteStorage::~SqliteStorage()
{
    sqlite3_close_v2(m_pDb);
}

void SqliteStorage::sqlite_execute(const std::string& sql)
{
    char* pError = nullptr;
    if (auto rv = sqlite3_exec(m_pDb, sql.c_str(), 0, nullptr, &pError); rv != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed sqlite3 query in database '"
                  << m_path << "' error: " << (pError ? pError : "unknown")
                  << " sql '" << sql << '\'');
    }
}

SqliteStorage::SelectCanIdRes SqliteStorage::select_can_id(size_t hash)
{
    auto select_can_id_cb = [](void* pData, int nColumns, char** ppColumn, char** ppNames){
        mxb_assert(nColumns == 1);

        SqliteStorage::SelectCanIdRes* pSelect_res = static_cast<SqliteStorage::SelectCanIdRes*>(pData);
        pSelect_res->exists = true;
        pSelect_res->can_id = std::stoul(ppColumn[0]); // TODO throws, rethrow as WcarError, maybe.

        return 0;
    };

    auto sql = MAKE_STR("select can_id from canonical where hash = " << hash);
    SelectCanIdRes select_res;
    char* pError = nullptr;

    if (int rv = sqlite3_exec(m_pDb, sql.c_str(), select_can_id_cb, &select_res, &pError); rv != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed sqlite3 query in database '"
                  << m_path << "' error: " << (pError ? pError : "unknown")
                  << " sql '" << sql << '\'');
    }

    return select_res;
}

void SqliteStorage::add_query_event(QueryEvent&& qevent)
{
    size_t hash = std::hash<std::string> {}(qevent.canonical);
    size_t can_id;

    SelectCanIdRes select_res = select_can_id(hash);

    if (select_res.exists)
    {
        can_id = select_res.can_id;
    }
    else
    {
        can_id = next_can_id();
        auto insert_canonical = MAKE_STR("insert into canonical values("
                                         << hash << ", " << can_id << ", '" << qevent.canonical << "')");
        sqlite_execute(insert_canonical);
    }

    if (qevent.event_id == -1)
    {
        qevent.event_id = next_event_id();
    }

    auto insert_event = MAKE_STR("insert into event values( " << qevent.event_id << ',' << can_id << ')');

    sqlite_execute(insert_event);

    if (!qevent.canonical_args.empty())
    {
        std::ostringstream insert_args_os;
        insert_args_os << "insert into argument values ";

        auto first = true;
        for (const auto& arg : qevent.canonical_args)
        {
            if (!first)
            {
                insert_args_os << ',';
            }
            first = false;

            insert_args_os << '(' << qevent.event_id << ','
                           << arg.pos << ", '"
                           << arg.value << "')";
        }

        sqlite_execute(insert_args_os.str());
    }
}

Storage::Iterator SqliteStorage::begin()
{
    QueryEvent qe{"", maxsimd::CanonicalArgs{}, 0};
    return Storage::Iterator(this, next_event(std::move(qe)));
}

Storage::Iterator SqliteStorage::end() const
{
    return {nullptr, QueryEvent {}};
}

size_t SqliteStorage::num_unread() const
{
    return 42;      // TODO
}

QueryEvent SqliteStorage::next_event(const QueryEvent& event)
{
    // TODO
    return QueryEvent{};
}
