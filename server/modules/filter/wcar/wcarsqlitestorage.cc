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

SqliteStorage::SqliteStorage(const fs::path& path, Access access)
    : m_access(access)
    , m_path(path)
{
    if (access == Access::READ_WRITE && fs::exists(m_path))
    {
        MXB_THROW(WcarError, "sqlite3 database '"
                  << m_path << "' already exists."
                  << " Appending to existing database is not allowed.");
    }

    int flags = (access == Access::READ_WRITE) ?
        SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE :
        SQLITE_OPEN_READONLY;

    if (sqlite3_open_v2(m_path.c_str(), &m_pDb, flags, nullptr) != SQLITE_OK)
    {
        std::string error_msg = "Could not create sqlite3 database '" + m_path.string() + '\'';
        if (m_pDb)
        {
            error_msg += " error: "s + sqlite3_errmsg(m_pDb);
        }

        MXB_THROW(WcarError, error_msg);
    }

    if (access == Access::READ_WRITE)
    {
        for (const auto& create : CREATE_TABLES_SQL)
        {
            sqlite_execute(create);
        }
    }
}

SqliteStorage::~SqliteStorage()
{
    sqlite3_close_v2(m_pDb);
}

void SqliteStorage::sqlite_execute(const std::string& sql)
{
    char* pError = nullptr;
    if (sqlite3_exec(m_pDb, sql.c_str(), 0, nullptr, &pError) != SQLITE_OK)
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
        pSelect_res->can_id = std::stol(ppColumn[0]);       // TODO throws, rethrow as WcarError, maybe.

        return 0;
    };

    auto sql = MAKE_STR("select can_id from canonical where hash = " << hash);
    SelectCanIdRes select_res;
    char* pError = nullptr;

    if (sqlite3_exec(m_pDb, sql.c_str(), select_can_id_cb, &select_res, &pError) != SQLITE_OK)
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
    if (m_pEvent_stmt != nullptr)
    {
        sqlite3_finalize(m_pEvent_stmt);
        m_pEvent_stmt = nullptr;
    }

    auto event_query = MAKE_STR("select event_id, can_id from event where event_id > " << m_last_event_read);

    if (sqlite3_prepare_v2(m_pDb, event_query.c_str(), -1, &m_pEvent_stmt, nullptr) != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed to prepare stmt '" << event_query << "' in database "
                                                        << m_path << "' error: " << sqlite3_errmsg(m_pDb));
    }

    return Storage::Iterator(this, next_event());
}

Storage::Iterator SqliteStorage::end() const
{
    return {nullptr, QueryEvent {}};
}

size_t SqliteStorage::num_unread() const
{
    return 42;      // TODO
}

std::string SqliteStorage::select_canonical(ssize_t can_id)
{

    auto select_can_id_cb = [](void* pData, int nColumns, char** ppColumn, char** ppNames){

        mxb_assert(nColumns == 1);

        std::string* pCanonical = static_cast<std::string*>(pData);
        *pCanonical = ppColumn[0];

        return 0;
    };

    auto sql = MAKE_STR("select canonical from canonical where can_id = " << can_id);
    std::string canonical;
    char* pError = nullptr;

    if (sqlite3_exec(m_pDb, sql.c_str(), select_can_id_cb, &canonical, &pError) != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed sqlite3 query in database '"
                  << m_path << "' error: " << (pError ? pError : "unknown")
                  << " sql '" << sql << '\'');
    }

    return canonical;
}

maxsimd::CanonicalArgs SqliteStorage::select_canonical_args(ssize_t event_id)
{

    auto select_can_args_cb = [](void* pData, int nColumns, char** ppColumn, char** ppNames){

        mxb_assert(nColumns == 2);

        maxsimd::CanonicalArgs* pArgs = static_cast<maxsimd::CanonicalArgs*>(pData);
        pArgs->emplace_back(std::stoi(ppColumn[0]), std::string(ppColumn[1]));      // TODO throws

        return 0;
    };

    auto sql = MAKE_STR("select pos, value from argument where event_id = " << event_id);

    maxsimd::CanonicalArgs canonical_args;
    char* pError = nullptr;

    if (sqlite3_exec(m_pDb, sql.c_str(), select_can_args_cb, &canonical_args, &pError)  != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed sqlite3 query in database '"
                  << m_path << "' error: " << (pError ? pError : "unknown")
                  << " sql '" << sql << '\'');
    }

    return canonical_args;
}

QueryEvent SqliteStorage::next_event()
{
    mxb_assert(m_pEvent_stmt != nullptr);
    auto rc = sqlite3_step(m_pEvent_stmt);

    if (rc == SQLITE_DONE)
    {
        sqlite3_finalize(m_pEvent_stmt);
        m_pEvent_stmt = nullptr;
        return QueryEvent {};
    }
    else if (rc != SQLITE_ROW)
    {
        MXB_THROW(WcarError, "sqlite3_step error: "
                  << sqlite3_errmsg(m_pDb)
                  << " database " << m_path
                  << " Note: add_query_event() cannot be called during iteration");
    }

    auto event_id = sqlite3_column_int64(m_pEvent_stmt, 0);
    auto can_id = sqlite3_column_int64(m_pEvent_stmt, 1);

    auto canonical = select_canonical(can_id);
    auto can_args = select_canonical_args(event_id);

    m_last_event_read = event_id;

    return QueryEvent{std::move(canonical), std::move(can_args), event_id};
}
