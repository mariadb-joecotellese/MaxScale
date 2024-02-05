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
    ", session_id int"
    ", flags int"
    ", start_time int"
    ", end_time int"
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

static const char SQL_CANONICAL_INSERT[] = "insert into canonical values(?, ?, ?)";
static const char SQL_EVENT_INSERT[] = "insert into event values(?, ?, ?, ?, ?, ?)";
static const char SQL_CANONICAL_ARGUMENT_INSERT[] = "insert into argument values(?, ?, ?)";

static const fs::path FILE_EXTENSION = "sqlite";

SqliteStorage::SqliteStorage(const fs::path& path, Access access)
    : m_access(access)
    , m_path(path)
{
    if (m_path.extension() != FILE_EXTENSION)
    {
        m_path.replace_extension(FILE_EXTENSION);
    }

    if (access == Access::READ_WRITE && fs::exists(m_path))
    {
        MXB_THROW(WcarError, "sqlite3 database '"
                  << m_path << "' already exists."
                  << " Appending to existing database is not allowed.");
    }

    int flags = (access == Access::READ_WRITE) ?
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE :
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

        sqlite_prepare(SQL_CANONICAL_INSERT, &m_pCanonical_insert_stmt);
        sqlite_prepare(SQL_EVENT_INSERT, &m_pEvent_insert_stmt);
        sqlite_prepare(SQL_CANONICAL_ARGUMENT_INSERT, &m_pArg_insert_stmt);
    }
}

SqliteStorage::~SqliteStorage()
{
    sqlite3_finalize(m_pCanonical_insert_stmt);
    sqlite3_finalize(m_pEvent_read_stmt);
    sqlite3_finalize(m_pArg_insert_stmt);
    sqlite3_close_v2(m_pDb);
}

void SqliteStorage::sqlite_prepare(const string& sql, sqlite3_stmt** ppStmt)
{
    if (sqlite3_prepare_v2(m_pDb, sql.c_str(), sql.length() + 1, ppStmt, nullptr) != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed to prepare stmt '" << sql << "' in database "
                                                        << m_path << "' error: " << sqlite3_errmsg(m_pDb));
    }
}

void SqliteStorage::insert_canonical(int64_t hash, int64_t id, const std::string& canonical)
{
    int idx = 0;
    sqlite3_bind_int64(m_pCanonical_insert_stmt, ++idx, hash);
    sqlite3_bind_int64(m_pCanonical_insert_stmt, ++idx, id);
    sqlite3_bind_text(m_pCanonical_insert_stmt, ++idx, canonical.c_str(), canonical.size() + 1, nullptr);

    if (sqlite3_step(m_pCanonical_insert_stmt) != SQLITE_DONE)
    {
        MXB_THROW(WcarError, "Failed to execute canonical insert prepared stmt in database "
                  << m_path << "' error: " << sqlite3_errmsg(m_pDb));
    }

    sqlite3_reset(m_pCanonical_insert_stmt);
}

void SqliteStorage::insert_event(const QueryEvent& qevent, int64_t can_id)
{
    int idx = 0;
    sqlite3_bind_int64(m_pEvent_insert_stmt, ++idx, qevent.event_id);
    sqlite3_bind_int64(m_pEvent_insert_stmt, ++idx, can_id);
    sqlite3_bind_int64(m_pEvent_insert_stmt, ++idx, qevent.session_id);
    sqlite3_bind_int64(m_pEvent_insert_stmt, ++idx, qevent.flags);

    static_assert(sizeof(mxb::Duration) == sizeof(int64_t));

    mxb::Duration start_time_dur = qevent.start_time.time_since_epoch();
    mxb::Duration end_time_dur = qevent.end_time.time_since_epoch();

    int64_t start_time_64 = *reinterpret_cast<const int64_t*>(&start_time_dur);
    int64_t end_time_64 = *reinterpret_cast<const int64_t*>(&end_time_dur);

    sqlite3_bind_int64(m_pEvent_insert_stmt, ++idx, start_time_64);
    sqlite3_bind_int64(m_pEvent_insert_stmt, ++idx, end_time_64);

    if (sqlite3_step(m_pEvent_insert_stmt) != SQLITE_DONE)
    {
        MXB_THROW(WcarError, "Failed to execute canonical insert prepared stmt in database "
                  << m_path << "' error: " << sqlite3_errmsg(m_pDb));
    }

    sqlite3_reset(m_pEvent_insert_stmt);
}

void SqliteStorage::insert_canonical_args(int64_t event_id, const maxsimd::CanonicalArgs& args)
{
    for (const auto& arg : args)
    {
        int idx = 0;
        sqlite3_bind_int64(m_pArg_insert_stmt, ++idx, event_id);
        sqlite3_bind_int64(m_pArg_insert_stmt, ++idx, arg.pos);
        sqlite3_bind_text(m_pArg_insert_stmt, ++idx, arg.value.c_str(), arg.value.size() + 1, nullptr);
        if (sqlite3_step(m_pArg_insert_stmt) != SQLITE_DONE)
        {
            MXB_THROW(WcarError, "Failed to execute canonical insert prepared stmt in database "
                      << m_path << "' error: " << sqlite3_errmsg(m_pDb));
        }

        sqlite3_reset(m_pArg_insert_stmt);
    }
}

void SqliteStorage::sqlite_execute(const std::string& sql)
{
    char* pError = nullptr;
    if (sqlite3_exec(m_pDb, sql.c_str(), nullptr, nullptr, &pError) != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed sqlite3 query in database '"
                  << m_path << "' error: " << (pError ? pError : "unknown")
                  << " sql '" << sql << '\'');
    }
}

SqliteStorage::SelectCanIdRes SqliteStorage::select_can_id(int64_t hash)
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
    int64_t hash {static_cast<int64_t>(std::hash<std::string> {}(*qevent.sCanonical))};
    int64_t can_id;

    SelectCanIdRes select_res = select_can_id(hash);

    if (select_res.exists)
    {
        can_id = select_res.can_id;
    }
    else
    {
        can_id = next_can_id();
        insert_canonical(hash, can_id, *qevent.sCanonical);
    }

    insert_event(qevent, can_id);

    if (!qevent.canonical_args.empty())
    {
        insert_canonical_args(qevent.event_id, std::move(qevent.canonical_args));
    }
}

void SqliteStorage::add_query_event(std::vector<QueryEvent>& qevents)
{
    sqlite_execute("begin transaction");

    for (auto& event : qevents)
    {
        add_query_event(std::move(event));
    }

    sqlite_execute("commit transaction");
}

Storage::Iterator SqliteStorage::begin()
{
    if (m_pEvent_read_stmt != nullptr)
    {
        sqlite3_finalize(m_pEvent_read_stmt);
        m_pEvent_read_stmt = nullptr;
    }

    std::string event_query = "select event_id, can_id, session_id, flags, start_time, end_time from event ";
    if (m_sort_by_start_time)
    {
        event_query += "order by start_time";
    }
    else
    {
        event_query += "order by event_id";
    }

    sqlite_prepare(event_query, &m_pEvent_read_stmt);

    return Storage::Iterator(this, next_event());
}

Storage::Iterator SqliteStorage::end() const
{
    return {nullptr, QueryEvent {}};
}

int64_t SqliteStorage::num_unread() const
{
    return 42;      // TODO
}

std::string SqliteStorage::select_canonical(int64_t can_id)
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

maxsimd::CanonicalArgs SqliteStorage::select_canonical_args(int64_t event_id)
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

    if (sqlite3_exec(m_pDb, sql.c_str(), select_can_args_cb, &canonical_args, &pError) != SQLITE_OK)
    {
        MXB_THROW(WcarError, "Failed sqlite3 query in database '"
                  << m_path << "' error: " << (pError ? pError : "unknown")
                  << " sql '" << sql << '\'');
    }

    return canonical_args;
}

QueryEvent SqliteStorage::next_event()
{
    mxb_assert(m_pEvent_read_stmt != nullptr);
    auto rc = sqlite3_step(m_pEvent_read_stmt);

    if (rc == SQLITE_DONE)
    {
        sqlite3_finalize(m_pEvent_read_stmt);
        m_pEvent_read_stmt = nullptr;
        return QueryEvent {};
    }
    else if (rc != SQLITE_ROW)
    {
        MXB_THROW(WcarError, "sqlite3_step error: "
                  << sqlite3_errmsg(m_pDb)
                  << " database " << m_path
                  << " Note: add_query_event() cannot be called during iteration");
    }

    int idx = -1;
    auto event_id = sqlite3_column_int64(m_pEvent_read_stmt, ++idx);
    auto can_id = sqlite3_column_int64(m_pEvent_read_stmt, ++idx);
    auto session_id = sqlite3_column_int64(m_pEvent_read_stmt, ++idx);
    auto flags = sqlite3_column_int64(m_pEvent_read_stmt, ++idx);
    auto start_time_64 = sqlite3_column_int64(m_pEvent_read_stmt, ++idx);
    auto end_time_64 = sqlite3_column_int64(m_pEvent_read_stmt, ++idx);

    auto pS = reinterpret_cast<mxb::TimePoint::rep*>(&start_time_64);
    auto pE = reinterpret_cast<mxb::TimePoint::rep*>(&end_time_64);
    mxb::TimePoint start_time{mxb::Duration{*pS}};
    mxb::TimePoint end_time{mxb::Duration{*pE}};

    auto canonical = select_canonical(can_id);
    auto can_args = select_canonical_args(event_id);

    return QueryEvent{make_shared<std::string>(std::move(canonical)),
                      std::move(can_args),
                      session_id,
                      uint64_t(flags),
                      start_time,
                      end_time,
                      event_id};
}
