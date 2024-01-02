/**
 * @section LICENCE
 *
 * This file is distributed as part of the MariaDB Corporation MaxScale. It is
 * free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the
 * Free Software Foundation, version 2.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *
 * Copyright (c) MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * @file
 *
 */

// The server sources do not use override.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsuggest-override"
#define EMBEDDED_LIBRARY
#define MYSQL_YACC
#define MYSQL_LEX012
#define MYSQL_SERVER
#define DBUG_OFF
#if defined (MYSQL_CLIENT)
#undef MYSQL_CLIENT
#endif
#include <my_global.h>
#include <my_config.h>
#include <mysql.h>
#include <my_sys.h>
#include <my_dbug.h>
#include <my_base.h>
#if defined(ER_QUERY_EXCEEDED_ROWS_EXAMINED_LIMIT)
#undef ER_QUERY_EXCEEDED_ROWS_EXAMINED_LIMIT
#endif
// We need to get access to Item::str_value, which is protected. So we cheat.
#define protected public
#include <item.h>
#undef protected
#include <sql_list.h>
#include <mysqld_error.h>
#include <sql_class.h>
#include <sql_lex.h>
#include <embedded_priv.h>
#include <sql_lex.h>
#include <sql_parse.h>
#include <errmsg.h>
#include <client_settings.h>
// In client_settings.h mysql_server_init and mysql_server_end are defined to
// mysql_client_plugin_init and mysql_client_plugin_deinit respectively.
// Those must be undefined, so that we here really call mysql_server_[init|end].
#undef mysql_server_init
#undef mysql_server_end
#include <set_var.h>
#include <strfunc.h>
#include <item_func.h>
#undef UNKNOWN
#pragma GCC diagnostic pop
#include <pthread.h>

#undef PCRE2_CODE_UNIT_WIDTH
#define json_type mxs_json_type
#include <maxbase/assert.hh>
#include <maxbase/string.hh>
#include <maxsimd/canonical.hh>
#include <maxsimd/multistmt.hh>
#include <maxscale/log.hh>
#include <maxscale/parser.hh>
#include <maxscale/protocol/mariadb/mariadbparser.hh>
#include <maxscale/protocol/mariadb/mysql.hh>
#include <maxscale/protocol/mariadb/trxboundaryparser.hh>
#include <maxscale/paths.hh>
#include <maxscale/modinfo.hh>
#include <maxscale/utils.hh>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <set>

using namespace std;
using mxb::sv_case_eq;
using mxs::Parser;
using mxs::ParserPlugin;

#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 2
#define CTE_SUPPORTED
#define WF_SUPPORTED
#endif

namespace
{

enum pp_result_t
{
    PP_RESULT_OK,
    PP_RESULT_ERROR
};

}

extern "C"
{

my_bool _db_my_assert(const char *file, int line, const char *msg)
{
    return true;
}

}

#if defined (CTE_SUPPORTED)
// We need to be able to access private data of With_element that has no
// public access methods. So, we use this very questionable method of
// making the private parts public. Ok, as pp_myselembedded is only
// used for verifying the output of pp_sqlite.
#define private public
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsuggest-override"
#include <sql_cte.h>
#pragma GCC diagnostic pop
#undef private
#endif

/**
 * Defines what a particular name should be mapped to.
 */
typedef struct name_mapping
{
    const char* from;
    const char* to;
} NAME_MAPPING;

static NAME_MAPPING function_name_mappings_default[] =
{
    {"octet_length", "length"},
    {NULL, NULL}
};

static NAME_MAPPING function_name_mappings_oracle[] =
{
    {"octet_length",           "lengthb"},
    {"decode_oracle",          "decode"},
    {"char_length",            "length"},
    {"concat_operator_oracle", "concat"},
    {"case",                   "decode"},
    {NULL,                     NULL    }
};

static const char* map_function_name(NAME_MAPPING* function_name_mappings, const char* from)
{
    NAME_MAPPING* map = function_name_mappings;
    const char* to = NULL;

    while (!to && map->from)
    {
        if (strcasecmp(from, map->from) == 0)
        {
            to = map->to;
        }
        else
        {
            ++map;
        }
    }

    return to ? to : from;
}

#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 5
#define mxs_my_strdup(a, f) my_strdup(PSI_NOT_INSTRUMENTED, a, f)
#define mxs_strptr(a) a.str
#else
#define mxs_my_strdup(a, f) my_strdup(a, f)
#define mxs_strptr(a) a
#endif

#define MYSQL_COM_QUERY_HEADER_SIZE 5   /*< 3 bytes size, 1 sequence, 1 command */
#define MAX_QUERYBUF_SIZE           2048
struct TableName
{
    std::string db;
    std::string table;

    operator Parser::TableName() const
    {
        return Parser::TableName (this->db, this->table);
    }
};

class parsing_info_t : public GWBUF::ProtocolInfo
{
public:
    parsing_info_t(const parsing_info_t&) = delete;
    parsing_info_t& operator=(const parsing_info_t&) = delete;

    parsing_info_t(const Parser::Helper& helper, const GWBUF* querybuf);
    ~parsing_info_t();

    std::string_view get_string_view(const char* zContext, const char* zNeedle)
    {
        std::string_view rv;

        const char* pMatch = nullptr;
        size_t n = strlen(zNeedle);

        auto i = this->canonical.find(zNeedle);

        if (i != std::string::npos)
        {
            pMatch = &this->canonical[i];
        }
        else
        {
            // Ok, let's try case-insensitively.
            pMatch = strcasestr(const_cast<char*>(this->canonical.c_str()), zNeedle);

            if (!pMatch)
            {
                complain_about_missing(zContext, zNeedle);

                std::string_view needle(zNeedle);

                for (const auto& scratch : this->scratchs)
                {
                    if (sv_case_eq(std::string_view(scratch.data(), scratch.size()), needle))
                    {
                        pMatch = scratch.data();
                        break;
                    }
                }

                if (!pMatch)
                {
                    this->scratchs.emplace_back(needle.begin(), needle.end());

                    const auto& scratch = this->scratchs.back();

                    pMatch = scratch.data();
                }
            }
        }

        rv = std::string_view(pMatch, n);

        return rv;
    }

    void populate_field_info(Parser::FieldInfo& info,
                             const char* zDatabase, const char* zTable, const char* zColumn)
    {
        if (zDatabase)
        {
            info.database = get_string_view("database", zDatabase);
        }

        if (zTable)
        {
            info.table = get_string_view("table", zTable);
        }

        mxb_assert(zColumn);
        info.column = get_string_view("column", zColumn);
    }

    void complain_about_missing(const char* zWhat, const char* zKey)
    {
#if defined(SS_DEBUG)
        if (strcmp(zKey, "<>") != 0                        // != => <>
            && strcasecmp(zKey, "cast") != 0               // convert() => cast()
            && strcasecmp(zKey, "current_timestamp") != 0  // now() => current_timestamp()
            && strcasecmp(zKey, "ifnull") != 0             // NVL() => ifnull()
            && strcasecmp(zKey, "isnull") != 0             // is null => isnull()
            && strcasecmp(zKey, "isnotnull") != 0          // is not null => isnotnull()
            && strcasecmp(zKey, "date_add_interval") != 0) // Various date functions
        {
            MXB_WARNING("The %s '%s' is not found in the canonical statement '%s' created from "
                        "the statement '%s'.",
                        zWhat, zKey, this->canonical.c_str(), this->pi_query_plain_str.c_str());
        }
#endif
    }

    size_t size() const override
    {
        return sizeof(*this); // TODO: calculate better if needed. Not really relevant.
    }

    MYSQL*                pi_handle { nullptr }; /*< parsing info object pointer */
    std::string           pi_query_plain_str;    /*< query as plain string */
    Parser::FieldInfo*    field_infos { nullptr };
    size_t                field_infos_len { 0 };
    size_t                field_infos_capacity { 0 };
    Parser::FunctionInfo* function_infos { 0 };
    size_t                function_infos_len { 0 };
    size_t                function_infos_capacity { 0 };
    GWBUF*                preparable_stmt { 0 };
    Parser::Result        result { Parser::Result::INVALID };
    int32_t               type_mask { 0 };
    NAME_MAPPING*         function_name_mappings { 0 };
    string                created_table_name;
    vector<string>        database_names;
    vector<TableName>     table_names;
    string                prepare_name;
    string                canonical;
    vector<vector<char>>  scratchs;
};

#define QTYPE_LESS_RESTRICTIVE_THAN_WRITE(t) (t < mxs::sql::TYPE_WRITE ? true : false)

static THD*          get_or_create_thd_for_parsing(MYSQL* mysql, const char* query_str);
static unsigned long set_client_flags(MYSQL* mysql);
static bool          create_parse_tree(THD* thd);
static uint32_t      resolve_query_type(parsing_info_t*, THD* thd);
static bool          skygw_stmt_causes_implicit_commit(LEX* lex, int* autocommit_stmt);
static int           is_autocommit_stmt(LEX* lex);

static std::unique_ptr<parsing_info_t> parsing_info_init(const Parser::Helper& helper, const GWBUF* querybuf);

static TABLE_LIST* skygw_get_affected_tables(void* lexptr);
static bool        ensure_query_is_parsed(const Parser::Helper& helper, const GWBUF* query);
static bool        parse_query(const Parser::Helper& helper, const GWBUF* querybuf);
static bool        query_is_parsed(const GWBUF* buf);
int32_t            pp_mysql_get_field_info(const Parser::Helper& helper,
                                           const GWBUF* buf,
                                           const Parser::FieldInfo** infos,
                                           uint32_t* n_infos);

#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 3
inline void get_string_and_length(const LEX_CSTRING& ls, const char** s, size_t* length)
{
    *s = ls.str;
    *length = ls.length;
}
#else
inline void get_string_and_length(const char* cs, const char** s, size_t* length)
{
    *s = cs;
    *length = cs ? strlen(cs) : 0;
}
#endif

#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 4
#define MARIADB_10_4

class Expose_Lex_prepared_stmt
{
    Lex_ident_sys m_name; // Statement name (in all queries)
    Item *m_code;         // PREPARE or EXECUTE IMMEDIATE source expression
    List<Item> m_params;  // List of parameters for EXECUTE [IMMEDIATE]

public:
    static Item* code(Lex_prepared_stmt* lps)
    {
        return reinterpret_cast<Expose_Lex_prepared_stmt*>(lps)->m_code;
    }
};

static_assert(sizeof(Lex_prepared_stmt) == sizeof(Expose_Lex_prepared_stmt),
              "Update Expose_Lex_prepared_stmt, some member variable(s) is(are) missing.");

#define QCME_STRING(name, s) LEX_CSTRING name { s, strlen(s) }

inline int qcme_thd_set_db(THD* thd, LEX_CSTRING& s)
{
    return thd->set_db(&s);
}

inline bool qcme_item_is_int(Item* item)
{
    return
        item->type() == Item::CONST_ITEM
        && static_cast<Item_basic_value*>(item)->const_ptr_longlong();
}

inline bool qcme_item_is_string(Item* item)
{
    return
        item->type() == Item::CONST_ITEM
        && static_cast<Item_basic_value*>(item)->const_ptr_string();
}

inline const char* qcme_string_get(const LEX_CSTRING& s)
{
    return s.str && s.length ? s.str : (s.str != nullptr && *s.str == 0 ? s.str : nullptr);
}

#define PP_CF_IMPLICIT_COMMIT_BEGIN CF_IMPLICIT_COMMIT_BEGIN
#define PP_CF_IMPLICIT_COMMIT_END   CF_IMPLICIT_COMMIT_END

inline SELECT_LEX* qcme_get_first_select_lex(LEX* lex)
{
    return lex->first_select_lex();
}

inline const LEX_CSTRING& qcme_get_prepared_stmt_name(LEX* lex)
{
    Lex_prepared_stmt& prepared_stmt = lex->prepared_stmt;
    return prepared_stmt.name();
}

inline Item* qcme_get_prepared_stmt_code(LEX* lex)
{
    return Expose_Lex_prepared_stmt::code(&lex->prepared_stmt);
}

extern "C"
{

void _db_flush_(void)
{
}

}

#else

#define QCME_STRING(name, s) const char* name = s

inline int qcme_thd_set_db(THD* thd, const char* s)
{
    return thd->set_db(s, strlen(s));
}

inline bool qcme_item_is_int(Item* item)
{
    return item->type() == Item::INT_ITEM;
}

inline bool qcme_item_is_string(Item* item)
{
    return item->type() == Item::STRING_ITEM;
}

inline const char* qcme_string_get(const char* s)
{
    return s;
}

#define PP_CF_IMPLICIT_COMMIT_BEGIN CF_IMPLICT_COMMIT_BEGIN
#define PP_CF_IMPLICIT_COMMIT_END   CF_IMPLICIT_COMMIT_END

inline SELECT_LEX* qcme_get_first_select_lex(LEX* lex)
{
    return &lex->select_lex;
}

#if MYSQL_VERSION_MINOR >= 3

const LEX_CSTRING& qcme_get_prepared_stmt_name(LEX* lex)
{
    return lex->prepared_stmt_name;
}

inline Item* qcme_get_prepared_stmt_code(LEX* lex)
{
    return lex->prepared_stmt_code;
}

#else

const LEX_STRING& qcme_get_prepared_stmt_name(LEX* lex)
{
    return lex->prepared_stmt_name;
}

#endif

#endif


static struct
{
    Parser::SqlMode sql_mode;
    pthread_mutex_t sql_mode_mutex;
    NAME_MAPPING*   function_name_mappings;
} this_unit =
{
    Parser::SqlMode::DEFAULT,
    PTHREAD_MUTEX_INITIALIZER,
    function_name_mappings_default
};

static thread_local struct
{
    Parser::SqlMode          sql_mode { Parser::SqlMode::DEFAULT };
    uint32_t                 options { 0 };
    NAME_MAPPING*            function_name_mappings { function_name_mappings_default };
    uint64_t                 version { 0 };
} this_thread;


parsing_info_t::parsing_info_t(const Parser::Helper& helper, const GWBUF* querybuf)
    : pi_query_plain_str(helper.get_sql(*querybuf))
    , canonical(pi_query_plain_str)
{
    maxsimd::get_canonical(&this->canonical);

    MYSQL* mysql = mysql_init(NULL);
    mxb_assert(mysql);

    /** Set methods and authentication to mysql */
    mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "libmysqld_skygw");
    mysql_options(mysql, MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);

    const char* user = "skygw";
    const char* db = "skygw";

    mysql->methods = &embedded_methods;
    mysql->user = mxs_my_strdup(user, MYF(0));
    mysql->db = mxs_my_strdup(db, MYF(0));
    mysql->passwd = NULL;

    /** Set handle and free function to parsing info struct */
    this->pi_handle = mysql;
    mxb_assert(::this_thread.function_name_mappings);
    this->function_name_mappings = ::this_thread.function_name_mappings;
}

parsing_info_t::~parsing_info_t()
{
    MYSQL* mysql = this->pi_handle;

    if (mysql->thd != NULL)
    {
        auto* thd = (THD*) mysql->thd;
        thd->end_statement();
        thd->cleanup_after_query();
#if MYSQL_VERSION_MAJOR == 10 && MYSQL_VERSION_MINOR < 7
        (*mysql->methods->free_embedded_thd)(mysql);
#endif
        mysql->thd = NULL;
    }

    mysql_close(mysql);

    free(this->field_infos);

    for (size_t i = 0; i < this->function_infos_len; ++i)
    {
        Parser::FunctionInfo& fi = this->function_infos[i];

        for (size_t j = 0; j < fi.n_fields; ++j)
        {
            Parser::FieldInfo& field = fi.fields[j];
        }
        free(fi.fields);
    }
    free(this->function_infos);

    gwbuf_free(this->preparable_stmt);
}


/**
 * Ensures that the query is parsed. If it is not already parsed, it
 * will be parsed.
 *
 * @return true if the query is parsed, false otherwise.
 */
bool ensure_query_is_parsed(const Parser::Helper& helper, const GWBUF* query)
{
    bool parsed = query_is_parsed(query);

    if (!parsed)
    {
        // Instead of modifying global_system_variables, from which
        // thd->variables.sql_mode will be initialied, we should modify
        // thd->variables.sql_mode _after_ it has been created and
        // initialized.
        //
        // However, for whatever reason, the offset of that variable is
        // different when accessed from within libmysqld and pp_mysqlembedded,
        // so we will not modify the right variable even if it appears we do.
        //
        // So, for the time being we modify global_system_variables.sql_mode and
        // serialize the parsing. That's ok, since pp_mysqlembedded is only
        // used for verifying the behaviour of pp_sqlite.

        MXB_AT_DEBUG(int rv);

        MXB_AT_DEBUG(rv = ) pthread_mutex_lock(&this_unit.sql_mode_mutex);
        mxb_assert(rv == 0);

        if (::this_thread.sql_mode == Parser::SqlMode::ORACLE)
        {
            global_system_variables.sql_mode |= MODE_ORACLE;
        }
        else
        {
            global_system_variables.sql_mode &= ~MODE_ORACLE;
        }

        parsed = parse_query(helper, query);

        MXB_AT_DEBUG(rv = ) pthread_mutex_unlock(&this_unit.sql_mode_mutex);
        mxb_assert(rv == 0);

        if (!parsed)
        {
            MXB_ERROR("Unable to parse query, out of resources?");
        }
    }

    return parsed;
}

int32_t pp_mysql_parse(const Parser::Helper& helper,
                       const GWBUF* querybuf, uint32_t collect, Parser::Result* result)
{
    bool parsed = ensure_query_is_parsed(helper, querybuf);

    // Since the query is parsed using the same parser - subject to version
    // differences between the embedded library and the server - either the
    // query is valid and hence correctly parsed, or the query is invalid in
    // which case the server will also consider it invalid and reject it. So,
    // it's always ok to claim it has been parsed.

    if (parsed)
    {
        auto pi = static_cast<parsing_info_t*>(querybuf->get_protocol_info().get());
        mxb_assert(pi);
        *result = pi->result;
    }
    else
    {
        *result = Parser::Result::INVALID;
    }

    return PP_RESULT_OK;
}

int32_t pp_mysql_get_type_mask(const Parser::Helper& helper,
                               const GWBUF* querybuf, uint32_t* type_mask)
{
    int32_t rv = PP_RESULT_OK;

    *type_mask = mxs::sql::TYPE_UNKNOWN;
    MYSQL* mysql;
    bool succp;

    mxb_assert_message(querybuf != NULL, ("querybuf is NULL"));

    if (querybuf == NULL)
    {
        succp = false;
        goto retblock;
    }

    succp = ensure_query_is_parsed(helper, querybuf);

    /** Read thd pointer and resolve the query type with it. */
    if (succp)
    {
        auto pi = static_cast<parsing_info_t*>(querybuf->get_protocol_info().get());

        if (pi != NULL)
        {
            mysql = (MYSQL*) pi->pi_handle;

            /** Find out the query type */
            if (mysql != NULL)
            {
                *type_mask = resolve_query_type(pi, (THD*) mysql->thd);
#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 3
                // If in 10.3 mode we need to ensure that sequence related functions
                // are taken into account. That we can ensure by querying for the fields.
                const Parser::FieldInfo* field_infos;
                uint32_t n_field_infos;

                rv = pp_mysql_get_field_info(helper, querybuf, &field_infos, &n_field_infos);

                if (rv == PP_RESULT_OK)
                {
                    *type_mask |= pi->type_mask;
                }
#endif
            }
        }
    }

retblock:
    return rv;
}

/**
 * Create parsing info and try to parse the query included in the query buffer.
 * Store pointer to created parse_tree_t object to buffer.
 *
 * @param querybuf buffer including the query and possibly the parsing information
 *
 * @return true if succeed, false otherwise
 */
static bool parse_query(const Parser::Helper& helper, const GWBUF* querybuf)
{
    bool succp;
    THD* thd;
    uint8_t* data;
    size_t len;
    char* query_str = NULL;

    /** Do not parse without releasing previous parse info first */
    mxb_assert(!query_is_parsed(querybuf));

    if (querybuf == NULL || query_is_parsed(querybuf))
    {
        MXB_ERROR("Query is NULL (%p) or query is already parsed.", querybuf);
        return false;
    }

    /** Create parsing info */
    std::unique_ptr<parsing_info_t> pi = parsing_info_init(helper, querybuf);

    /** Get one or create new THD object to be use in parsing */
    thd = get_or_create_thd_for_parsing(pi->pi_handle, pi->pi_query_plain_str.c_str());
    mxb_assert(thd);

    /**
     * Create parse_tree inside thd.
     * thd and lex are readable even if creating parse tree fails.
     */
    if (create_parse_tree(thd))
    {
        pi->result = Parser::Result::PARSED;
    }

    if (pi && pi->type_mask & (mxs::sql::TYPE_ENABLE_AUTOCOMMIT | mxs::sql::TYPE_DISABLE_AUTOCOMMIT))
    {
        pi->set_cacheable(false);
    }

    /** Add complete parsing info struct to the query buffer */
    const_cast<GWBUF*>(querybuf)->set_protocol_info(std::move(pi));

    // By calling pp_mysql_get_field_info() now, the result will be
    // Parser::Result::PARTIALLY_PARSED, if some field is not found in the
    // canonical string.
    const Parser::FieldInfo* infos;
    uint32_t n_infos;
    pp_mysql_get_field_info(helper, querybuf, &infos, &n_infos);

    return true;
}

/**
 * If buffer has non-NULL gwbuf_parsing_info it is parsed and it has parsing
 * information included.
 *
 * @param buf buffer being examined
 *
 * @return true or false
 */
static bool query_is_parsed(const GWBUF* buf)
{
    return buf != NULL && buf->get_protocol_info().get() != nullptr;
}

/**
 * Create a thread context, thd, init embedded server, connect to it, and allocate
 * query to thd.
 *
 * Parameters:
 * @param mysql         Database handle
 *
 * @param query_str     Query in plain txt string
 *
 * @return Thread context pointer
 *
 */
static THD* get_or_create_thd_for_parsing(MYSQL* mysql, const char* query_str)
{
    THD* thd = NULL;
    unsigned long client_flags;
    char* db = mysql->options.db;
    bool failp = FALSE;
    size_t query_len;

    mxb_assert_message(mysql != NULL, ("mysql is NULL"));
    mxb_assert_message(query_str != NULL, ("query_str is NULL"));

    query_len = strlen(query_str);
    client_flags = set_client_flags(mysql);

    /** Get THD.
     * NOTE: Instead of creating new every time, THD instance could
     * be get from a pool of them.
     */
    thd = (THD*) create_embedded_thd(client_flags);

    if (thd == NULL)
    {
        MXB_ERROR("Failed to create thread context for parsing.");
        goto return_thd;
    }

    mysql->thd = thd;
    init_embedded_mysql(mysql, client_flags);
    failp = check_embedded_connection(mysql, db);

    if (failp)
    {
        MXB_ERROR("Call to check_embedded_connection failed.");
        goto return_err_with_thd;
    }

    thd->clear_data_list();

    /** Check that we are calling the client functions in right order */
    if (mysql->status != MYSQL_STATUS_READY)
    {
        set_mysql_error(mysql, CR_COMMANDS_OUT_OF_SYNC, unknown_sqlstate);
        MXB_ERROR("Invalid status %d in embedded server.",
                  mysql->status);
        goto return_err_with_thd;
    }

    /** Clear result variables */
    thd->current_stmt = NULL;
    thd->store_globals();
    /**
     * We have to call free_old_query before we start to fill mysql->fields
     * for new query. In the case of embedded server we collect field data
     * during query execution (not during data retrieval as it is in remote
     * client). So we have to call free_old_query here
     */
    free_old_query(mysql);
    thd->extra_length = query_len;
    thd->extra_data = (char*)query_str;
    alloc_query(thd, query_str, query_len);
    goto return_thd;

return_err_with_thd:
#if MYSQL_VERSION_MAJOR == 10 && MYSQL_VERSION_MINOR < 7
    (*mysql->methods->free_embedded_thd)(mysql);
#endif
    thd = 0;
    mysql->thd = 0;
return_thd:
    return thd;
}

/**
 * @node  Set client flags. This is copied from libmysqld.c:mysql_real_connect
 *
 * Parameters:
 * @param mysql - <usage>
 *          <description>
 *
 * @return
 *
 *
 * @details (write detailed description here)
 *
 */
static unsigned long set_client_flags(MYSQL* mysql)
{
    unsigned long f = 0;

    f |= mysql->options.client_flag;

    /* Send client information for access check */
    f |= CLIENT_CAPABILITIES;

    if (f & CLIENT_MULTI_STATEMENTS)
    {
        f |= CLIENT_MULTI_RESULTS;
    }

    /**
     * No compression in embedded as we don't send any data,
     * and no pluggable auth, as we cannot do a client-server dialog
     */
    f &= ~(CLIENT_COMPRESS | CLIENT_PLUGIN_AUTH);

    if (mysql->options.db != NULL)
    {
        f |= CLIENT_CONNECT_WITH_DB;
    }

    return f;
}

static bool create_parse_tree(THD* thd)
{
    Parser_state parser_state;
    bool failp = FALSE;

    QCME_STRING(virtual_db, "skygw_virtual");

    if (parser_state.init(thd, thd->query(), thd->query_length()))
    {
        failp = TRUE;
        goto return_here;
    }

    thd->reset_for_next_command();

    /**
     * Set some database to thd so that parsing won't fail because of
     * missing database. Then parse.
     */
    failp = qcme_thd_set_db(thd, virtual_db);

    if (failp)
    {
        MXB_ERROR("Failed to set database in thread context.");
    }

    failp = parse_sql(thd, &parser_state, NULL);

return_here:
    return !failp;
}

/**
 * Sniff whether the statement is
 *
 *    SET ROLE ...
 *    SET NAMES ...
 *    SET PASSWORD ...
 *    SET CHARACTER ...
 *
 * Depending on what kind of SET statement it is, the parser of the embedded
 * library creates instances of set_var_user, set_var, set_var_password,
 * set_var_role, etc. that all are derived from set_var_base. However, there
 * is no type-information available in set_var_base, which is the type of the
 * instances when accessed from the lexer. Consequently, we cannot know what
 * kind of statment it is based on that, only whether it is a system variable
 * or not.
 *
 * Consequently, we just look at the string and deduce whether it is a
 * set [ROLE|NAMES|PASSWORD|CHARACTER] statement.
 */
enum set_type_t
{
    SET_TYPE_CHARACTER,
    SET_TYPE_NAMES,
    SET_TYPE_PASSWORD,
    SET_TYPE_ROLE,
    SET_TYPE_DEFAULT_ROLE,
    SET_TYPE_TRANSACTION,
    SET_TYPE_UNKNOWN,
};

set_type_t get_set_type2(const char* s)
{
    set_type_t rv = SET_TYPE_UNKNOWN;

    while (isspace(*s))
    {
        ++s;
    }

    const char* token = s;

    while (!isspace(*s) && (*s != 0) && (*s != '='))
    {
        ++s;
    }

    if (s - token == 4)     // Might be "role"
    {
        if (strncasecmp(token, "role", 4) == 0)
        {
            // YES it was!
            rv = SET_TYPE_ROLE;
        }
    }
    else if (s - token == 5)    // Might be "names"
    {
        if (strncasecmp(token, "names", 5) == 0)
        {
            // YES it was!
            rv = SET_TYPE_NAMES;
        }
    }
    else if (s - token == 6)    // Might be "global"
    {
        if (strncasecmp(token, "global", 6) == 0)
        {
            rv = get_set_type2(s);
        }
    }
    else if (s - token == 7)    // Might be "default" || "session"
    {
        if (strncasecmp(token, "default", 7) == 0)
        {
            // YES it was!
            while (isspace(*s))
            {
                ++s;
            }

            token = s;

            while (!isspace(*s) && (*s != 0) && (*s != '='))
            {
                ++s;
            }

            if (s - token == 4) // Might be "role"
            {
                if (strncasecmp(token, "role", 4) == 0)
                {
                    rv = SET_TYPE_DEFAULT_ROLE;
                }
            }
        }
        else if (strncasecmp(token, "session", 7) == 0)
        {
            rv = get_set_type2(s);
        }
    }
    else if (s - token == 8)    // Might be "password
    {
        if (strncasecmp(token, "password", 8) == 0)
        {
            // YES it was!
            rv = SET_TYPE_PASSWORD;
        }
    }
    else if (s - token == 9)    // Might be "character"
    {
        if (strncasecmp(token, "character", 9) == 0)
        {
            // YES it was!
            rv = SET_TYPE_CHARACTER;
        }
    }
    else if (s - token == 11)   // Might be "transaction"
    {
        if (strncasecmp(token, "transaction", 11) == 0)
        {
            // YES it was!
            rv = SET_TYPE_TRANSACTION;
        }
    }

    return rv;
}

set_type_t get_set_type(const char* s)
{
    set_type_t rv = SET_TYPE_UNKNOWN;

    // Remove space from the beginning.
    while (isspace(*s))
    {
        ++s;
    }

    const char* token = s;

    // Find next non-space character.
    while (!isspace(*s) && (*s != 0))
    {
        ++s;
    }

    if (s - token == 3)     // Might be "set"
    {
        if (strncasecmp(token, "set", 3) == 0)
        {
            rv = get_set_type2(s);
        }
    }

    return rv;
}

/**
 * Detect query type by examining parsed representation of it.
 *
 * @param pi    The parsing info.
 * @param thd   MariaDB thread context.
 *
 * @return Copy of query type value.
 *
 *
 * @details Query type is deduced by checking for certain properties
 * of them. The order is essential. Some SQL commands have multiple
 * flags set and changing the order in which flags are tested,
 * the resulting type may be different.
 *
 */
static uint32_t resolve_query_type(parsing_info_t* pi, THD* thd)
{
    mxs::sql::Type qtype = mxs::sql::TYPE_UNKNOWN;
    uint32_t type = mxs::sql::TYPE_UNKNOWN;
    int set_autocommit_stmt = -1;   /*< -1 no, 0 disable, 1 enable */
    LEX* lex;
    Item* item;
    /**
     * By default, if sql_log_bin, that is, recording data modifications
     * to binary log, is disabled, gateway treats operations normally.
     * Effectively nothing is replicated.
     * When force_data_modify_op_replication is TRUE, gateway distributes
     * all write operations to all nodes.
     */
#if defined (NOT_IN_USE)
    bool force_data_modify_op_replication;
    force_data_modify_op_replication = FALSE;
#endif /* NOT_IN_USE */
    mxb_assert_message(thd != NULL, ("thd is NULL\n"));

    lex = thd->lex;

    /** SELECT ..INTO variable|OUTFILE|DUMPFILE */
    if (lex->result != NULL)
    {
        if (dynamic_cast<select_to_file*>(lex->result))
        {
            // SELECT ... INTO DUMPFILE|OUTFILE ...
            type = mxs::sql::TYPE_WRITE;
        }
        else
        {
            // SELECT ... INTO @var
            type = mxs::sql::TYPE_GSYSVAR_WRITE;
        }
        goto return_qtype;
    }

    if (lex->describe)
    {
        type = mxs::sql::TYPE_READ;
        goto return_qtype;
    }

    if (skygw_stmt_causes_implicit_commit(lex, &set_autocommit_stmt))
    {
        if (mxb_log_should_log(LOG_INFO))
        {
            if (sql_command_flags[lex->sql_command] & PP_CF_IMPLICIT_COMMIT_BEGIN)
            {
                MXB_INFO("Implicit COMMIT before executing the next command.");
            }
            else if (sql_command_flags[lex->sql_command] & PP_CF_IMPLICIT_COMMIT_END)
            {
                MXB_INFO("Implicit COMMIT after executing the next command.");
            }
        }

        if (set_autocommit_stmt == 1)
        {
            type |= mxs::sql::TYPE_ENABLE_AUTOCOMMIT;
            type |= mxs::sql::TYPE_COMMIT;
        }
    }

    if (set_autocommit_stmt == 0)
    {
        if (mxb_log_should_log(LOG_INFO))
        {
            MXB_INFO("Disable autocommit : implicit START TRANSACTION"
                     " before executing the next command.");
        }

        type |= mxs::sql::TYPE_DISABLE_AUTOCOMMIT;
        type |= mxs::sql::TYPE_BEGIN_TRX;
    }

    if (lex->sql_command == SQLCOM_SHOW_STATUS)
    {
        if (lex->option_type == OPT_GLOBAL)
        {
            // Force to master.
            type = mxs::sql::TYPE_WRITE;
        }
        else
        {
            type = mxs::sql::TYPE_READ;
        }

        goto return_qtype;
    }

    if (lex->sql_command == SQLCOM_SHOW_VARIABLES)
    {
        if (lex->option_type == OPT_GLOBAL)
        {
            type |= mxs::sql::TYPE_GSYSVAR_READ;
        }
        else
        {
            type |= mxs::sql::TYPE_SYSVAR_READ;
        }

        goto return_qtype;
    }

    if (lex->option_type == OPT_GLOBAL && lex->sql_command != SQLCOM_SET_OPTION)
    {
        /**
         * REVOKE ALL, ASSIGN_TO_KEYCACHE,
         * PRELOAD_KEYS, FLUSH, RESET, CREATE|ALTER|DROP SERVER
         */
        type |= mxs::sql::TYPE_GSYSVAR_WRITE;

        goto return_qtype;
    }

    if (lex->sql_command == SQLCOM_SET_OPTION)
    {
        switch (get_set_type(pi->pi_query_plain_str.c_str()))
        {
        case SET_TYPE_PASSWORD:
            type |= mxs::sql::TYPE_WRITE;
            break;

        case SET_TYPE_DEFAULT_ROLE:
            type |= mxs::sql::TYPE_WRITE;
            break;

        case SET_TYPE_NAMES:
            {
                type |= mxs::sql::TYPE_SESSION_WRITE;

                List_iterator<set_var_base> ilist(lex->var_list);

                while (set_var_base* var = ilist++)
                {
                    if (var->is_system())
                    {
                        type |= mxs::sql::TYPE_GSYSVAR_WRITE;
                    }
                }
            }
            break;

        case SET_TYPE_TRANSACTION:
            {
                if (lex->option_type == SHOW_OPT_GLOBAL)
                {
                    type |= mxs::sql::TYPE_GSYSVAR_WRITE;
                }
                else
                {
                    if (lex->option_type == SHOW_OPT_SESSION)
                    {
                        type |= mxs::sql::TYPE_SESSION_WRITE;
                    }
                    else
                    {
                        type |= mxs::sql::TYPE_NEXT_TRX;
                    }

                    List_iterator<set_var_base> ilist(lex->var_list);

                    while (set_var* var = static_cast<set_var*>(ilist++))
                    {
                        mxb_assert(var);
                        var->update(thd);

                        if (strcasestr(pi->pi_query_plain_str.c_str(), "write"))
                        {
                            type |= mxs::sql::TYPE_READWRITE;
                        }
                        else if (strcasestr(pi->pi_query_plain_str.c_str(), "only"))
                        {
                            type |= mxs::sql::TYPE_READONLY;
                        }
                    }
                }
            }
            break;

        case SET_TYPE_UNKNOWN:
            {
                type |= mxs::sql::TYPE_SESSION_WRITE;
                /** Either user- or system variable write */
                List_iterator<set_var_base> ilist(lex->var_list);
                size_t n = 0;

                while (set_var_base* var = ilist++)
                {
                    if (var->is_system())
                    {
                        type |= mxs::sql::TYPE_GSYSVAR_WRITE;
                    }
                    else
                    {
                        type |= mxs::sql::TYPE_USERVAR_WRITE;
                    }
                    ++n;
                }

                if (n == 0)
                {
                    type |= mxs::sql::TYPE_GSYSVAR_WRITE;
                }
            }
            break;

        default:
            type |= mxs::sql::TYPE_SESSION_WRITE;
        }

        goto return_qtype;
    }

    /**
     * 1:ALTER TABLE, TRUNCATE, REPAIR, OPTIMIZE, ANALYZE, CHECK.
     * 2:CREATE|ALTER|DROP|TRUNCATE|RENAME TABLE, LOAD, CREATE|DROP|ALTER DB,
     *   CREATE|DROP INDEX, CREATE|DROP VIEW, CREATE|DROP TRIGGER,
     *   CREATE|ALTER|DROP EVENT, UPDATE, INSERT, INSERT(SELECT),
     *   DELETE, REPLACE, REPLACE(SELECT), CREATE|RENAME|DROP USER,
     *   GRANT, REVOKE, OPTIMIZE, CREATE|ALTER|DROP FUNCTION|PROCEDURE,
     *   CREATE SPFUNCTION, INSTALL|UNINSTALL PLUGIN
     */
    if (is_log_table_write_query(lex->sql_command)
        || is_update_query(lex->sql_command))
    {
#if defined (NOT_IN_USE)

        if (thd->variables.sql_log_bin == 0
            && force_data_modify_op_replication)
        {
            /** Not replicated */
            type |= mxs::sql::TYPE_SESSION_WRITE;
        }
        else
#endif /* NOT_IN_USE */
        {
            /** Written to binlog, that is, replicated except tmp tables */
            type |= mxs::sql::TYPE_WRITE;   /*< to master */

            if (lex->sql_command == SQLCOM_CREATE_TABLE
                && (lex->create_info.options & HA_LEX_CREATE_TMP_TABLE))
            {
                type |= mxs::sql::TYPE_CREATE_TMP_TABLE;    /*< remember in router */
            }
        }
    }

    /** Try to catch session modifications here */
    switch (lex->sql_command)
    {
    case SQLCOM_EMPTY_QUERY:
        type |= mxs::sql::TYPE_READ;
        break;

    case SQLCOM_CHANGE_DB:
        type |= mxs::sql::TYPE_SESSION_WRITE;
        break;

    case SQLCOM_DEALLOCATE_PREPARE:
        type |= mxs::sql::TYPE_DEALLOC_PREPARE;
        break;

    case SQLCOM_SELECT:
        type |= mxs::sql::TYPE_READ;
        break;

    case SQLCOM_CALL:
        type |= mxs::sql::TYPE_WRITE;
        break;

    case SQLCOM_BEGIN:
        type |= mxs::sql::TYPE_BEGIN_TRX;
        if (lex->start_transaction_opt & MYSQL_START_TRANS_OPT_READ_WRITE)
        {
            type |= mxs::sql::TYPE_WRITE;
        }
        else if (lex->start_transaction_opt & MYSQL_START_TRANS_OPT_READ_ONLY)
        {
            type |= mxs::sql::TYPE_READ;
        }
        goto return_qtype;
        break;

    case SQLCOM_COMMIT:
        type |= mxs::sql::TYPE_COMMIT;
        goto return_qtype;
        break;

    case SQLCOM_ROLLBACK:
        type |= mxs::sql::TYPE_ROLLBACK;
        goto return_qtype;
        break;

    case SQLCOM_PREPARE:
        type |= mxs::sql::TYPE_PREPARE_NAMED_STMT;
        goto return_qtype;
        break;

    case SQLCOM_SET_OPTION:
        type |= mxs::sql::TYPE_SESSION_WRITE;
        goto return_qtype;
        break;

    case SQLCOM_SHOW_CREATE:
    case SQLCOM_SHOW_CREATE_DB:
    case SQLCOM_SHOW_CREATE_FUNC:
    case SQLCOM_SHOW_CREATE_PROC:
    case SQLCOM_SHOW_DATABASES:
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_FUNC_CODE:
    case SQLCOM_SHOW_GRANTS:
    case SQLCOM_SHOW_PROC_CODE:
    case SQLCOM_SHOW_SLAVE_HOSTS:
    case SQLCOM_SHOW_SLAVE_STAT:
    case SQLCOM_SHOW_STATUS:
    case SQLCOM_SHOW_TABLES:
    case SQLCOM_SHOW_TABLE_STATUS:
        type |= mxs::sql::TYPE_READ;
        goto return_qtype;
        break;

    case SQLCOM_END:
        goto return_qtype;
        break;

    case SQLCOM_RESET:
        if (lex->type & REFRESH_QUERY_CACHE)
        {
            type |= mxs::sql::TYPE_SESSION_WRITE;
        }
        else
        {
            type |= mxs::sql::TYPE_WRITE;
        }
        break;

    case SQLCOM_XA_START:
        type |= mxs::sql::TYPE_BEGIN_TRX;
        break;

    case SQLCOM_XA_END:
        type |= mxs::sql::TYPE_COMMIT;
        break;

    default:
        type |= mxs::sql::TYPE_WRITE;
        break;
    }

#if defined (UPDATE_VAR_SUPPORT)

    if (QTYPE_LESS_RESTRICTIVE_THAN_WRITE(type))
#endif
    // TODO: This test is meaningless, since at this point
    // TODO: qtype (not type) is mxs::sql::TYPE_UNKNOWN.
    if (Parser::type_mask_contains(qtype, mxs::sql::TYPE_UNKNOWN)
        || Parser::type_mask_contains(qtype, mxs::sql::TYPE_READ)
        || Parser::type_mask_contains(qtype, mxs::sql::TYPE_USERVAR_READ)
        || Parser::type_mask_contains(qtype, mxs::sql::TYPE_SYSVAR_READ)
        || Parser::type_mask_contains(qtype, mxs::sql::TYPE_GSYSVAR_READ))
    {
        /**
         * These values won't change qtype more restrictive than write.
         * UDFs and procedures could possibly cause session-wide write,
         * but unless their content is replicated this is a limitation
         * of this implementation.
         * In other words : UDFs and procedures are not allowed to
         * perform writes which are not replicated but need to repeat
         * in every node.
         * It is not sure if such statements exist. vraa 25.10.13
         */

        /**
         * Search for system functions, UDFs and stored procedures.
         */
        for (item = thd->free_list; item != NULL; item = item->next)
        {
            Item::Type itype;

            itype = item->type();

            if (itype == Item::SUBSELECT_ITEM)
            {
                continue;
            }
            else if (itype == Item::FUNC_ITEM)
            {
                int func_qtype = mxs::sql::TYPE_UNKNOWN;
                /**
                 * Item types:
                 * FIELD_ITEM = 0, FUNC_ITEM,
                 * SUM_FUNC_ITEM,  STRING_ITEM,    INT_ITEM,
                 * REAL_ITEM,      NULL_ITEM,      VARBIN_ITEM,
                 * COPY_STR_ITEM,  FIELD_AVG_ITEM,
                 * DEFAULT_VALUE_ITEM,             PROC_ITEM,
                 * COND_ITEM,      REF_ITEM,       FIELD_STD_ITEM,
                 * FIELD_VARIANCE_ITEM,
                 * INSERT_VALUE_ITEM,
                 * SUBSELECT_ITEM, ROW_ITEM,       CACHE_ITEM,
                 * TYPE_HOLDER,    PARAM_ITEM,
                 * TRIGGER_FIELD_ITEM,             DECIMAL_ITEM,
                 * XPATH_NODESET,  XPATH_NODESET_CMP,
                 * VIEW_FIXER_ITEM,
                 * EXPR_CACHE_ITEM == 27
                 **/

                Item_func::Functype ftype;
                ftype = ((Item_func*) item)->functype();

                /**
                 * Item_func types:
                 *
                 * UNKNOWN_FUNC = 0,EQ_FUNC,      EQUAL_FUNC,
                 * NE_FUNC,         LT_FUNC,      LE_FUNC,
                 * GE_FUNC,         GT_FUNC,      FT_FUNC,
                 * LIKE_FUNC == 10, ISNULL_FUNC,  ISNOTNULL_FUNC,
                 * COND_AND_FUNC,   COND_OR_FUNC, XOR_FUNC,
                 * BETWEEN,         IN_FUNC,
                 * MULT_EQUAL_FUNC, INTERVAL_FUNC,
                 * ISNOTNULLTEST_FUNC == 20,
                 * SP_EQUALS_FUNC,  SP_DISJOINT_FUNC,
                 * SP_INTERSECTS_FUNC,
                 * SP_TOUCHES_FUNC, SP_CROSSES_FUNC,
                 * SP_WITHIN_FUNC,  SP_CONTAINS_FUNC,
                 * SP_OVERLAPS_FUNC,
                 * SP_STARTPOINT,   SP_ENDPOINT == 30,
                 * SP_EXTERIORRING, SP_POINTN,    SP_GEOMETRYN,
                 * SP_INTERIORRINGN,NOT_FUNC,     NOT_ALL_FUNC,
                 * NOW_FUNC,        TRIG_COND_FUNC,
                 * SUSERVAR_FUNC,   GUSERVAR_FUNC == 40,
                 * COLLATE_FUNC,    EXTRACT_FUNC,
                 * CHAR_TYPECAST_FUNC,
                 * FUNC_SP,         UDF_FUNC,     NEG_FUNC,
                 * GSYSVAR_FUNC == 47
                 **/
                switch (ftype)
                {
                case Item_func::FUNC_SP:
                    /**
                     * An unknown (for maxscale) function / sp
                     * belongs to this category.
                     */
                    func_qtype |= mxs::sql::TYPE_WRITE;
                    break;

                case Item_func::UDF_FUNC:
                    func_qtype |= mxs::sql::TYPE_WRITE;
                    break;

                case Item_func::NOW_FUNC:
                    // If this is part of a CREATE TABLE, then local read is not
                    // applicable.
                    break;

                /** System session variable */
                case Item_func::GSYSVAR_FUNC:
                    {
                        const char* name;
                        size_t length;
                        get_string_and_length(item->name, &name, &length);

                        const char identity[] = "@@identity";
                        const char last_gtid[] = "@@last_gtid";
                        const char last_insert_id[] = "@@last_insert_id";

                        if (name
                            && (((length == sizeof(last_insert_id) - 1)
                                 && (strcasecmp(name, last_insert_id) == 0))
                                || ((length == sizeof(identity) - 1)
                                    && (strcasecmp(name, identity) == 0))
                                || ((length == sizeof(last_gtid) - 1)
                                    && (strcasecmp(name, last_gtid) == 0))))
                        {
                            func_qtype |= mxs::sql::TYPE_MASTER_READ;
                        }
                        else
                        {
                            func_qtype |= mxs::sql::TYPE_SYSVAR_READ;
                        }
                    }
                    break;

                /** User-defined variable read */
                case Item_func::GUSERVAR_FUNC:
                    func_qtype |= mxs::sql::TYPE_USERVAR_READ;
                    break;

                /** User-defined variable modification */
                case Item_func::SUSERVAR_FUNC:
                    func_qtype |= mxs::sql::TYPE_USERVAR_WRITE;
                    break;

                case Item_func::UNKNOWN_FUNC:

                    if (((Item_func*) item)->func_name() != NULL
                        && strcmp((char*) ((Item_func*) item)->func_name(), "last_insert_id") == 0)
                    {
                        func_qtype |= mxs::sql::TYPE_MASTER_READ;
                    }
                    else
                    {
                        func_qtype |= mxs::sql::TYPE_READ;
                    }

                    /**
                     * Many built-in functions are of this
                     * type, for example, rand(), soundex(),
                     * repeat() .
                     */
                    break;

                default:
                    break;
                }       /**< switch */

                /**< Set new query type */
                type |= func_qtype;
            }

#if defined (UPDATE_VAR_SUPPORT)

            /**
             * Write is as restrictive as it gets due functions,
             * so break.
             */
            if ((type & mxs::sql::TYPE_WRITE) == mxs::sql::TYPE_WRITE)
            {
                break;
            }

#endif
        }   /**< for */
    }       /**< if */

return_qtype:
    qtype = (mxs::sql::Type) type;
    return qtype;
}

/**
 * Checks if statement causes implicit COMMIT.
 * autocommit_stmt gets values 1, 0 or -1 if stmt is enable, disable or
 * something else than autocommit.
 *
 * @param lex           Parse tree
 * @param autocommit_stmt   memory address for autocommit status
 *
 * @return true if statement causes implicit commit and false otherwise
 */
static bool skygw_stmt_causes_implicit_commit(LEX* lex, int* autocommit_stmt)
{
    bool succp;

    if (!(sql_command_flags[lex->sql_command] & CF_AUTO_COMMIT_TRANS))
    {
        succp = false;
        goto return_succp;
    }

    switch (lex->sql_command)
    {
    case SQLCOM_DROP_TABLE:
        succp = !(lex->create_info.options & HA_LEX_CREATE_TMP_TABLE);
        break;

    case SQLCOM_ALTER_TABLE:
    case SQLCOM_CREATE_TABLE:
        /* If CREATE TABLE of non-temporary table, do implicit commit */
        succp = !(lex->create_info.options & HA_LEX_CREATE_TMP_TABLE);
        break;

    case SQLCOM_SET_OPTION:
        if ((*autocommit_stmt = is_autocommit_stmt(lex)) == 1)
        {
            succp = true;
        }
        else
        {
            succp = false;
        }

        break;

    default:
        succp = true;
        break;
    }

return_succp:
    return succp;
}

/**
 * Finds out if stmt is SET autocommit
 * and if the new value matches with the enable_cmd argument.
 *
 * @param lex   parse tree
 *
 * @return 1, 0, or -1 if command was:
 * enable, disable, or not autocommit, respectively.
 */
static int is_autocommit_stmt(LEX* lex)
{
    struct list_node* node;
    set_var* setvar;
    int rc = -1;
    static char target[8];      /*< for converted string */
    Item* item = NULL;

    node = lex->var_list.first_node();
    setvar = (set_var*) node->info;

    if (setvar == NULL)
    {
        goto return_rc;
    }

    do      /*< Search for the last occurrence of 'autocommit' */
    {
        if ((sys_var*) setvar->var == Sys_autocommit_ptr)
        {
            item = setvar->value;
        }

        node = node->next;
    }
    while ((setvar = (set_var*) node->info) != NULL);

    if (item != NULL)   /*< found autocommit command */
    {
        if (qcme_item_is_int(item))
        {
            rc = item->val_int();

            if (rc > 1 || rc < 0)
            {
                rc = -1;
            }
        }
        else if (qcme_item_is_string(item))
        {
            String str(target, sizeof(target), system_charset_info);
            String* res = item->val_str(&str);

            if ((rc = find_type(&bool_typelib, res->ptr(), res->length(), false)))
            {
                mxb_assert(rc >= 0 && rc <= 2);
                /**
                 * rc is the position of matchin string in
                 * typelib's value array.
                 * 1=OFF, 2=ON.
                 */
                rc -= 1;
            }
        }
    }

return_rc:
    return rc;
}

#if defined (NOT_USED)

char* pp_get_stmtname(const GWBUF* buf)
{
    MYSQL* mysql;

    if (buf == NULL
        || buf->gwbuf_bufobj == NULL
        || buf->gwbuf_bufobj->bo_data == NULL
        || (mysql = (MYSQL*) ((parsing_info_t*) buf->gwbuf_bufobj->bo_data)->pi_handle) == NULL
        || mysql->thd == NULL
        || (THD*) (mysql->thd))
    {
        ->lex == NULL
        || (THD*) (mysql->thd))->lex->prepared_stmt_name == NULL)
        {
            return NULL;
        }

        return ((THD*) (mysql->thd))->lex->prepared_stmt_name.str;
    }
}
#endif

/**
 * Get the parsing info structure from a GWBUF
 *
 * @param querybuf A GWBUF
 *
 * @return The parsing info object, or NULL
 */
parsing_info_t* get_pinfo(const GWBUF* querybuf)
{
    parsing_info_t* pi = NULL;

    if ((querybuf != NULL) && querybuf->get_protocol_info().get() != nullptr)
    {
        pi = static_cast<parsing_info_t*>(querybuf->get_protocol_info().get());
    }

    return pi;
}

LEX* get_lex(parsing_info_t* pi)
{
    MYSQL* mysql = (MYSQL*) pi->pi_handle;
    mxb_assert(mysql);
    THD* thd = (THD*) mysql->thd;
    mxb_assert(thd);

    return thd->lex;
}

/**
 * Get the parse tree from parsed querybuf.
 * @param querybuf  The parsed GWBUF
 *
 * @return Pointer to the LEX struct or NULL if an error occurred or the query
 * was not parsed
 */
LEX* get_lex(const GWBUF* querybuf)
{
    LEX* lex = NULL;
    parsing_info_t* pi = get_pinfo(querybuf);

    if (pi)
    {
        MYSQL* mysql = (MYSQL*) pi->pi_handle;
        mxb_assert(mysql);
        THD* thd = (THD*) mysql->thd;
        mxb_assert(thd);
        lex = thd->lex;
    }

    return lex;
}

/**
 * Finds the head of the list of tables affected by the current select statement.
 * @param thd Pointer to a valid THD
 * @return Pointer to the head of the TABLE_LIST chain or NULL in case of an error
 */
static TABLE_LIST* skygw_get_affected_tables(void* lexptr)
{
    LEX* lex = (LEX*) lexptr;

    if (lex == NULL || lex->current_select == NULL)
    {
        mxb_assert(lex != NULL && lex->current_select != NULL);
        return NULL;
    }

    TABLE_LIST* tbl = lex->current_select->table_list.first;

    if (tbl && tbl->schema_select_lex && tbl->schema_select_lex->table_list.elements
        && lex->sql_command != SQLCOM_SHOW_KEYS)
    {
        /**
         * Some statements e.g. EXPLAIN or SHOW COLUMNS give `information_schema`
         * as the underlying table and the table in the query is stored in
         * @c schema_select_lex.
         *
         * SHOW [KEYS | INDEX] does the reverse so we need to skip the
         * @c schema_select_lex when processing a SHOW [KEYS | INDEX] statement.
         */
        tbl = tbl->schema_select_lex->table_list.first;
    }

    return tbl;
}

static bool is_show_command(int sql_command)
{
    bool rv = false;

    switch (sql_command)
    {
    case SQLCOM_SHOW_CREATE:
    case SQLCOM_SHOW_DATABASES:
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_KEYS:
#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 5
    case SQLCOM_SHOW_BINLOG_STAT:
#else
    case SQLCOM_SHOW_MASTER_STAT:
#endif
    case SQLCOM_SHOW_SLAVE_STAT:
    case SQLCOM_SHOW_STATUS:
    case SQLCOM_SHOW_TABLES:
    case SQLCOM_SHOW_TABLE_STATUS:
    case SQLCOM_SHOW_VARIABLES:
    case SQLCOM_SHOW_WARNS:
        rv = true;
        break;

    default:
        break;
    }

    return rv;
}

int32_t pp_mysql_get_table_names(const Parser::Helper& helper,
                                 const GWBUF* querybuf, vector<Parser::TableName>* tables)
{
    LEX* lex;
    TABLE_LIST* tbl;

    if (!ensure_query_is_parsed(helper, querybuf))
    {
        return PP_RESULT_OK;
    }

    auto* pi = get_pinfo(querybuf);

    if (pi->table_names.empty())
    {
        if ((lex = get_lex(querybuf)) == NULL)
        {
            return PP_RESULT_OK;
        }

        if (lex->describe || (is_show_command(lex->sql_command) && !(lex->sql_command == SQLCOM_SHOW_FIELDS)))
        {
            return PP_RESULT_OK;
        }

        lex->current_select = lex->all_selects_list;

        while (lex->current_select)
        {
            tbl = skygw_get_affected_tables(lex);

            while (tbl)
            {
                const char* zTable = qcme_string_get(tbl->table_name);

                if (strcmp(zTable, "*") != 0)
                {
                    string db;
                    const char* zDb = qcme_string_get(tbl->db);

                    if (zDb && (strcmp(zDb, "skygw_virtual") != 0))
                    {
                        db = zDb;
                    }

                    string table(zTable);

                    auto end1 = pi->table_names.end();
                    auto it1 = find_if(pi->table_names.begin(), end1, [db, table](const auto& n) {
                            return n.db == db && n.table == table;
                        });

                    if (it1 == end1)
                    {
                        TableName t { db, table };

                        pi->table_names.push_back(t);
                    }
                }

                tbl = tbl->next_local;
            }   /*< while (tbl) */

            lex->current_select = lex->current_select->next_select_in_list();
        }   /*< while(lex->current_select) */
    }

    tables->assign(pi->table_names.begin(), pi->table_names.end());

    return PP_RESULT_OK;
}

/**
 * Create parsing information; initialize mysql handle, allocate parsing info
 * struct and set handle and free function pointer to it.
 *
 * @return pointer to parsing information
 */
static std::unique_ptr<parsing_info_t> parsing_info_init(const Parser::Helper& helper, const GWBUF* querybuf)
{
    return std::make_unique<parsing_info_t>(helper, querybuf);
}

/**
 * Add plain text query string to parsing info.
 *
 * @param ptr   Pointer to parsing info struct, cast required
 * @param str   String to be added
 *
 * @return void
 */
int32_t pp_mysql_get_database_names(const Parser::Helper& helper,
                                    const GWBUF* querybuf, vector<string_view>* pNames)
{
    if (!querybuf || !ensure_query_is_parsed(helper, querybuf))
    {
        return PP_RESULT_OK;
    }

    auto* pi = get_pinfo(querybuf);

    if (pi->database_names.empty())
    {
        LEX* lex = get_lex(querybuf);

        if (!lex)
        {
            return PP_RESULT_OK;
        }

        if (lex->describe || (is_show_command(lex->sql_command)
                              && !(lex->sql_command == SQLCOM_SHOW_TABLES)
                              && !(lex->sql_command == SQLCOM_SHOW_TABLE_STATUS)
                              && !(lex->sql_command == SQLCOM_SHOW_FIELDS)))
        {
            return PP_RESULT_OK;
        }

        if (lex->sql_command == SQLCOM_CHANGE_DB
            || lex->sql_command == SQLCOM_SHOW_TABLES
            || lex->sql_command == SQLCOM_SHOW_TABLE_STATUS)
        {
            SELECT_LEX* select_lex = qcme_get_first_select_lex(lex);
            if (qcme_string_get(select_lex->db)
                && (strcmp(qcme_string_get(select_lex->db), "skygw_virtual") != 0))
            {
                pi->database_names.push_back(qcme_string_get(select_lex->db));
            }
        }
        else
        {
            lex->current_select = lex->all_selects_list;

            while (lex->current_select)
            {
                TABLE_LIST* tbl = lex->current_select->table_list.first;

                while (tbl)
                {
                    if (lex->sql_command == SQLCOM_SHOW_FIELDS)
                    {
                        // If we are describing, we want the actual table, not the information_schema.
                        if (tbl->schema_select_lex)
                        {
                            tbl = tbl->schema_select_lex->table_list.first;
                        }
                    }

                    // The database is sometimes an empty string. So as not to return
                    // an array of empty strings, we need to check for that possibility.
                    if ((strcmp(qcme_string_get(tbl->db), "skygw_virtual") != 0)
                        && (*qcme_string_get(tbl->db) != 0))
                    {
                        auto str = qcme_string_get(tbl->db);

                        if (find(pi->database_names.begin(), pi->database_names.end(), str) == pi->database_names.end())
                        {
                            pi->database_names.push_back(str);
                        }
                    }

                    tbl = tbl->next_local;
                }

                lex->current_select = lex->current_select->next_select_in_list();
            }
        }
    }

    pNames->clear();
    copy(pi->database_names.begin(), pi->database_names.end(), back_inserter(*pNames));

    return PP_RESULT_OK;
}

int32_t pp_mysql_get_kill_info(const GWBUF* querybuf, Parser::KillInfo* pKill)
{
    // TODO: Implement this
    return PP_RESULT_ERROR;
}

int32_t pp_mysql_get_operation(const Parser::Helper& helper,
                               const GWBUF* querybuf, int32_t* operation)
{
    *operation = mxs::sql::OP_UNDEFINED;

    if (querybuf)
    {
        if (ensure_query_is_parsed(helper, querybuf))
        {
            parsing_info_t* pi = get_pinfo(querybuf);
            LEX* lex = get_lex(pi);

            if (lex)
            {
                if (lex->describe || lex->analyze_stmt)
                {
                    *operation = mxs::sql::OP_EXPLAIN;
                }
                else
                {
                    switch (lex->sql_command)
                    {
                    case SQLCOM_ANALYZE:
                        *operation = mxs::sql::OP_EXPLAIN;
                        break;

                    case SQLCOM_SELECT:
                        *operation = mxs::sql::OP_SELECT;
                        break;

                    case SQLCOM_CREATE_DB:
                    case SQLCOM_CREATE_EVENT:
                    case SQLCOM_CREATE_FUNCTION:
                    case SQLCOM_CREATE_INDEX:
                    case SQLCOM_CREATE_PROCEDURE:
#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 3
                    case SQLCOM_CREATE_SEQUENCE:
#endif
                    case SQLCOM_CREATE_SERVER:
                    case SQLCOM_CREATE_SPFUNCTION:
                    case SQLCOM_CREATE_TRIGGER:
                    case SQLCOM_CREATE_USER:
                    case SQLCOM_CREATE_VIEW:
                        *operation = mxs::sql::OP_CREATE;
                        break;

                    case SQLCOM_CREATE_TABLE:
                        *operation = mxs::sql::OP_CREATE_TABLE;
                        break;

                    case SQLCOM_ALTER_DB:
                    case SQLCOM_ALTER_DB_UPGRADE:
                    case SQLCOM_ALTER_EVENT:
                    case SQLCOM_ALTER_FUNCTION:
                    case SQLCOM_ALTER_PROCEDURE:
                    case SQLCOM_ALTER_SERVER:
#if MYSQL_VERSION_MAJOR != 10 || MYSQL_VERSION_MINOR < 7
                        case SQLCOM_ALTER_TABLESPACE:
#endif
                        *operation = mxs::sql::OP_ALTER;
                        break;

                    case SQLCOM_ALTER_TABLE:
                        *operation = mxs::sql::OP_ALTER_TABLE;
                        break;

                    case SQLCOM_UPDATE:
                    case SQLCOM_UPDATE_MULTI:
                        *operation = mxs::sql::OP_UPDATE;
                        break;

                    case SQLCOM_INSERT:
                    case SQLCOM_INSERT_SELECT:
                    case SQLCOM_REPLACE:
                    case SQLCOM_REPLACE_SELECT:
                        *operation = mxs::sql::OP_INSERT;
                        break;

                    case SQLCOM_DELETE:
                    case SQLCOM_DELETE_MULTI:
                        *operation = mxs::sql::OP_DELETE;
                        break;

                    case SQLCOM_TRUNCATE:
                        *operation = mxs::sql::OP_TRUNCATE;
                        break;

                    case SQLCOM_DROP_DB:
                    case SQLCOM_DROP_EVENT:
                    case SQLCOM_DROP_FUNCTION:
                    case SQLCOM_DROP_INDEX:
                    case SQLCOM_DROP_PROCEDURE:
#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 3
                    case SQLCOM_DROP_SEQUENCE:
#endif
                    case SQLCOM_DROP_SERVER:
                    case SQLCOM_DROP_TRIGGER:
                    case SQLCOM_DROP_USER:
                    case SQLCOM_DROP_VIEW:
                        *operation = mxs::sql::OP_DROP;
                        break;

                    case SQLCOM_DROP_TABLE:
                        *operation = mxs::sql::OP_DROP_TABLE;
                        break;

                    case SQLCOM_CHANGE_DB:
                        *operation = mxs::sql::OP_CHANGE_DB;
                        break;

                    case SQLCOM_LOAD:
                        *operation = mxs::sql::OP_LOAD_LOCAL;
                        break;

                    case SQLCOM_GRANT:
                        *operation = mxs::sql::OP_GRANT;
                        break;

                    case SQLCOM_REVOKE:
                    case SQLCOM_REVOKE_ALL:
                        *operation = mxs::sql::OP_REVOKE;
                        break;

                    case SQLCOM_SET_OPTION:
                        switch (get_set_type(pi->pi_query_plain_str.c_str()))
                        {
                        case SET_TYPE_TRANSACTION:
                            *operation = mxs::sql::OP_SET_TRANSACTION;
                            break;

                        default:
                            *operation = mxs::sql::OP_SET;
                        }
                        break;

                    case SQLCOM_SHOW_DATABASES:
                        *operation = mxs::sql::OP_SHOW_DATABASES;
                        break;

                    case SQLCOM_SHOW_CREATE:
                    case SQLCOM_SHOW_CREATE_DB:
                    case SQLCOM_SHOW_CREATE_FUNC:
                    case SQLCOM_SHOW_CREATE_PROC:
                    case SQLCOM_SHOW_FIELDS:
                    case SQLCOM_SHOW_FUNC_CODE:
                    case SQLCOM_SHOW_GRANTS:
                    case SQLCOM_SHOW_KEYS:
#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 5
                    case SQLCOM_SHOW_BINLOG_STAT:
#else
                    case SQLCOM_SHOW_MASTER_STAT:
#endif
                    case SQLCOM_SHOW_PROC_CODE:
                    case SQLCOM_SHOW_SLAVE_HOSTS:
                    case SQLCOM_SHOW_SLAVE_STAT:
                    case SQLCOM_SHOW_STATUS:
                    case SQLCOM_SHOW_TABLES:
                    case SQLCOM_SHOW_TABLE_STATUS:
                    case SQLCOM_SHOW_VARIABLES:
                    case SQLCOM_SHOW_WARNS:
                        *operation = mxs::sql::OP_SHOW;
                        break;

                    case SQLCOM_EXECUTE:
                        *operation = mxs::sql::OP_EXECUTE;
                        break;

                    case SQLCOM_CALL:
                        *operation = mxs::sql::OP_CALL;
                        break;

                    default:
                        *operation = mxs::sql::OP_UNDEFINED;
                    }
                }
            }
        }
    }

    return PP_RESULT_OK;
}

int32_t pp_mysql_get_prepare_name(const Parser::Helper& helper,
                                  const GWBUF* stmt, std::string_view* namep)
{
    *namep = std::string_view {};

    if (stmt)
    {
        if (ensure_query_is_parsed(helper, stmt))
        {
            auto* pi = get_pinfo(stmt);

            if (pi->prepare_name.empty())
            {
                LEX* lex = get_lex(stmt);

                if (!lex->describe)
                {
                    if ((lex->sql_command == SQLCOM_PREPARE)
                        || (lex->sql_command == SQLCOM_EXECUTE)
                        || (lex->sql_command == SQLCOM_DEALLOCATE_PREPARE))
                    {
                        // LEX_STRING or LEX_CSTRING
                        const auto& prepared_stmt_name = qcme_get_prepared_stmt_name(lex);
                        pi->prepare_name.assign(prepared_stmt_name.str, prepared_stmt_name.length);
                    }
                }
            }

            *namep = pi->prepare_name;
        }
    }

    return PP_RESULT_OK;
}

int32_t pp_mysql_get_preparable_stmt(const Parser::Helper& helper,
                                     const GWBUF* stmt, GWBUF** preparable_stmt)
{
    if (stmt)
    {
        if (ensure_query_is_parsed(helper, stmt))
        {
            LEX* lex = get_lex(stmt);

            if ((lex->sql_command == SQLCOM_PREPARE) && !lex->describe)
            {
                parsing_info_t* pi = get_pinfo(stmt);

                if (!pi->preparable_stmt)
                {
                    const char* zpreparable_stmt;
                    size_t preparable_stmt_len;
// MYSQL_VERSION_PATCH might be smaller, but this was detected with 10.2.32.
#if MYSQL_VERSION_MINOR >= 3 || (MYSQL_VERSION_MINOR == 2 && MYSQL_VERSION_PATCH >= 32)
                    zpreparable_stmt = qcme_get_prepared_stmt_code(lex)->str_value.ptr();
                    preparable_stmt_len = qcme_get_prepared_stmt_code(lex)->str_value.length();
#else
                    zpreparable_stmt = lex->prepared_stmt_code.str;
                    preparable_stmt_len = lex->prepared_stmt_code.length;
#endif
                    size_t payload_len = preparable_stmt_len + 1;
                    size_t packet_len = MYSQL_HEADER_LEN + payload_len;

                    char* tmp = new char [payload_len];
                    char* s = tmp;

                    // We copy the statment, blindly replacing all '?':s (always)
                    // and ':N' (in Oracle mode) with '0':s as otherwise the parsing of the
                    // preparable statement as a regular statement will not always succeed.
                    Parser::SqlMode sql_mode = ::this_thread.sql_mode;
                    const char* p = zpreparable_stmt;
                    const char* end = zpreparable_stmt + preparable_stmt_len;
                    bool replacement = false;
                    while (p < end)
                    {
                        if (*p == '?')
                        {
                            *s = '0';
                        }
                        else if (sql_mode == Parser::SqlMode::ORACLE)
                        {
                            if (*p == ':' && p + 1 < end)
                            {
                                // This may be an Oracle specific positional parameter.
                                char c = *(p + 1);
                                if (isalnum(c))
                                {
                                    ++p;
                                    // e.g. :4711 or :aaa
                                    while (p + 1 < end && isalnum(*(p + 1)))
                                    {
                                        ++p;
                                    }

                                    replacement = true;
                                    *s = '0';
                                }
                                else if (c == '\'' || c == '\"')
                                {
                                    // e.g. :"abc"
                                    char quote = *p;
                                    while (p + 1 < end && *(p + 1) != quote)
                                    {
                                        ++p;
                                    }

                                    replacement = true;
                                    *s = '0';
                                }
                            }
                            else
                            {
                                *s = *p;
                            }
                        }
                        else
                        {
                            *s = *p;
                        }

                        if (p != end)
                        {
                            ++p;
                        }

                        ++s;
                    }

                    if (replacement)
                    {
                        // If something has been replaced, then we stash a NULL at the
                        // end so that parsing will stop at the right spot.
                        *s = 0;
                    }

                    std::string_view sv(tmp, s - tmp);
                    GWBUF* preparable_packet = new GWBUF(helper.create_packet(sv));
                    delete [] tmp;

                    pi->preparable_stmt = preparable_packet;
                }

                *preparable_stmt = pi->preparable_stmt;
            }
        }
    }

    return PP_RESULT_OK;
}

static bool should_exclude(const char* name, List<Item>* excludep)
{
    bool exclude = false;
    List_iterator<Item> ilist(*excludep);
    Item* exclude_item;

    while (!exclude && (exclude_item = ilist++))
    {
        const char* exclude_name;
        size_t length;
        get_string_and_length(exclude_item->name, &exclude_name, &length);

        if (exclude_name
            && (strlen(name) == length)
            && (strcasecmp(name, exclude_name) == 0))
        {
            exclude = true;
        }

        if (!exclude)
        {
            exclude_name = strrchr(exclude_item->full_name(), '.');

            if (exclude_name)
            {
                ++exclude_name;     // Char after the '.'

                if (strcasecmp(name, exclude_name) == 0)
                {
                    exclude = true;
                }
            }
        }
    }

    return exclude;
}

static void unalias_names(st_select_lex* select,
                          const char* from_database,
                          const char* from_table,
                          const char** to_database,
                          const char** to_table)
{
    *to_database = from_database;
    *to_table = from_table;

    if (!from_database && from_table)
    {
        st_select_lex* s = select;

        while ((*to_table == from_table) && s)
        {
            TABLE_LIST* tbl = s->table_list.first;

            while ((*to_table == from_table) && tbl)
            {
                if (qcme_string_get(tbl->alias)
                    && (strcasecmp(qcme_string_get(tbl->alias), from_table) == 0)
                    && (strcasecmp(qcme_string_get(tbl->table_name), "*") != 0))
                {
                    // The dummy default database "skygw_virtual" is not included.
                    if (qcme_string_get(tbl->db)
                        && *qcme_string_get(tbl->db)
                        && (strcmp(qcme_string_get(tbl->db), "skygw_virtual") != 0))
                    {
                        *to_database = (char*)qcme_string_get(tbl->db);
                    }
                    *to_table = (char*)qcme_string_get(tbl->table_name);
                }

                tbl = tbl->next_local;
            }

            s = s->outer_select();
        }
    }
}

static void add_field_info(parsing_info_t* pi,
                           const char* database,
                           const char* table,
                           const char* column,
                           List<Item>* excludep)
{
    mxb_assert(column);

    Parser::FieldInfo item;

    if (database)
    {
        item.database = database;
    }

    if (table)
    {
        item.table = table;
    }

    if (column)
    {
        item.column = column;
    }

    size_t i;
    for (i = 0; i < pi->field_infos_len; ++i)
    {
        Parser::FieldInfo* field_info = pi->field_infos + i;

        if (sv_case_eq(item.column, field_info->column))
        {
            if (item.table.empty() && field_info->table.empty())
            {
                mxb_assert(item.database.empty() && field_info->database.empty());
                break;
            }
            else if (!item.table.empty() && sv_case_eq(item.table, field_info->table))
            {
                if (item.database.empty() && field_info->database.empty())
                {
                    break;
                }
                else if (!item.database.empty() && sv_case_eq(item.database, field_info->database))
                {
                    break;
                }
            }
        }
    }

    Parser::FieldInfo* field_infos = NULL;

    if (i == pi->field_infos_len)     // If true, the field was not present already.
    {
        // If only a column is specified, but not a table or database and we
        // have a list of expressions that should be excluded, we check if the column
        // value is present in that list. This is in order to exclude the second "d" in
        // a statement like "select a as d from x where d = 2".
        if (!(column && !table && !database && excludep && should_exclude(column, excludep)))
        {
            if (pi->field_infos_len < pi->field_infos_capacity)
            {
                field_infos = pi->field_infos;
            }
            else
            {
                size_t capacity = pi->field_infos_capacity ? 2 * pi->field_infos_capacity : 8;
                field_infos = (Parser::FieldInfo*)realloc(pi->field_infos,
                                                          capacity * sizeof(Parser::FieldInfo));

                if (field_infos)
                {
                    pi->field_infos = field_infos;
                    pi->field_infos_capacity = capacity;
                }
            }
        }
    }

    // If field_infos is NULL, then the field was found and has already been noted.
    if (field_infos)
    {
        pi->populate_field_info(item, database, table, column);

        field_infos[pi->field_infos_len++] = item;
    }
}

static void add_field_info(parsing_info_t* pi,
                           st_select_lex* select,
                           const char* database,
                           const char* table,
                           const char* column,
                           List<Item>* excludep)
{
    mxb_assert(column);

    unalias_names(select, database, table, &database, &table);

    add_field_info(pi, database, table, column, excludep);
}

static void add_function_field_usage(parsing_info_t* pi,
                                     const char* database,
                                     const char* table,
                                     const char* column,
                                     Parser::FunctionInfo* fi)
{
    bool found = false;
    uint32_t i = 0;

    while (!found && (i < fi->n_fields))
    {
        Parser::FieldInfo& field = fi->fields[i];

        if (sv_case_eq(field.column, column))
        {
            if (field.table.empty() && !table)
            {
                found = true;
            }
            else if (!field.table.empty() && table && sv_case_eq(field.table, table))
            {
                if (field.database.empty() && !database)
                {
                    found = true;
                }
                else if (!field.database.empty() && database && sv_case_eq(field.database, database))
                {
                    found = true;
                }
            }
        }

        ++i;
    }

    if (!found)
    {
        Parser::FieldInfo* fields = (Parser::FieldInfo*)realloc(fi->fields,
                                                                (fi->n_fields + 1)
                                                                * sizeof(Parser::FieldInfo));
        mxb_assert(fields);

        if (fields)
        {
            fi->fields = fields;

            Parser::FieldInfo field;
            pi->populate_field_info(field, database, table, column);

            fi->fields[fi->n_fields] = field;
            ++fi->n_fields;
        }
    }
}

static void add_function_field_usage(parsing_info_t* pi,
                                     st_select_lex* select,
                                     Item_field* item,
                                     Parser::FunctionInfo* fi)
{
    const char* database = mxs_strptr(item->db_name);
    const char* table = mxs_strptr(item->table_name);

    unalias_names(select, mxs_strptr(item->db_name), mxs_strptr(item->table_name), &database, &table);

    const char* s1;
    size_t l1;
    get_string_and_length(item->field_name, &s1, &l1);
    char* column = NULL;

    if (!database && !table)
    {
        if (select)
        {
            List_iterator<Item> ilist(select->item_list);
            Item* item2;

            while (!column && (item2 = ilist++))
            {
                if (item2->type() == Item::FIELD_ITEM)
                {
                    Item_field* field = (Item_field*)item2;

                    const char* s2;
                    size_t l2;
                    get_string_and_length(field->name, &s2, &l2);

                    if (l1 == l2)
                    {
                        if (strncasecmp(s1, s2, l1) == 0)
                        {
                            get_string_and_length(field->orig_field_name, &s1, &l1);
                            column = strndup(s1, l1);

                            table = mxs_strptr(field->orig_table_name);
                            database = mxs_strptr(field->orig_db_name);
                        }
                    }
                }
            }
        }
    }

    if (!column)
    {
        get_string_and_length(item->field_name, &s1, &l1);
        column = strndup(s1, l1);
    }

    add_function_field_usage(pi, database, table, column, fi);

    free(column);
}

static void add_function_field_usage(parsing_info_t* pi,
                                     st_select_lex* select,
                                     Item** items,
                                     int n_items,
                                     Parser::FunctionInfo* fi)
{
    for (int i = 0; i < n_items; ++i)
    {
        Item* item = items[i];

        switch (item->type())
        {
        case Item::FIELD_ITEM:
            add_function_field_usage(pi, select, static_cast<Item_field*>(item), fi);
            break;

        default:
            if (qcme_item_is_string(item))
            {
                if (::this_thread.options & Parser::OPTION_STRING_ARG_AS_FIELD)
                {
                    String* s = item->val_str();
                    int len = s->length();
                    char tmp[len + 1];
                    memcpy(tmp, s->ptr(), len);
                    tmp[len] = 0;

                    add_function_field_usage(pi, nullptr, nullptr, tmp, fi);
                }
            }
            else
            {
                // mxb_assert(!true);
            }
        }
    }
}

static void add_function_field_usage(parsing_info_t* pi,
                                     st_select_lex* select,
                                     st_select_lex* sub_select,
                                     Parser::FunctionInfo* fi)
{
    List_iterator<Item> ilist(sub_select->item_list);

    while (Item* item = ilist++)
    {
        if (item->type() == Item::FIELD_ITEM)
        {
            add_function_field_usage(pi, select, static_cast<Item_field*>(item), fi);
        }
    }
}

static Parser::FunctionInfo* get_function_info(parsing_info_t* pi, const char* zName)
{
    Parser::FunctionInfo* function_info = NULL;

    size_t i;
    for (i = 0; i < pi->function_infos_len; ++i)
    {
        function_info = pi->function_infos + i;

        if (sv_case_eq(zName, function_info->name))
        {
            break;
        }
    }

    if (i == pi->function_infos_len)
    {
        // Not found

        if (pi->function_infos_len == pi->function_infos_capacity)
        {
            size_t capacity = pi->function_infos_capacity ? 2 * pi->function_infos_capacity : 8;
            Parser::FunctionInfo* function_infos =
                (Parser::FunctionInfo*)realloc(pi->function_infos,
                                               capacity * sizeof(Parser::FunctionInfo));
            assert(function_infos);

            pi->function_infos = function_infos;
            pi->function_infos_capacity = capacity;
        }

        std::string_view name = pi->get_string_view("function", zName);

        pi->function_infos[pi->function_infos_len] = Parser::FunctionInfo { name, nullptr, 0 };
        function_info = &pi->function_infos[pi->function_infos_len++];
    }

    return function_info;
}

static Parser::FunctionInfo* add_function_info(parsing_info_t* pi,
                                               st_select_lex* select,
                                               const char* zName,
                                               Item** items,
                                               int n_items)
{
    mxb_assert(zName);

    Parser::FunctionInfo* function_info = NULL;

    zName = map_function_name(pi->function_name_mappings, zName);

    size_t i;
    for (i = 0; i < pi->function_infos_len; ++i)
    {
        if (sv_case_eq(zName, pi->function_infos[i].name))
        {
            function_info = &pi->function_infos[i];
            break;
        }
    }

    Parser::FunctionInfo* function_infos = NULL;

    if (!function_info)
    {
        if (pi->function_infos_len < pi->function_infos_capacity)
        {
            function_infos = pi->function_infos;
        }
        else
        {
            size_t capacity = pi->function_infos_capacity ? 2 * pi->function_infos_capacity : 8;
            function_infos = (Parser::FunctionInfo*)realloc(pi->function_infos,
                                                            capacity * sizeof(Parser::FunctionInfo));
            assert(function_infos);

            pi->function_infos = function_infos;
            pi->function_infos_capacity = capacity;
        }

        std::string_view name = pi->get_string_view("function", zName);

        pi->function_infos[pi->function_infos_len] = Parser::FunctionInfo { name, nullptr, 0 };
        function_info = &pi->function_infos[pi->function_infos_len++];
    }

    add_function_field_usage(pi, select, items, n_items, function_info);

    return function_info;
}

static void add_field_info(parsing_info_t* pi,
                           st_select_lex* select,
                           Item_field* item,
                           List<Item>* excludep)
{
    const char* database = mxs_strptr(item->db_name);
    const char* table = mxs_strptr(item->table_name);
    const char* s;
    size_t l;
    get_string_and_length(item->field_name, &s, &l);
    char column[l + 1];
    strncpy(column, s, l);
    column[l] = 0;

    LEX* lex = get_lex(pi);

    switch (lex->sql_command)
    {
    case SQLCOM_SHOW_FIELDS:
        if (!database)
        {
            database = "information_schema";
        }

        if (!table)
        {
            table = "COLUMNS";
        }
        break;

    case SQLCOM_SHOW_KEYS:
        if (!database)
        {
            database = "information_schema";
        }

        if (!table)
        {
            table = "STATISTICS";
        }
        break;

    case SQLCOM_SHOW_STATUS:
        if (!database)
        {
            database = "information_schema";
        }

        if (!table)
        {
            table = "SESSION_STATUS";
        }
        break;

    case SQLCOM_SHOW_TABLES:
        if (!database)
        {
            database = "information_schema";
        }

        if (!table)
        {
            table = "TABLE_NAMES";
        }
        break;

    case SQLCOM_SHOW_TABLE_STATUS:
        if (!database)
        {
            database = "information_schema";
        }

        if (!table)
        {
            table = "TABLES";
        }
        break;

    case SQLCOM_SHOW_VARIABLES:
        if (!database)
        {
            database = "information_schema";
        }

        if (!table)
        {
            table = "SESSION_STATUS";
        }
        break;

    default:
        break;
    }

    add_field_info(pi, select, database, table, column, excludep);
}

static void add_field_info(parsing_info_t* pi,
                           st_select_lex* select,
                           Item* item,
                           List<Item>* excludep)
{
    const char* database = NULL;
    const char* table = NULL;
    const char* s;
    size_t l;
    get_string_and_length(item->name, &s, &l);
    char column[l + 1];
    strncpy(column, s, l);
    column[l] = 0;

    add_field_info(pi, select, database, table, column, excludep);
}

typedef enum collect_source
{
    COLLECT_SELECT,
    COLLECT_WHERE,
    COLLECT_HAVING,
    COLLECT_GROUP_BY,
    COLLECT_ORDER_BY
} collect_source_t;

static void update_field_infos(parsing_info_t* pi,
                               LEX* lex,
                               st_select_lex* select,
                               List<Item>* excludep);

static void remove_surrounding_back_ticks(char* s)
{
    size_t len = strlen(s);

    if (*s == '`')
    {
        --len;
        memmove(s, s + 1, len);
        s[len] = 0;
    }

    if (s[len - 1] == '`')
    {
        s[len - 1] = 0;
    }
}

static bool should_function_be_ignored(parsing_info_t* pi, const char* func_name, std::string* final_func_name)
{
    bool rv = false;

    *final_func_name = func_name;

    // We want to ignore functions that do not really appear as such in an
    // actual SQL statement. E.g. "SELECT @a" appears as a function "get_user_var".
    if ((strcasecmp(func_name, "decimal_typecast") == 0)
        || (strcasecmp(func_name, "cast_as_char") == 0)
        || (strcasecmp(func_name, "cast_as_date") == 0)
        || (strcasecmp(func_name, "cast_as_datetime") == 0)
        || (strcasecmp(func_name, "cast_as_time") == 0)
        || (strcasecmp(func_name, "cast_as_signed") == 0)
        || (strcasecmp(func_name, "cast_as_unsigned") == 0))
    {
        *final_func_name = "cast";
        rv = false;
    }
    else if ((strcasecmp(func_name, "get_user_var") == 0)
             || (strcasecmp(func_name, "get_system_var") == 0)
             || (strcasecmp(func_name, "not") == 0)
             || (strcasecmp(func_name, "collate") == 0)
             || (strcasecmp(func_name, "set_user_var") == 0)
             || (strcasecmp(func_name, "set_system_var") == 0))
    {
        rv = true;
    }

    // Any sequence related functions should be ignored as well.
#if MYSQL_VERSION_MAJOR >= 10 && MYSQL_VERSION_MINOR >= 3
    if (!rv)
    {
        if ((strcasecmp(func_name, "lastval") == 0)
            || (strcasecmp(func_name, "nextval") == 0))
        {
            pi->type_mask |= mxs::sql::TYPE_WRITE;
            rv = true;
        }
    }
#endif

#ifdef WF_SUPPORTED
    if (!rv)
    {
        if (strcasecmp(func_name, "WF") == 0)
        {
            rv = true;
        }
    }
#endif

    return rv;
}

static void update_field_infos(parsing_info_t* pi,
                               st_select_lex* select,
                               collect_source_t source,
                               Item* item,
                               List<Item>* excludep)
{
    switch (item->type())
    {
    case Item::COND_ITEM:
        {
            Item_cond* cond_item = static_cast<Item_cond*>(item);
            List_iterator<Item> ilist(*cond_item->argument_list());

            while (Item* i = ilist++)
            {
                update_field_infos(pi, select, source, i, excludep);
            }
        }
        break;

    case Item::FIELD_ITEM:
        add_field_info(pi, select, static_cast<Item_field*>(item), excludep);
        break;

    case Item::REF_ITEM:
        {
            if (source != COLLECT_SELECT)
            {
                Item_ref* ref_item = static_cast<Item_ref*>(item);

                add_field_info(pi, select, item, excludep);

                size_t n_items = ref_item->cols();

                for (size_t i = 0; i < n_items; ++i)
                {
                    Item* reffed_item = ref_item->element_index(i);

                    if (reffed_item != ref_item)
                    {
                        update_field_infos(pi, select, source, ref_item->element_index(i), excludep);
                    }
                }
            }
        }
        break;

    case Item::ROW_ITEM:
        {
            Item_row* row_item = static_cast<Item_row*>(item);
            size_t n_items = row_item->cols();

            for (size_t i = 0; i < n_items; ++i)
            {
                update_field_infos(pi, select, source, row_item->element_index(i), excludep);
            }
        }
        break;

    case Item::FUNC_ITEM:
    case Item::SUM_FUNC_ITEM:
#ifdef WF_SUPPORTED
    case Item::WINDOW_FUNC_ITEM:
#endif
        {
            Item_func_or_sum* func_item = static_cast<Item_func_or_sum*>(item);
            Item** items = func_item->arguments();
            size_t n_items = func_item->argument_count();

            // From comment in Item_func_or_sum(server/sql/item.h) abount the
            // func_name() member function:
            /*
             *  This method is used for debug purposes to print the name of an
             *  item to the debug log. The second use of this method is as
             *  a helper function of print() and error messages, where it is
             *  applicable. To suit both goals it should return a meaningful,
             *  distinguishable and sintactically correct string. This method
             *  should not be used for runtime type identification, use enum
             *  {Sum}Functype and Item_func::functype()/Item_sum::sum_func()
             *  instead.
             *  Added here, to the parent class of both Item_func and Item_sum.
             *
             *  NOTE: for Items inherited from Item_sum, func_name() return part of
             *  function name till first argument (including '(') to make difference in
             *  names for functions with 'distinct' clause and without 'distinct' and
             *  also to make printing of items inherited from Item_sum uniform.
             */
            // However, we have no option but to use it.

            const char* f = func_item->func_name();

            char func_name[strlen(f) + 3 + 1];      // strlen(substring) - strlen(substr) from below.
            strcpy(func_name, f);
            mxb::trim(func_name);   // Sometimes the embedded parser leaves leading and trailing whitespace.

            // Non native functions are surrounded by back-ticks, let's remove them.
            remove_surrounding_back_ticks(func_name);

            char* dot = strchr(func_name, '.');

            if (dot)
            {
                // If there is a dot in the name we assume we have something like
                // db.fn(). We remove the scope, can't return that in pp_sqlite
                ++dot;
                memmove(func_name, dot, strlen(func_name) - (dot - func_name) + 1);
                remove_surrounding_back_ticks(func_name);
            }

            char* parenthesis = strchr(func_name, '(');

            if (parenthesis)
            {
                // The func_name of count in "SELECT count(distinct ...)" is
                // "count(distinct", so we need to strip that away.
                *parenthesis = 0;
            }

            // We want to ignore functions that do not really appear as such in an
            // actual SQL statement. E.g. "SELECT @a" appears as a function "get_user_var".
            std::string final_func_name;
            if (!should_function_be_ignored(pi, func_name, &final_func_name))
            {
                const char* s = func_name;
                if (strcmp(func_name, "%") == 0)
                {
                    // Embedded library silently changes "mod" into "%". We need to check
                    // what it originally was, so that the result agrees with that of
                    // pp_sqlite.
                    const char* s2;
                    size_t l;
                    get_string_and_length(func_item->name, &s2, &l);
                    if (s2 && (strncasecmp(s, "mod", 3) == 0))
                    {
                        strcpy(func_name, "mod");
                    }
                }
                else if (strcmp(func_name, "<=>") == 0)
                {
                    // pp_sqlite does not distinguish between "<=>" and "=", so we
                    // change "<=>" into "=".
                    strcpy(func_name, "=");
                }
                else if (strcasecmp(func_name, "substr") == 0)
                {
                    // Embedded library silently changes "substring" into "substr". We need
                    // to check what it originally was, so that the result agrees with
                    // that of pp_sqlite. We reserved space for this above.
                    const char* s2;
                    size_t l;
                    get_string_and_length(func_item->name, &s2, &l);
                    if (s2 && (strncasecmp(s, "substring", 9) == 0))
                    {
                        strcpy(func_name, "substring");
                    }
                }
                else if (strcasecmp(func_name, "add_time") == 0)
                {
                    // For whatever reason the name of "addtime" is returned as "add_time".
                    strcpy(func_name, "addtime");
                }
                else
                {
                    s = final_func_name.c_str();
                }

                add_function_info(pi, select, s, items, n_items);
            }

            for (size_t i = 0; i < n_items; ++i)
            {
                update_field_infos(pi, select, source, items[i], excludep);
            }
        }
        break;

    case Item::SUBSELECT_ITEM:
        {
            Item_subselect* subselect_item = static_cast<Item_subselect*>(item);
            Parser::FunctionInfo* fi = NULL;
            switch (subselect_item->substype())
            {
            case Item_subselect::IN_SUBS:
                fi = add_function_info(pi, select, "in", 0, 0);
                [[fallthrough]];
            case Item_subselect::ALL_SUBS:
            case Item_subselect::ANY_SUBS:
                {
                    Item_in_subselect* in_subselect_item = static_cast<Item_in_subselect*>(item);

#if (((MYSQL_VERSION_MAJOR == 5)   \
                    && ((MYSQL_VERSION_MINOR > 5)   \
                    || ((MYSQL_VERSION_MINOR == 5) && (MYSQL_VERSION_PATCH >= 48)) \
                                                       ) \
                                                       )   \
                    || (MYSQL_VERSION_MAJOR >= 10) \
                        )
                    if (in_subselect_item->left_expr_orig)
                    {
                        update_field_infos(pi,
                                           select,
                                           source,              // TODO: Might be wrong select.
                                           in_subselect_item->left_expr_orig,
                                           excludep);

                        if (subselect_item->substype() == Item_subselect::IN_SUBS)
                        {
                            Item* item2 = in_subselect_item->left_expr_orig;

                            if (item2->type() == Item::FIELD_ITEM)
                            {
                                add_function_field_usage(pi, select, static_cast<Item_field*>(item2), fi);
                            }
                        }
                    }
                    st_select_lex* ssl = in_subselect_item->get_select_lex();
                    if (ssl)
                    {
                        update_field_infos(pi,
                                           get_lex(pi),
                                           ssl,
                                           excludep);

                        if (subselect_item->substype() == Item_subselect::IN_SUBS)
                        {
                            assert(fi);
                            add_function_field_usage(pi, select, ssl, fi);
                        }
                    }
#else
#pragma message "Figure out what to do with versions < 5.5.48."
#endif
                    // TODO: Anything else that needs to be looked into?
                }
                break;

            case Item_subselect::EXISTS_SUBS:
                {
                    Item_exists_subselect* exists_subselect_item =
                        static_cast<Item_exists_subselect*>(item);

                    st_select_lex* ssl = exists_subselect_item->get_select_lex();
                    if (ssl)
                    {
                        update_field_infos(pi,
                                           get_lex(pi),
                                           ssl,
                                           excludep);
                    }
                }
                break;

            case Item_subselect::SINGLEROW_SUBS:
                {
                    Item_singlerow_subselect* ss_item = static_cast<Item_singlerow_subselect*>(item);
                    st_select_lex* ssl = ss_item->get_select_lex();

                    update_field_infos(pi, get_lex(pi), ssl, excludep);
                }
                break;

            case Item_subselect::UNKNOWN_SUBS:
            default:
                MXB_ERROR("Unknown subselect type: %d", subselect_item->substype());
                break;
            }
        }
        break;

    default:
        if (qcme_item_is_string(item))
        {
            if (::this_thread.options & Parser::OPTION_STRING_AS_FIELD)
            {
                String* s = item->val_str();
                int len = s->length();
                char tmp[len + 1];
                memcpy(tmp, s->ptr(), len);
                tmp[len] = 0;

                add_field_info(pi, nullptr, nullptr, tmp, excludep);
            }
        }
        break;
    }
}

#ifdef CTE_SUPPORTED
static void update_field_infos(parsing_info_t* pi,
                               LEX* lex,
                               st_select_lex_unit* select,
                               List<Item>* excludep)
{
    st_select_lex* s = select->first_select();

    if (s)
    {
        update_field_infos(pi, lex, s, excludep);
    }
}
#endif

static void update_field_infos(parsing_info_t* pi,
                               LEX* lex,
                               st_select_lex* select,
                               List<Item>* excludep)
{
    List_iterator<Item> ilist(select->item_list);

    while (Item* item = ilist++)
    {
        update_field_infos(pi, select, COLLECT_SELECT, item, NULL);
    }

    if (select->group_list.first)
    {
        ORDER* order = select->group_list.first;
        while (order)
        {
            Item* item = *order->item;

            update_field_infos(pi, select, COLLECT_GROUP_BY, item, &select->item_list);

            order = order->next;
        }
    }

    if (select->order_list.first)
    {
        ORDER* order = select->order_list.first;
        while (order)
        {
            Item* item = *order->item;

            update_field_infos(pi, select, COLLECT_ORDER_BY, item, &select->item_list);

            order = order->next;
        }
    }

    if (select->where)
    {
        update_field_infos(pi,
                           select,
                           COLLECT_WHERE,
                           select->where,
                           &select->item_list);
    }

#if defined (COLLECT_HAVING_AS_WELL)
    // A HAVING clause can only refer to fields that already have been
    // mentioned. Consequently, they need not be collected.
    if (select->having)
    {
        update_field_infos(pi,
                           COLLECT_HAVING,
                           select->having,
                           0,
                           &select->item_list);
    }
#endif

    TABLE_LIST* table_list = select->get_table_list();

    if (table_list)
    {
        st_select_lex* sl = table_list->get_single_select();

        if (sl)
        {
            // This is for "SELECT 1 FROM (SELECT ...)"
            update_field_infos(pi, get_lex(pi), sl, excludep);
        }
    }
}

namespace
{

void collect_from_list(set<TABLE_LIST*>& seen, parsing_info_t* pi, SELECT_LEX* select_lex, TABLE_LIST* pList)
{
    if (seen.find(pList) != seen.end())
    {
        return;
    }

    seen.insert(pList);

    if (pList->on_expr)
    {
        update_field_infos(pi, select_lex, COLLECT_SELECT, pList->on_expr, NULL);
    }

    if (pList->next_global)
    {
        collect_from_list(seen, pi, select_lex, pList->next_global);
    }

    if (pList->next_local)
    {
        collect_from_list(seen, pi, select_lex, pList->next_local);
    }

    st_nested_join* pJoin = pList->nested_join;

    if (pJoin)
    {
        List_iterator<TABLE_LIST> it(pJoin->join_list);

        TABLE_LIST* pList2 = it++;

        while (pList2)
        {
            collect_from_list(seen, pi, select_lex, pList2);
            pList2 = it++;
        }
    }
}

}

namespace
{

void add_value_func_item(parsing_info_t* pi, Item_func* func_item)
{
    const char* func_name = func_item->func_name();
    std::string final_func_name;

    if (!should_function_be_ignored(pi, func_name, &final_func_name))
    {
        Item** arguments = func_item->arguments();
        auto argument_count = func_item->argument_count();

        for (size_t i = 0; i < argument_count; ++i)
        {
            Item* argument = arguments[i];

            switch (argument->type())
            {
            case Item::FIELD_ITEM:
                {
                    Item_field* field_argument = static_cast<Item_field*>(argument);

                    add_field_info(pi, nullptr, field_argument, nullptr);
                }
                break;

            case Item::FUNC_ITEM:
                add_value_func_item(pi, static_cast<Item_func*>(argument));
                break;

            default:
                break;
            }
        }

        add_function_info(pi, nullptr, final_func_name.c_str(),
                          arguments, argument_count);
    }
}

}

int32_t pp_mysql_get_field_info(const Parser::Helper& helper,
                                const GWBUF* buf, const Parser::FieldInfo** infos, uint32_t* n_infos)
{
    *infos = NULL;
    *n_infos = 0;

    if (!buf)
    {
        return PP_RESULT_OK;
    }

    if (!ensure_query_is_parsed(helper, buf))
    {
        return PP_RESULT_ERROR;
    }

    parsing_info_t* pi = get_pinfo(buf);
    mxb_assert(pi);

    if (!pi->field_infos)
    {
        LEX* lex = get_lex(buf);
        mxb_assert(lex);

        if (!lex)
        {
            return PP_RESULT_ERROR;
        }

        if (lex->describe || is_show_command(lex->sql_command))
        {
            *infos = NULL;
            *n_infos = 0;
            return PP_RESULT_OK;
        }

        SELECT_LEX* select_lex = qcme_get_first_select_lex(lex);
        lex->current_select = select_lex;

        update_field_infos(pi, lex, select_lex, NULL);

        set<TABLE_LIST*> seen;

        if (lex->query_tables)
        {
            collect_from_list(seen, pi, select_lex, lex->query_tables);
        }

        List_iterator<TABLE_LIST> it1(select_lex->top_join_list);

        TABLE_LIST* pList = it1++;

        while (pList)
        {
            collect_from_list(seen, pi, select_lex, pList);
            pList = it1++;
        }

        List_iterator<TABLE_LIST> it2(select_lex->sj_nests);

        /*TABLE_LIST**/ pList = it2++;

        while (pList)
        {
            collect_from_list(seen, pi, select_lex, pList);
            pList = it2++;
        }

        Parser::FunctionInfo* fi = NULL;

        if ((lex->sql_command == SQLCOM_UPDATE) || (lex->sql_command == SQLCOM_UPDATE_MULTI))
        {
            List_iterator<Item> ilist(lex->current_select->item_list);
            Item* item = ilist++;

            fi = get_function_info(pi, "=");

            while (item)
            {
                update_field_infos(pi, lex->current_select, COLLECT_SELECT, item, NULL);

                if (item->type() == Item::FIELD_ITEM)
                {
                    add_function_field_usage(pi, lex->current_select, static_cast<Item_field*>(item), fi);
                }

                item = ilist++;
            }
        }

#ifdef CTE_SUPPORTED
        if (lex->with_clauses_list)
        {
            With_clause* with_clause = lex->with_clauses_list;

            while (with_clause)
            {
                SQL_I_List<With_element>& with_list = with_clause->with_list;
                With_element* element = with_list.first;

                while (element)
                {
                    update_field_infos(pi, lex, element->spec, NULL);

                    if (element->is_recursive && element->first_recursive)
                    {
                        update_field_infos(pi, lex, element->first_recursive, NULL);
                    }

                    element = element->next;
                }

                with_clause = with_clause->next_with_clause;
            }
        }
#endif

        List_iterator<Item> ilist(lex->value_list);
        while (Item* item = ilist++)
        {
            update_field_infos(pi, lex->current_select, COLLECT_SELECT, item, NULL);

            if (fi)
            {
                if (item->type() == Item::FIELD_ITEM)
                {
                    add_function_field_usage(pi, lex->current_select, static_cast<Item_field*>(item), fi);
                }
            }
        }

        if ((lex->sql_command == SQLCOM_INSERT)
            || (lex->sql_command == SQLCOM_INSERT_SELECT)
            || (lex->sql_command == SQLCOM_REPLACE)
            || (lex->sql_command == SQLCOM_REPLACE_SELECT))
        {
            List_iterator<Item> ilist2(lex->field_list);
            Item* item = ilist2++;

            if (item)
            {
                while (item)
                {
                    update_field_infos(pi, lex->current_select, COLLECT_SELECT, item, NULL);

                    item = ilist2++;
                }
            }

            // The following will dig out "a" from a statement like "INSERT INTO t1 VALUES (a+2)"
            List_iterator<List_item> it_many_values(lex->many_values);
            List_item* list_item = it_many_values++;

            while (list_item)
            {
                List_iterator<Item> it_list_item(*list_item);
                Item* item2 = it_list_item++;

                while (item2)
                {
                    if (item2->type() == Item::FUNC_ITEM)
                    {
                        add_value_func_item(pi, static_cast<Item_func*>(item2));
                    }

                    item2 = it_list_item++;
                }

                list_item = it_many_values++;
            }

            if (lex->insert_list)
            {
                List_iterator<Item> ilist3(*lex->insert_list);
                while (Item* item3 = ilist++)
                {
                    update_field_infos(pi, lex->current_select, COLLECT_SELECT, item3, NULL);
                }
            }
        }

#ifdef CTE_SUPPORTED
        // TODO: Check whether this if can be removed altogether also
        // TODO: when CTE are not supported.
        if (true)
#else
        if (lex->sql_command == SQLCOM_SET_OPTION)
#endif
        {
            if (lex->sql_command == SQLCOM_SET_OPTION)
            {
#if defined (WAY_TO_DOWNCAST_SET_VAR_BASE_EXISTS)
                // The list of set_var_base contains the value of variables.
                // However, the actual type is a derived type of set_var_base
                // and there is no information using which we could do the
                // downcast...
                List_iterator<set_var_base> ilist(lex->var_list);
                while (set_var_base* var = ilist++)
                {
                    // Is set_var_base a set_var, set_var_user, set_var_password
                    // set_var_role
                    ...
                }
#endif
                // ...so, we will simply assume that any nested selects are
                // from statements like "set @a:=(SELECT a from t1)". The
                // code after the closing }.
            }

            st_select_lex* select = lex->all_selects_list;

            while (select)
            {
                if (select->nest_level != 0)    // Not the top-level select.
                {
                    update_field_infos(pi, lex, select, NULL);
                }

                select = select->next_select_in_list();
            }
        }
    }

    *infos = pi->field_infos;
    *n_infos = pi->field_infos_len;

    return PP_RESULT_OK;
}

int32_t pp_mysql_get_function_info(const Parser::Helper& helper,
                                   const GWBUF* buf,
                                   const Parser::FunctionInfo** function_infos,
                                   uint32_t* n_function_infos)
{
    *function_infos = NULL;
    *n_function_infos = 0;

    int32_t rv = PP_RESULT_OK;

    if (buf)
    {
        const Parser::FieldInfo* field_infos;
        uint32_t n_field_infos;

        // We ensure the information has been collected by querying the fields first.
        rv = pp_mysql_get_field_info(helper, buf, &field_infos, &n_field_infos);

        if (rv == PP_RESULT_OK)
        {
            parsing_info_t* pi = get_pinfo(buf);
            mxb_assert(pi);

            *function_infos = pi->function_infos;
            *n_function_infos = pi->function_infos_len;
        }
    }

    return rv;
}

void pp_mysql_set_server_version(uint64_t version)
{
    ::this_thread.version = version;
}

void pp_mysql_get_server_version(uint64_t* version)
{
    *version = ::this_thread.version;
}

namespace
{

// Do not change the order without making corresponding changes to IDX_... below.
const char* server_options[] =
{
    "MariaDB Corporation MaxScale",
    "--no-defaults",
    "--datadir=",
    "--language=",
#if MYSQL_VERSION_MINOR < 3
    // TODO: 10.3 understands neither "--skip-innodb" or "--innodb=OFF", although it should.
    "--skip-innodb",
#endif
    "--default-storage-engine=myisam",
    NULL
};

const int IDX_DATADIR = 2;
const int IDX_LANGUAGE = 3;
const int N_OPTIONS = (sizeof(server_options) / sizeof(server_options[0])) - 1;

const char* server_groups[] =
{
    "embedded",
    "server",
    "server",
    "embedded",
    "server",
    "server",
    NULL
};

const int OPTIONS_DATADIR_SIZE = 10 + PATH_MAX;     // strlen("--datadir=");
const int OPTIONS_LANGUAGE_SIZE = 11 + PATH_MAX;    // strlen("--language=");

char datadir_arg[OPTIONS_DATADIR_SIZE];
char language_arg[OPTIONS_LANGUAGE_SIZE];


void configure_options(const char* datadir, const char* langdir)
{
    int rv;

    rv = snprintf(datadir_arg, OPTIONS_DATADIR_SIZE, "--datadir=%s", datadir);
    mxb_assert(rv < OPTIONS_DATADIR_SIZE);      // Ensured by create_datadir().
    server_options[IDX_DATADIR] = datadir_arg;

    rv = sprintf(language_arg, "--language=%s", langdir);
    mxb_assert(rv < OPTIONS_LANGUAGE_SIZE);     // Ensured by pp_process_init().
    server_options[IDX_LANGUAGE] = language_arg;

    // To prevent warning of unused variable when built in release mode,
    // when mxb_assert() turns into empty statement.
    (void)rv;
}
}

int32_t pp_mysql_setup(Parser::SqlMode sql_mode)
{
    this_unit.sql_mode = sql_mode;

    if (sql_mode == Parser::SqlMode::ORACLE)
    {
        this_unit.function_name_mappings = function_name_mappings_oracle;
    }

    return PP_RESULT_OK;
}

int32_t pp_mysql_process_init(void)
{
    bool inited = false;

    if (strlen(mxs::langdir()) >= PATH_MAX)
    {
        fprintf(stderr, "MaxScale: error: Language path is too long: %s.", mxs::langdir());
    }
    else
    {
        char datadir[PATH_MAX];

        sprintf(datadir, "%s/pp_mysqlembedded_%d%d",
                mxs::process_datadir(), MYSQL_VERSION_MAJOR, MYSQL_VERSION_MINOR);

        if (mxs_mkdir_all(datadir, 0777))
        {
            configure_options(datadir, mxs::langdir());

            int argc = N_OPTIONS;
            char** argv = const_cast<char**>(server_options);
            char** groups = const_cast<char**>(server_groups);

            int rc = mysql_library_init(argc, argv, groups);

            if (rc != 0)
            {
                ::this_thread.sql_mode = this_unit.sql_mode;
                mxb_assert(this_unit.function_name_mappings);
                ::this_thread.function_name_mappings = this_unit.function_name_mappings;

                MXB_ERROR("mysql_library_init() failed. Error code: %d", rc);
            }
            else
            {
#if MYSQL_VERSION_ID >= 100000
                set_malloc_size_cb(NULL);
#endif
                MXB_NOTICE("Query classifier initialized.");
                inited = true;
            }
        }
    }

    return inited ? PP_RESULT_OK : PP_RESULT_ERROR;
}

void pp_mysql_process_end(void)
{
    mysql_library_end();
}

int32_t pp_mysql_thread_init(void)
{
    ::this_thread.sql_mode = this_unit.sql_mode;
    mxb_assert(this_unit.function_name_mappings);
    ::this_thread.function_name_mappings = this_unit.function_name_mappings;

    bool inited = (mysql_thread_init() == 0);

    if (!inited)
    {
        MXB_ERROR("mysql_thread_init() failed.");
    }

    return inited ? PP_RESULT_OK : PP_RESULT_ERROR;
}

void pp_mysql_thread_end(void)
{
    mysql_thread_end();
}

int32_t pp_mysql_get_sql_mode(Parser::SqlMode* sql_mode)
{
    *sql_mode = ::this_thread.sql_mode;
    return PP_RESULT_OK;
}

int32_t pp_mysql_set_sql_mode(Parser::SqlMode sql_mode)
{
    int32_t rv = PP_RESULT_OK;

    switch (sql_mode)
    {
    case Parser::SqlMode::DEFAULT:
        ::this_thread.sql_mode = sql_mode;
        ::this_thread.function_name_mappings = function_name_mappings_default;
        break;

    case Parser::SqlMode::ORACLE:
        ::this_thread.sql_mode = sql_mode;
        ::this_thread.function_name_mappings = function_name_mappings_oracle;
        break;

    default:
        rv = PP_RESULT_ERROR;
    }

    return rv;
}

uint32_t pp_mysql_get_options()
{
    return ::this_thread.options;
}

int32_t pp_mysql_set_options(uint32_t options)
{
    int32_t rv = PP_RESULT_OK;

    if ((options & ~Parser::OPTION_MASK) == 0)
    {
        ::this_thread.options = options;
    }
    else
    {
        rv = PP_RESULT_ERROR;
    }

    return rv;
}

int32_t pp_mysql_get_current_stmt(const char** ppStmt, size_t* pLen)
{
    return PP_RESULT_ERROR;
}

namespace
{

class MysqlParser : public Parser
{
public:
    MysqlParser(const Helper* pHelper);

    Result parse(const GWBUF& stmt, uint32_t collect) const override
    {
        Result result = Parser::Result::INVALID;

        pp_mysql_parse(m_helper, &stmt, collect, &result);

        return result;
    }

    std::string_view get_canonical(const GWBUF& stmt) const override
    {
        std::string_view rv;

        if (ensure_query_is_parsed(m_helper, &stmt))
        {
            auto* pi = get_pinfo(&stmt);

            rv = pi->canonical;
        }

        return rv;
    }

    Parser::DatabaseNames get_database_names(const GWBUF& stmt) const override
    {
        Parser::DatabaseNames names;

        pp_mysql_get_database_names(m_helper, &stmt, &names);

        return names;
    }

    void get_field_info(const GWBUF& stmt, const Parser::FieldInfo** ppInfos, size_t* pnInfos) const override
    {
        uint32_t n = 0;
        pp_mysql_get_field_info(m_helper, &stmt, ppInfos, &n);
        *pnInfos = n;
    }

    void get_function_info(const GWBUF& stmt,
                           const Parser::FunctionInfo** ppInfos,
                           size_t* pnInfos) const override
    {
        uint32_t n = 0;
        pp_mysql_get_function_info(m_helper, &stmt, ppInfos, &n);
        *pnInfos = n;
    }

    Parser::KillInfo get_kill_info(const GWBUF& stmt) const override
    {
        Parser::KillInfo kill;

        pp_mysql_get_kill_info(&stmt, &kill);

        return kill;
    }

    mxs::sql::OpCode get_operation(const GWBUF& stmt) const override
    {
        int32_t op = 0;

        pp_mysql_get_operation(m_helper, &stmt, &op);

        return static_cast<mxs::sql::OpCode>(op);
    }

    uint32_t get_options() const override
    {
        return pp_mysql_get_options();
    }

    GWBUF* get_preparable_stmt(const GWBUF& stmt) const override
    {
        GWBUF* pPreparable_stmt = nullptr;

        pp_mysql_get_preparable_stmt(m_helper, &stmt, &pPreparable_stmt);

        return pPreparable_stmt;
    }

    std::string_view get_prepare_name(const GWBUF& stmt) const override
    {
        std::string_view name;

        pp_mysql_get_prepare_name(m_helper, &stmt, &name);

        return name;
    }

    uint64_t get_server_version() const override
    {
        uint64_t version = 0;
        pp_mysql_get_server_version(&version);

        return version;
    }

    Parser::SqlMode get_sql_mode() const override
    {
        Parser::SqlMode sql_mode;

        pp_mysql_get_sql_mode(&sql_mode);

        return sql_mode;
    }

    Parser::TableNames get_table_names(const GWBUF& stmt) const override
    {
        Parser::TableNames names;

        pp_mysql_get_table_names(m_helper, &stmt, &names);

        return names;
    }

    uint32_t get_trx_type_mask(const GWBUF& stmt) const override
    {
        maxscale::TrxBoundaryParser parser;
        return parser.type_mask_of(m_helper.get_sql(stmt));
    }

    uint32_t get_type_mask(const GWBUF& stmt) const override
    {
        uint32_t type_mask = 0;

        pp_mysql_get_type_mask(m_helper, &stmt, &type_mask);

        return type_mask;
    }

    bool relates_to_previous(const GWBUF& packet) const override
    {
        // TODO: E.g. "SHOW WARNINGS" also relates to previous.
        bool rv = false;

        const mxs::Parser::FunctionInfo* pInfos = nullptr;
        size_t nInfos = 0;
        get_function_info(packet, &pInfos, &nInfos);

        for (size_t i = 0; i < nInfos; ++i)
        {
            if (mxb::sv_case_eq(pInfos[i].name, "FOUND_ROWS"))
            {
                rv = true;
                break;
            }
        }

        return rv;
    }

    bool is_multi_stmt(const GWBUF& stmt) const override
    {
        return maxsimd::is_multi_stmt(helper().get_sql(stmt));
    }

    void set_sql_mode(Parser::SqlMode sql_mode) override
    {
        pp_mysql_set_sql_mode(sql_mode);
    }

    bool set_options(uint32_t options) override
    {
        return pp_mysql_set_options(options) == PP_RESULT_OK;
    }

    void set_server_version(uint64_t version) override
    {
        pp_mysql_set_server_version(version);
    }

    QueryInfo get_query_info(const GWBUF& stmt) const override
    {
        QueryInfo rval = m_helper.get_query_info(stmt);

        if (rval.type_mask_status == mxs::Parser::TypeMaskStatus::NEEDS_PARSING)
        {
            rval.type_mask = get_type_mask(stmt);
            rval.multi_stmt = is_multi_stmt(stmt);
            rval.op = get_operation(stmt);
            rval.relates_to_previous = relates_to_previous(stmt);
        }

        return rval;
    }

};

class MysqlParserPlugin : public ParserPlugin
{
public:
    bool setup(Parser::SqlMode sql_mode) override
    {
        return pp_mysql_setup(sql_mode) == PP_RESULT_OK;
    }

    bool thread_init(void) const override
    {
        return pp_mysql_thread_init() == PP_RESULT_OK;
    }

    void thread_end(void) const override
    {
        pp_mysql_thread_end();
    }

    const Parser::Helper& default_helper() const override
    {
        return MariaDBParser::Helper::get();
    }

    bool get_current_stmt(const char** ppStmt, size_t* pLen) const override
    {
        return pp_mysql_get_current_stmt(ppStmt, pLen) == PP_RESULT_OK;
    }

    Parser::StmtResult get_stmt_result(const GWBUF::ProtocolInfo* info) const override
    {
        // Not supported.
        return Parser::StmtResult {};
    }

    std::string_view get_canonical(const GWBUF::ProtocolInfo* info) const override
    {
        // Not supported.
        return std::string_view {};
    }

    std::unique_ptr<Parser> create_parser(const Parser::Helper* pHelper) const override
    {
        return std::make_unique<MysqlParser>(pHelper);
    }
};

MysqlParserPlugin mysql_plugin;

MysqlParser::MysqlParser(const Helper* pHelper)
    : Parser(&mysql_plugin, pHelper)
{
}

}

/**
 * EXPORTS
 */

extern "C"
{

MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        "pp_mysqlembedded",
        mxs::ModuleType::PARSER,
        mxs::ModuleStatus::GA,
        MXS_PARSER_VERSION,
        "MariaDB SQL parser using MySQL Embedded",
        "V1.0.0",
        MXS_NO_MODULE_CAPABILITIES,
        &mysql_plugin,
        pp_mysql_process_init,
        pp_mysql_process_end,
        pp_mysql_thread_init,
        pp_mysql_thread_end
    };

    return &info;
}
}
