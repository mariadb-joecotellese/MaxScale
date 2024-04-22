/*
 * Copyright (c) 2019 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "user_data.hh"

#include <netinet/in.h>
#include <arpa/inet.h>
#include <mysqld_error.h>
#include <maxbase/format.hh>
#include <maxbase/host.hh>
#include <maxbase/threadpool.hh>
#include <maxsql/mariadb_connector.hh>
#include <maxsql/sqlite_strlike.hh>
#include <maxscale/server.hh>
#include <maxscale/service.hh>
#include <maxscale/protocol/mariadb/module_names.hh>
#include <maxscale/config.hh>
#include <maxscale/cn_strings.hh>
#include <maxscale/secrets.hh>
#include <maxscale/paths.hh>

using std::string;
using std::vector;
using std::move;
using mxq::MariaDB;
using Guard = std::lock_guard<std::mutex>;
using UserEntry = mariadb::UserEntry;
using UserEntryType = mariadb::UserEntryType;
using mariadb::UserSearchSettings;
using mariadb::UserEntryResult;
using ServerType = SERVER::VersionInfo::Type;

namespace
{
constexpr auto acquire = std::memory_order_acquire;
constexpr auto relaxed = std::memory_order_relaxed;
constexpr auto npos = string::npos;

const int ipv4min_len = 7;      // 1.1.1.1
const string mysql_default_auth = "mysql_native_password";
const string info_schema = "information_schema";    // Any user can access this even without a grant.

namespace mariadb_queries
{
const string users_query = "SELECT * FROM mysql.user;";

// Select users/roles with general db-level privs, the db:s may contain wildcards.
const string db_wc_grants_query = "SELECT DISTINCT user, host, db FROM mysql.db;";

const string db_grants_query_old =
    "SELECT DISTINCT * FROM ("
    // Select table level privs counting as db-level privs
    "(SELECT a.user, a.host, a.db FROM mysql.tables_priv AS a) UNION "
    // and combine with column-level privs as db-level privs
    "(SELECT a.user, a.host, a.db FROM mysql.columns_priv AS a) ) AS c;";

// The query above does not check the procs_priv-table. To avoid requiring new privileges in existing
// installations, keep the existing query as an alternative. The old query can be removed in 6.
const string db_grants_query =
    "SELECT DISTINCT * FROM ("
    // Select table level privs counting as db-level privs
    "(SELECT a.user, a.host, a.db FROM mysql.tables_priv AS a) UNION "
    // and combine with column-level privs as db-level privs
    "(SELECT a.user, a.host, a.db FROM mysql.columns_priv AS a) UNION "
    // and combine with procedure-level privs as db-level privs.
    "(SELECT a.user, a.host, a.db FROM mysql.procs_priv AS a) ) AS c;";

const string proxies_query = "SELECT DISTINCT a.user, a.host FROM mysql.proxies_priv AS a "
                             "WHERE a.proxied_host <> '' AND a.proxied_user <> '';";
const string db_names_query = "SHOW DATABASES;";
const string roles_query = "SELECT a.user, a.host, a.role FROM mysql.roles_mapping AS a;";
const string my_grants_query = "SHOW GRANTS;";
const string current_user_query = "SELECT current_user();";
}

namespace xpand_queries
{
const string users_query = "SELECT * FROM system.users;";
const string db_grants_query = "SELECT u.username, u.host, a.dbname, a.privileges FROM system.user_acl AS a "
                               "LEFT JOIN system.users AS u ON (u.user = a.role);";
}

bool looks_like_ipv4(const std::string& addr)
{
    const char* ptr = addr.c_str();

    while (isdigit(*ptr))
    {
        ++ptr;
    }

    return ptr != addr.c_str() && *ptr == '.';
}

bool impossible_hostname(const std::string& addr)
{
    const char* ptr = addr.c_str();

    while (isalnum(*ptr) || *ptr == '.' || *ptr == '-' || *ptr == '%' || *ptr == '_')
    {
        ++ptr;
    }

    return *ptr != 0;
}
}

MariaDBUserManager::MariaDBUserManager()
{
    m_userdb = std::make_shared<UserDatabase>();    // Must never be null
}

std::string MariaDBUserManager::protocol_name() const
{
    return MXS_MARIADB_PROTOCOL_NAME;
}

bool MariaDBUserManager::update_users()
{
    LoadSettings sett = get_load_settings();

    auto temp_userdata = std::make_unique<UserDatabase>();
    UserLoadRes res1;
    UserLoadRes res2;
    bool file_enabled = !sett.users_file_path.empty();

    if (file_enabled && sett.users_file_usage == UsersFileUsage::FILE_ONLY_ALWAYS)
    {
        res1 = load_users_from_file(sett.users_file_path, *temp_userdata);
    }
    else
    {
        res1 = load_users_from_backends(std::move(sett.conn_user),
                                        std::move(sett.conn_pw), std::move(sett.conn_prev_pw),
                                        std::move(sett.backends), *temp_userdata);
        if (file_enabled && sett.users_file_usage == UsersFileUsage::ADD_WHEN_LOAD_OK && res1.success)
        {
            res2 = load_users_from_file(sett.users_file_path, *temp_userdata);
        }
    }

    if (res1.success)
    {
        auto build_msg = [this, &res1, &res2](){
            string rval;
            if (res2.success)
            {
                rval = mxb::string_printf("Read %s for service '%s'. In addition, read %s.",
                                          res1.msg.c_str(), svc_name(), res2.msg.c_str());
            }
            else
            {
                rval = mxb::string_printf("Read %s for service '%s'.",
                                          res1.msg.c_str(), svc_name());
            }
            return rval;
        };

        // Got some data. Update the master database if the contents differ. Usually they don't.

        // This comparison is not trivially cheap if there are many user entries,
        // but it avoids unnecessary user cache updates. The updates are now very cheap (copy of shared_ptr)
        // so this comparison could be removed. With 10k users the comparison takes ~4ms.
        // TODO: think if some kind of checksum could be used instead
        if (temp_userdata->equal_contents(*m_userdb))
        {
            MXB_INFO("%s The data was identical to existing user data.", build_msg().c_str());
        }
        else
        {
            // Data changed, update main user db. Cache update message is sent by the caller.
            {
                Guard guard(m_userdb_lock);
                m_userdb = move(temp_userdata);
                m_userdb_version++;
            }
            MXB_NOTICE("%s", build_msg().c_str());
        }
    }
    return res1.success;
}

MariaDBUserManager::UserLoadRes
MariaDBUserManager::load_users_from_backends(string&& conn_user, string&& conn_pw, string&& conn_prev_pw,
                                             std::vector<SERVER*>&& backends, UserDatabase& temp_userdata)
{
    mxq::MariaDB con;
    auto& sett = con.connection_settings();
    sett.user = move(conn_user);
    sett.password = mxs::decrypt_password(conn_pw);
    sett.alternate_password = mxs::decrypt_password(conn_prev_pw);
    sett.clear_sql_mode = true;
    sett.charset = "utf8mb4";

    mxs::Config& glob_config = mxs::Config::get();
    sett.timeout = glob_config.auth_conn_timeout.get().count();
    auto& local_address = glob_config.local_address;
    if (!local_address.empty())
    {
        sett.local_address = local_address;
    }
    const bool union_over_bes = union_over_backends();

    // Filter out unusable backends.
    auto is_unusable = [](const SERVER* srv) {
        return !srv->active() || !srv->is_usable();
    };
    auto erase_iter = std::remove_if(backends.begin(), backends.end(), is_unusable);
    backends.erase(erase_iter, backends.end());
    if (backends.empty() && m_warn_no_servers.load(relaxed))
    {
        MXB_ERROR("No valid servers from which to query MariaDB user accounts found.");
    }

    // Order backends so that the master is checked first.
    auto compare = [](const SERVER* lhs, const SERVER* rhs) {
        return (lhs->is_master() && !rhs->is_master())
               || (lhs->is_slave() && (!rhs->is_master() && !rhs->is_slave()));
    };
    std::sort(backends.begin(), backends.end(), compare);

    bool got_data = false;
    std::vector<string> source_servernames;
    const char users_query_failed[] = "Failed to query server '%s' for user account info. %s";

    for (auto srv : backends)
    {
        // Different backends may have different ssl settings so need to update.
        sett.ssl = srv->ssl_config();
        if (srv->proxy_protocol())
        {
            con.set_local_text_proxy_header();
        }

        if (con.open_extra(srv->address(), srv->port(), srv->extra_port()))
        {
            auto load_result = LoadResult::QUERY_FAILED;

            // If server version is unknown (no monitor), update its version info.
            auto& srv_info = srv->info();
            if (srv_info.type() == ServerType::UNKNOWN)
            {
                auto new_info = con.version_info();
                srv->set_version(SERVER::BaseType::MARIADB, new_info.version, new_info.info,
                                 new_info.capabilities);
            }

            switch (srv_info.type())
            {
            case ServerType::MYSQL:
            case ServerType::MARIADB:
                load_result = load_users_mariadb(con, srv, &temp_userdata);
                break;

            case ServerType::XPAND:
                load_result = load_users_xpand(con, srv, &temp_userdata);
                break;

            case ServerType::UNKNOWN:
            case ServerType::BLR:
                // Cannot query these types.
                break;

            case ServerType::POSTGRESQL:
                mxb_assert(!true);      // Something is very wrong.
                break;
            }

            switch (load_result)
            {
            case LoadResult::SUCCESS:
                // Print successes after iteration is complete.
                source_servernames.emplace_back(srv->name());
                got_data = true;
                if (m_check_showdb_priv)
                {
                    check_show_dbs_priv(con, temp_userdata, srv_info.type(), srv->name());
                }
                break;

            case LoadResult::QUERY_FAILED:
                MXB_ERROR(users_query_failed, srv->name(), con.error());
                break;

            case LoadResult::INVALID_DATA:
                MXB_ERROR("Received invalid data from '%s' when querying user accounts.", srv->name());
                break;
            }

            if (got_data && !union_over_bes)
            {
                break;
            }
        }
        else
        {
            MXB_ERROR(users_query_failed, srv->name(), con.error());
        }
    }

    UserLoadRes rval;
    if (got_data)
    {
        rval.success = true;
        string datasource = mxb::create_list_string(source_servernames, ", ", " and ", "'");
        rval.msg = mxb::string_printf("%lu user@host entries from %s",
                                      temp_userdata.n_entries(), datasource.c_str());
    }
    return rval;
}

MariaDBUserManager::LoadResult
MariaDBUserManager::load_users_mariadb(mxq::MariaDB& con, SERVER* srv, UserDatabase* output)
{
    // Roles were added in server 10.0.5, default roles in server 10.1.1. Strictly speaking, reading the
    // roles_mapping table for 10.0.5 is not required as they won't be used. Read anyway in case
    // diagnostics prints it.
    auto& info = srv->info();
    bool role_support = (info.version_num().total >= 100005);

    // Run the queries as one multiquery.
    vector<string> multiquery;
    multiquery.reserve(6);
    multiquery = {mariadb_queries::users_query,     mariadb_queries::db_wc_grants_query,
                  mariadb_queries::db_grants_query, mariadb_queries::proxies_query,
                  mariadb_queries::db_names_query};
    if (role_support)
    {
        multiquery.push_back(mariadb_queries::roles_query);
    }

    auto rval = LoadResult::QUERY_FAILED;
    auto multiq_result = con.multiquery(multiquery);
    if (multiq_result.empty())
    {
        // If the error indicates insufficient privileges, try again with the old db-grants query.
        auto errornum = con.errornum();
        if (errornum == ER_TABLEACCESS_DENIED_ERROR || errornum == ER_COLUMNACCESS_DENIED_ERROR)
        {
            const char msg_fmt[] = "Using old user account query due to insufficient privileges. "
                                   "To avoid this warning, give the service user of '%s' access to "
                                   "the 'mysql.procs_priv'-table.";
            MXB_WARNING(msg_fmt, svc_name());

            multiquery[2] = mariadb_queries::db_grants_query_old;
            multiq_result = con.multiquery(multiquery);
        }
    }

    if (!multiq_result.empty())
    {
        QResult users_res = move(multiq_result[0]);
        QResult db_wc_grants_res = move(multiq_result[1]);
        QResult db_grants_res = move(multiq_result[2]);
        QResult proxies_res = move(multiq_result[3]);
        QResult dbs_res = move(multiq_result[4]);
        QResult roles_res = role_support ? move(multiq_result[5]) : nullptr;

        rval = LoadResult::INVALID_DATA;
        if (read_users_mariadb(move(users_res), info, output))
        {
            read_dbs_and_roles_mariadb(move(db_wc_grants_res), move(db_grants_res), move(roles_res), output);
            read_proxy_grants(move(proxies_res), output);
            read_databases(move(dbs_res), output);
            rval = LoadResult::SUCCESS;
        }
    }
    return rval;
}

MariaDBUserManager::LoadResult
MariaDBUserManager::load_users_xpand(mxq::MariaDB& con, SERVER* srv, UserDatabase* output)
{
    using std::move;
    vector<string> multiquery = {xpand_queries::users_query, xpand_queries::db_grants_query,
                                 mariadb_queries::db_names_query};
    auto rval = LoadResult::QUERY_FAILED;
    auto multiq_result = con.multiquery(multiquery);
    if (multiq_result.size() == multiquery.size())
    {
        QResult users_res = move(multiq_result[0]);
        QResult acl_res = move(multiq_result[1]);
        QResult dbs_res = move(multiq_result[2]);

        rval = LoadResult::INVALID_DATA;
        if (read_users_xpand(move(users_res), output))
        {
            read_db_privs_xpand(move(acl_res), output);
            read_databases(move(dbs_res), output);
            rval = LoadResult::SUCCESS;
        }
    }
    return rval;
}

/**
 * Read user fetch results from MariaDB or MySQL server. Xpand is handled by a different function.
 *
 * @param users Results from query
 * @param srv_info Server version info
 * @param output Results storage object
 * @return True on success
 */
bool MariaDBUserManager::read_users_mariadb(QResult users, const SERVER::VersionInfo& srv_info,
                                            UserDatabase* output)
{
    auto get_bool_enum = [&users](int64_t col_ind) {
        string val = users->get_string(col_ind);
        return val == "Y" || val == "y";
    };

    // MySQL-server 5.7 and later do not have a "Password"-column. The pw is in the
    // "authentication_string"-column.
    bool have_pw_column = srv_info.type() == ServerType::MARIADB || srv_info.version_num().total < 50700;

    // Get column indexes for the interesting fields. Depending on backend version, they may not all
    // exist. Some of the field name start with a capital and some don't. Should the index search be
    // ignorecase?
    auto ind_user = users->get_col_index("User");
    auto ind_host = users->get_col_index("Host");
    auto ind_sel_priv = users->get_col_index("Select_priv");
    auto ind_ins_priv = users->get_col_index("Insert_priv");
    auto ind_upd_priv = users->get_col_index("Update_priv");
    auto ind_del_priv = users->get_col_index("Delete_priv");
    auto ind_super_priv = users->get_col_index("Super_priv");
    auto ind_ssl = users->get_col_index("ssl_type");
    auto ind_plugin = users->get_col_index("plugin");
    auto ind_pw = users->get_col_index("Password");
    auto ind_auth_str = users->get_col_index("authentication_string");
    auto ind_is_role = users->get_col_index("is_role");
    auto ind_def_role = users->get_col_index("default_role");

    bool has_required_fields = (ind_user >= 0) && (ind_host >= 0)
        && (ind_sel_priv >= 0) && (ind_ins_priv >= 0) && (ind_upd_priv >= 0) && (ind_del_priv >= 0)
        && (ind_super_priv >= 0) && (ind_ssl >= 0) && (ind_plugin >= 0) && (!have_pw_column || ind_pw >= 0)
        && (ind_auth_str >= 0);

    if (has_required_fields)
    {
        while (users->next_row())
        {
            UserEntry new_entry;
            new_entry.username = users->get_string(ind_user);
            new_entry.host_pattern = users->get_string(ind_host);

            // Treat the user as having global privileges if any of the following global privileges
            // exists.
            new_entry.global_db_priv = get_bool_enum(ind_sel_priv) || get_bool_enum(ind_ins_priv)
                || get_bool_enum(ind_upd_priv) || get_bool_enum(ind_del_priv);

            new_entry.super_priv = get_bool_enum(ind_super_priv);

            // Require SSL if the entry is not empty.
            new_entry.ssl = !users->get_string(ind_ssl).empty();

            new_entry.plugin = mxb::tolower(users->get_string(ind_plugin));
            new_entry.password = have_pw_column ? users->get_string(ind_pw) : users->get_string(ind_auth_str);

            // Hex-form passwords have a '*' at the beginning, remove it.
            remove_star(new_entry.password);

            new_entry.auth_string = users->get_string(ind_auth_str);

            if (ind_is_role >= 0)
            {
                new_entry.is_role = get_bool_enum(ind_is_role);
            }
            if (ind_def_role >= 0)
            {
                new_entry.default_role = users->get_string(ind_def_role);
            }

            output->add_entry(std::move(new_entry));
        }
    }
    return has_required_fields;
}

void MariaDBUserManager::read_dbs_and_roles_mariadb(QResult db_wc_grants, QResult db_grants, QResult roles,
                                                    UserDatabase* output)
{
    using StringSetMap = UserDatabase::StringSetMap;

    auto map_builder = [](const string& grant_col_name, QResult source, bool strip_escape) {
        StringSetMap result;
        auto ind_user = source->get_col_index("user");
        auto ind_host = source->get_col_index("host");
        auto ind_grant = source->get_col_index(grant_col_name);
        bool valid_data = (ind_user >= 0 && ind_host >= 0 && ind_grant >= 0);
        if (valid_data)
        {
            while (source->next_row())
            {
                string grant = source->get_string(ind_grant);
                if (strip_escape)
                {
                    mxb::strip_escape_chars(grant);
                }
                string key = UserDatabase::form_db_mapping_key(source->get_string(ind_user),
                                                               source->get_string(ind_host));
                result[key].insert(grant);
            }
        }
        return result;
    };

    StringSetMap db_wc_grants_map = map_builder("db", std::move(db_wc_grants), false);
    StringSetMap db_grants_map = map_builder("db", std::move(db_grants), strip_db_esc());
    output->add_db_grants(move(db_wc_grants_map), move(db_grants_map));

    if (roles)
    {
        // Old backends may not have role data.
        StringSetMap role_mapping = map_builder("role", std::move(roles), false);
        output->add_role_mapping(move(role_mapping));
    }
}

void MariaDBUserManager::read_proxy_grants(MariaDBUserManager::QResult proxies, UserDatabase* output)
{
    if (proxies->get_row_count() > 0)
    {
        auto ind_user = proxies->get_col_index("user");
        auto ind_host = proxies->get_col_index("host");
        if (ind_user >= 0 && ind_host >= 0)
        {
            while (proxies->next_row())
            {
                auto entry = output->find_mutable_entry_equal(proxies->get_string(ind_user),
                                                              proxies->get_string(ind_host));
                if (entry)
                {
                    entry->proxy_priv = true;
                }
            }
        }
    }
}

void MariaDBUserManager::read_databases(MariaDBUserManager::QResult dbs, UserDatabase* output)
{
    // Should just have one column.
    if (dbs->get_col_count() == 1)
    {
        UserDatabase::StringSet db_names;
        while (dbs->next_row())
        {
            output->add_database_name(dbs->get_string(0));
        }
    }
}

bool MariaDBUserManager::read_users_xpand(QResult users, UserDatabase* output)
{
    // Xpand users are listed different from MariaDB/MySQL. The users-table does not have privilege
    // information and may have multiple rows for the same username&host. Multiple rows with the same
    // username&host need to be combined with the matching rows in the user_acl-table (with the
    // "user"-column) to get all database grants for a given user account. Also, privileges are coded into
    // a bitfield.

    // First, go through the system.users-table and add users. An empty password is overwritten by a
    // non-empty password, but not the other way around.
    auto ind_user = users->get_col_index("username");
    auto ind_host = users->get_col_index("host");
    auto ind_pw = users->get_col_index("password");
    auto ind_plugin = users->get_col_index("plugin");
    bool has_required_fields = (ind_user >= 0) && (ind_host >= 0) && (ind_pw >= 0) && (ind_plugin >= 0);

    if (has_required_fields)
    {
        while (users->next_row())
        {
            auto username = users->get_string(ind_user);
            auto host = users->get_string(ind_host);
            auto pw = users->get_string(ind_pw);

            // Hex-form passwords may have a '*' at the beginning, remove it.
            remove_star(pw);

            auto existing_entry = output->find_mutable_entry_equal(username, host);
            if (existing_entry)
            {
                // Entry exists, but password may be empty due to how Xpand handles user data.
                if (existing_entry->password.empty() && !pw.empty())
                {
                    existing_entry->password = pw;
                }
            }
            else
            {
                // New entry, insert it.
                UserEntry new_entry;
                new_entry.username = username;
                new_entry.host_pattern = host;
                new_entry.password = pw;
                new_entry.plugin = users->get_string(ind_plugin);
                new_entry.global_db_priv = true;    // TODO: Fix later!
                output->add_entry(std::move(new_entry));
            }
        }
    }

    return has_required_fields;
}

void MariaDBUserManager::read_db_privs_xpand(QResult acl, UserDatabase* output)
{
    auto ind_user = acl->get_col_index("username");
    auto ind_host = acl->get_col_index("host");
    auto ind_dbname = acl->get_col_index("dbname");
    auto ind_privs = acl->get_col_index("privileges");
    bool have_required_fields = (ind_user >= 0) && (ind_host >= 0) && (ind_dbname >= 0) && (ind_privs >= 0);
    bool strip_db_escape = strip_db_esc();

    if (have_required_fields)
    {
        UserDatabase::StringSetMap result;
        while (acl->next_row())
        {
            // Have two types of rows: global rows and db/table/column-specific rows. Global rows affect
            // the main user entry, others add to the database grants set.
            auto user = acl->get_string(ind_user);
            auto host = acl->get_string(ind_host);
            auto dbname = acl->get_string(ind_dbname);
            auto privs = acl->get_uint(ind_privs);

            if (dbname.empty())
            {
                // Global privilege. Add it to a matching user in the main user info container.
                auto existing_entry = output->find_mutable_entry_equal(user, host);
                if (existing_entry)
                {
                    const uint64_t sel_priv = 1u << 20u;        // 1048576
                    const uint64_t insert_priv = 1u << 13u;     // 8192
                    const uint64_t update_priv = 1u << 25u;     // 33554432
                    if (privs & (sel_priv | insert_priv | update_priv))
                    {
                        existing_entry->global_db_priv = true;
                    }
                }
            }
            else
            {
                if (strip_db_escape)
                {
                    mxb::strip_escape_chars(dbname);
                }
                string key = UserDatabase::form_db_mapping_key(user, host);
                result[key].insert(dbname);
            }
        }
    }
}

std::unique_ptr<mxs::UserAccountCache> MariaDBUserManager::create_user_account_cache()
{
    auto cache = std::make_unique<MariaDBUserCache>(*this);
    cache->update_from_master();
    return cache;
}

MariaDBUserManager::UserDBInfo MariaDBUserManager::get_user_database() const
{
    UserDBInfo rval;
    {
        // A lock is needed to ensure both the db and version number are from the same update.
        Guard guard(m_userdb_lock);
        rval.user_db = m_userdb;
        rval.version = m_userdb_version.load(relaxed);
    }
    return rval;
}

int MariaDBUserManager::userdb_version() const
{
    return m_userdb_version.load(acquire);
}

json_t* MariaDBUserManager::users_to_json() const
{
    SUserDB ptr_copy;
    {
        Guard guard(m_userdb_lock);
        ptr_copy = m_userdb;
    }
    return ptr_copy->users_to_json();
}

/**
 * Check if current user can see all databases. Needs either a "show databases"-grant or a global privilege
 * such as "SELECT ON *.*".
 *
 * @param con Connection to use
 * @param userdata Fetched user account data
 * @param type Server type
 * @param servername Servername, for logging
 */
void MariaDBUserManager::check_show_dbs_priv(mxq::MariaDB& con, const UserDatabase& userdata,
                                             SERVER::VersionInfo::Type type, const char* servername)
{
    const char invalid_data_fmt[] = "Received invalid data from '%s' to query '%s'.";
    vector<string> queries = {mariadb_queries::my_grants_query, mariadb_queries::current_user_query};
    auto results = con.multiquery(queries);
    if (results.size() != 2)
    {
        MXB_ERROR("Failed to query server '%s' for current user grants. %s", servername, con.error());
    }
    else
    {
        bool grant_found = false;
        bool invalid_data = false;
        {
            auto& res = results[0];
            if (res->get_col_count() == 1)
            {
                while (res->next_row())
                {
                    string grant = res->get_string(0);
                    if (grant.find("SHOW DATABASES") != string::npos)
                    {
                        grant_found = true;
                        break;
                    }
                }
            }
            else
            {
                MXB_ERROR(invalid_data_fmt, servername, queries[0].c_str());
                invalid_data = true;
            }
        }

        if (!invalid_data && !grant_found)
        {
            auto& res = results[1];
            if (res->get_col_count() == 1 && res->next_row())
            {
                string userhost = res->get_string(0);
                auto pos = userhost.find('@');
                if (pos != string::npos && pos < userhost.length() - 1)
                {
                    string username = userhost.substr(0, pos);
                    string hostpattern = userhost.substr(pos + 1);
                    if (type == SERVER::VersionInfo::Type::XPAND)
                    {
                        // The username and host pattern may be quoted on Xpand.
                        auto remove_quotes = [](string& str){
                            if (str.length() >= 2 && str[0] == '\'' && str.back() == '\'')
                            {
                                str.pop_back();
                                str.erase(0, 1);
                            }
                        };
                        remove_quotes(username);
                        remove_quotes(hostpattern);
                    }

                    auto my_entry = userdata.find_entry_equal(username, hostpattern);
                    if (my_entry && my_entry->global_db_priv)
                    {
                        grant_found = true;
                    }
                }
            }
            else
            {
                MXB_ERROR(invalid_data_fmt, servername, queries[1].c_str());
                invalid_data = true;
            }
        }

        if (grant_found)
        {
            m_check_showdb_priv = false;    // Assume that the privilege is never lost.
        }
        else if (!invalid_data)
        {
            // This will be printed repeatedly until admin adds the priv.
            const char msg[] = "Service user '%s' of service '%s' does not have 'SHOW DATABASES' or "
                               "a similar global privilege on '%s'. This may cause authentication errors on "
                               "clients logging in to a specific database.";
            MXB_WARNING(msg, con.connection_settings().user.c_str(), svc_name(), servername);
        }
    }
}

MariaDBUserManager::UserLoadRes
MariaDBUserManager::load_users_from_file(const string& src, UserDatabase& output)
{
    using mxb::Json;
    auto filepathc = src.c_str();

    auto read_str_if_exists = [filepathc](const Json& source, const char* key,
                                          const string& user, const string& host, string* out) {
        bool rval = true;
        if (source.contains(key))
        {
            if (!source.try_get_string(key, out))
            {
                MXB_ERROR("File '%s' contains field '%s' for user '%s'@'%s', but it is not a string.",
                          filepathc, key, user.c_str(), host.c_str());
                rval = false;
            }
        }
        return rval;
    };

    auto read_bool_if_exists = [filepathc](const Json& source, const char* key,
                                           const string& user, const string& host, bool* out) {
        bool rval = true;
        if (source.contains(key))
        {
            if (!source.try_get_bool(key, out))
            {
                MXB_ERROR("File '%s' contains field '%s' for user '%s'@'%s', but it is not a boolean.",
                          filepathc, key, user.c_str(), host.c_str());
                rval = false;
            }
        }
        return rval;
    };

    using EntryHandler = std::function<void (Json& elem, int ind)>;
    auto process_array = [filepathc](Json& all, const char* arr_obj_name, const EntryHandler& handler) {
        auto arr = all.get_array_elems(arr_obj_name);
        if (all.ok())
        {
            int ind = 0;
            for (auto& elem : arr)
            {
                handler(elem, ind);
                ind++;
            }
        }
        else
        {
            MXB_ERROR("Wrong object type in '%s': %s", filepathc, all.error_msg().c_str());
        }
    };

    UserLoadRes rval;
    Json all;
    if (all.load(src))
    {
        rval.success = true;
        int n_users = -1;
        int n_grants = -1;
        int n_roles = -1;

        const char grp_user[] = "user";
        if (all.contains(grp_user))
        {
            n_users = 0;
            EntryHandler user_handler = [&](Json& user_data, int ind) {
                // The user definition must contain at least 'user' and 'host' fields.
                string uname = user_data.get_string("user");
                string host = user_data.get_string("host");

                if (user_data.ok())
                {
                    auto read_str = [&user_data, &uname, &host, &read_str_if_exists]
                        (const char* key, string* out) {
                        return read_str_if_exists(user_data, key, uname, host, out);
                    };
                    auto read_bool = [&user_data, &uname, &host, &read_bool_if_exists]
                        (const char* key, bool* out) {
                        return read_bool_if_exists(user_data, key, uname, host, out);
                    };

                    mariadb::UserEntry new_entry;
                    new_entry.username = uname;
                    new_entry.host_pattern = host;

                    bool strings_ok = (read_str("password", &new_entry.password)
                        && read_str("plugin", &new_entry.plugin)
                        && read_str("authentication_string", &new_entry.auth_string)
                        && read_str("default_role", &new_entry.default_role));
                    // TODO: add "ssl"-field read once it is actually used for something.
                    bool booleans_ok = (read_bool("super_priv", &new_entry.super_priv)
                        && read_bool("global_db_priv", &new_entry.global_db_priv)
                        && read_bool("proxy_priv", &new_entry.proxy_priv)
                        && read_bool("is_role", &new_entry.is_role));

                    if (strings_ok && booleans_ok)
                    {
                        // Erase * from password if found. This is similar to mysql.user.
                        remove_star(new_entry.password);
                        output.add_entry(move(new_entry));
                        n_users++;
                    }
                }
                else
                {
                    MXB_ERROR("User entry %i in '%s'-array in file '%s' is missing a required field: %s",
                              ind + 1, grp_user, filepathc, user_data.error_msg().c_str());
                }
            };
            process_array(all, grp_user, user_handler);
        }

        const char grp_db[] = "db";
        if (all.contains(grp_db))
        {
            UserDatabase::StringSetMap db_grants_temp;
            n_grants = 0;
            EntryHandler grant_handler = [&](Json& grant_data, int ind) {
                // The grant or definition must contain 'user', 'host' and 'db'-fields.
                string uname = grant_data.get_string("user");
                string host = grant_data.get_string("host");
                string db = grant_data.get_string("db");

                if (grant_data.ok())
                {
                    string key = UserDatabase::form_db_mapping_key(uname, host);
                    db_grants_temp[key].insert(db);
                    n_grants++;

                    // The database defined in the grant will not exist if using "file-only-always"-mode.
                    // Add it so that client won't get an "Unknown database"-error. If using
                    // "add_when_load_ok"-mode, this should not have any effect as the entry should
                    // exist. If it doesn't, then it's the user's problem.
                    output.add_database_name(db);
                }
                else
                {
                    MXB_ERROR("Database grant entry %i in '%s'-array in file '%s' is missing a required "
                              "field: %s", ind + 1, grp_db, filepathc, grant_data.error_msg().c_str());
                }
            };
            process_array(all, grp_db, grant_handler);
            // Add all the db grants as wildcard grants, as we cannot know which type it is.
            UserDatabase::StringSetMap dummy;
            output.add_db_grants(move(db_grants_temp), move(dummy));
        }

        const char grp_roles_mapping[] = "roles_mapping";
        if (all.contains(grp_roles_mapping))
        {
            UserDatabase::StringSetMap role_map_tmp;
            n_roles = 0;
            EntryHandler role_handler = [&](Json& role_data, int ind) {
                // The role definition must contain 'user', 'host' and 'role'-fields.
                string uname = role_data.get_string("user");
                string host = role_data.get_string("host");
                string data = role_data.get_string("role");

                if (role_data.ok())
                {
                    string key = UserDatabase::form_db_mapping_key(uname, host);
                    role_map_tmp[key].insert(data);
                    n_roles++;
                }
                else
                {
                    MXB_ERROR("Role entry %i in '%s'-array in file '%s' is missing a required field: %s",
                              ind + 1, grp_roles_mapping, filepathc, role_data.error_msg().c_str());
                }
            };
            process_array(all, grp_roles_mapping, role_handler);
            output.add_role_mapping(move(role_map_tmp));
        }

        // Print a log message explaining how many of each item type was read.
        std::vector<string> list_items;
        auto message_helper = [&list_items](int n_items, const char* desc) {
            if (n_items == 1)
            {
                list_items.push_back(mxb::string_printf("1 %s entry", desc));
            }
            else if (n_items >= 0)
            {
                list_items.push_back(mxb::string_printf("%i %s entries", n_items, desc));
            }
        };
        message_helper(n_users, "user");
        message_helper(n_grants, "database grant");
        message_helper(n_roles, "role mapping");

        // Ensure that the returned message is never empty.
        string total_list;
        if (list_items.empty())
        {
            total_list = "0 user entries";
        }
        else
        {
            total_list = mxb::create_list_string(list_items, ", ", " and ");
        }
        rval.msg = mxb::string_printf("%s from '%s'", total_list.c_str(), filepathc);
    }
    else
    {
        MXB_ERROR("Failed to load users from file. %s", all.error_msg().c_str());
    }
    return rval;
}

void MariaDBUserManager::remove_star(string& pw)
{
    if (!pw.empty() && pw[0] == '*')
    {
        pw.erase(0, 1);
    }
}

/**
 * Generates the string "<user>@<host>"
 */
std::string UserDatabase::form_db_mapping_key(const string& user, const string& host)
{
    string rval;
    rval.reserve(user.length() + 1 + host.length());
    rval.append(user).push_back('@');
    rval.append(host);
    return rval;
}

bool UserDatabase::add_entry(mariadb::UserEntry&& entry)
{
    bool rval = false;
    auto& entrylist = m_users[entry.username];
    // Find the correct spot to insert. If the hostname pattern already exists, do nothing. Copies should
    // only exist when summing users from all servers or when processing Xpand users.
    auto low_bound = std::lower_bound(entrylist.begin(), entrylist.end(), entry,
                                      UserEntry::host_pattern_is_more_specific);
    // lower_bound is the first valid (not "smaller") position to insert. It can be equal to the new element.
    if (low_bound == entrylist.end() || low_bound->host_pattern != entry.host_pattern)
    {
        entrylist.insert(low_bound, std::move(entry));
        rval = true;
    }
    return rval;
}

void UserDatabase::clear()
{
    m_users.clear();
}

UserDatabase::FindEntryResult UserDatabase::find_entry(const std::string& username, const std::string& ip,
                                                       const std::optional<string>& hostname) const
{
    return find_entry(username, ip, hostname, HostPatternMode::MATCH);
}

UserDatabase::FindEntryResult UserDatabase::find_entry(const std::string& username) const
{
    return find_entry(username, "", {}, HostPatternMode::SKIP);
}

const mariadb::UserEntry*
UserDatabase::find_entry_equal(const string& username, const string& host_pattern) const
{
    return find_entry(username, host_pattern, {}, HostPatternMode::EQUAL).entry;
}

UserDatabase::FindEntryResult
UserDatabase::find_entry(const std::string& username, const std::string& ip,
                         const std::optional<string>& hostname, HostPatternMode mode) const
{
    FindEntryResult rval;
    auto iter = m_users.find(username);
    if (iter != m_users.end())
    {
        auto& entrylist = iter->second;
        // List is already ordered, take the first matching entry.
        for (auto& entry : entrylist)
        {
            // The entry must not be a role (they should have empty hostnames in any case) and the hostname
            // pattern should match the host.
            if (!entry.is_role)
            {
                bool found_match = false;
                bool need_rdns = false;
                switch (mode)
                {
                case HostPatternMode::SKIP:
                    found_match = true;
                    break;

                case HostPatternMode::MATCH:
                    {
                        auto res = address_matches_host_pattern(ip, hostname, entry);
                        if (res == MatchResult::YES)
                        {
                            found_match = true;
                        }
                        else if (res == MatchResult::NEED_RDNS)
                        {
                            need_rdns = true;
                        }
                    }
                    break;

                case HostPatternMode::EQUAL:
                    found_match = (ip == entry.host_pattern);
                    break;
                }

                if (found_match)
                {
                    rval.entry = &entry;
                    break;
                }
                else if (need_rdns)
                {
                    rval.need_rdns = true;
                    break;
                }
            }
        }
    }
    return rval;
}

mariadb::UserEntry* UserDatabase::find_mutable_entry_equal(const string& username, const string& host_pattern)
{
    mariadb::UserEntry* rval = nullptr;
    auto iter = m_users.find(username);
    if (iter != m_users.end())
    {
        EntryList& entries = iter->second;
        UserEntry needle;
        needle.host_pattern = host_pattern;
        auto low_bound = std::lower_bound(entries.begin(), entries.end(), needle,
                                          UserEntry::host_pattern_is_more_specific);
        if (low_bound != entries.end() && low_bound->host_pattern == needle.host_pattern)
        {
            rval = &(*low_bound);
        }
    }
    return rval;
}

size_t UserDatabase::n_usernames() const
{
    return m_users.size();
}

size_t UserDatabase::n_entries() const
{
    size_t rval = 0;
    for (const auto& elem : m_users)
    {
        rval += elem.second.size();
    }
    return rval;
}

/**
 * Helper function for updating mappings.
 *
 * @param target Which mapping to update
 * @param source Source data
 */
void UserDatabase::update_mapping(StringSetMap& target, StringSetMap&& source)
{
    if (target.empty())
    {
        // Typical case when not summing users over all servers.
        target = move(source);
    }
    else
    {
        // Need to sum the maps element by element, as this function may be called multiple times
        // for the same target.
        for (auto& source_elem : source)
        {
            const string& userhost = source_elem.first;
            if (target.count(userhost) == 0)
            {
                // If the username does not yet exist, simply assign the set contents.
                target[userhost] = move(source_elem.second);
            }
            else
            {
                // Sum the string sets.
                StringSet& existing_elems = target[userhost];
                StringSet& new_elems = source_elem.second;
                for (auto& elem : new_elems)
                {
                    existing_elems.insert(elem);
                }
            }
        }
    }
}

void UserDatabase::add_db_grants(StringSetMap&& db_wc_grants, StringSetMap&& db_grants)
{
    update_mapping(m_database_wc_grants, move(db_wc_grants));
    update_mapping(m_database_grants, move(db_grants));
}

void UserDatabase::add_role_mapping(StringSetMap&& role_mapping)
{
    update_mapping(m_roles_mapping, move(role_mapping));
}

bool UserDatabase::check_database_access(const UserEntry& entry, const std::string& db,
                                         bool case_sensitive_db) const
{
    auto& user = entry.username;
    auto& host = entry.host_pattern;
    auto& def_role = entry.default_role;

    // Accept the user if the entry has a direct global privilege,
    return entry.global_db_priv
            // or the user has a privilege to the database, or a table or column in the database,
           || (user_can_access_db(user, host, db, case_sensitive_db))
            // or the user can access db through its default role.
           || (!def_role.empty() && user_can_access_role(user, host, def_role)
               && role_can_access_db(def_role, db, case_sensitive_db));
}

bool UserDatabase::user_can_access_db(const string& user, const string& host_pattern, const string& target_db,
                                      bool case_sensitive_db) const
{
    string key = form_db_mapping_key(user, host_pattern);
    bool grant_found = false;

    auto like = [case_sensitive_db](const string& pattern, const string& subject) {
        char esc = '\\';
        auto pat = pattern.c_str();
        auto subj = subject.c_str();
        int ret = case_sensitive_db ? mxq::sql_strlike_case(pat, subj, esc) :
            mxq::sql_strlike(pat, subj, esc);
        return ret == 0;
    };

    // Need to check two database grant maps, one may have wildcard grants.
    auto wc_mapping_iter = m_database_wc_grants.find(key);
    if (wc_mapping_iter != m_database_wc_grants.end())
    {
        const StringSet& allowed_db_patterns = wc_mapping_iter->second;
        // First check for exact match. If not found, iterate over each elem.
        if (allowed_db_patterns.count(target_db))
        {
            grant_found = true;
        }
        else
        {
            // Compare each element as in LIKE. Escaped wildcards in the pattern are handled.
            for (const auto& allowed_db_pattern : allowed_db_patterns)
            {
                if (like(allowed_db_pattern, target_db))
                {
                    grant_found = true;
                    break;
                }
            }
        }
    }

    if (!grant_found)
    {
        // Grant not found in the wildcard set, check the normal set. Any wildcards in the elements are
        // treated as normal characters.
        auto mapping_iter = m_database_grants.find(key);
        if (mapping_iter != m_database_grants.end())
        {
            const StringSet& allowed_dbs = mapping_iter->second;
            if (allowed_dbs.count(target_db))
            {
                grant_found = true;     // found exact match.
            }
            else if (!case_sensitive_db)
            {
                // If comparing db-names case-insensitively, iterate through the set.
                for (const auto& allowed_db : allowed_dbs)
                {
                    if (strcasecmp(allowed_db.c_str(), target_db.c_str()) == 0)
                    {
                        grant_found = true;
                        break;
                    }
                }
            }
        }
    }
    return grant_found;
}

bool UserDatabase::user_can_access_role(const std::string& user, const std::string& host_pattern,
                                        const std::string& target_role) const
{
    string key = user + "@" + host_pattern;
    auto iter = m_roles_mapping.find(key);
    if (iter != m_roles_mapping.end())
    {
        return iter->second.count(target_role) > 0;
    }
    return false;
}

bool UserDatabase::role_can_access_db(const string& role, const string& db, bool case_sensitive_db) const
{
    auto role_has_global_priv = [this](const string& rol) {
        bool rval = false;
        auto iter = m_users.find(rol);
        if (iter != m_users.end())
        {
            auto& entrylist = iter->second;
            // Because roles have an empty host-pattern, they must be first in the list.
            if (!entrylist.empty())
            {
                auto& entry = entrylist.front();
                if (entry.is_role && entry.global_db_priv)
                {
                    rval = true;
                }
            }
        }
        return rval;
    };

    auto find_linked_roles = [this](const string& rol) {
        std::vector<string> rval;
        string key = rol + "@";
        auto iter = m_roles_mapping.find(key);
        if (iter != m_roles_mapping.end())
        {
            auto& roles_set = iter->second;
            for (auto& linked_role : roles_set)
            {
                rval.push_back(linked_role);
            }
        }
        return rval;
    };

    // Roles are tricky since one role may have access to other roles and so on.
    StringSet open_set;     // roles which still need to be expanded.
    StringSet closed_set;   // roles which have been checked already.

    open_set.insert(role);
    bool privilege_found = false;
    while (!open_set.empty() && !privilege_found)
    {
        string current_role = *open_set.begin();
        // First, check if role has global privilege.
        if (role_has_global_priv(current_role))
        {
            privilege_found = true;
        }
        // No global privilege, check db-level privilege.
        else if (user_can_access_db(current_role, "", db, case_sensitive_db))
        {
            privilege_found = true;
        }
        else
        {
            // The current role does not have access to db. Add linked roles to the open set.
            auto linked_roles = find_linked_roles(current_role);
            for (const auto& linked_role : linked_roles)
            {
                if (open_set.count(linked_role) == 0 && closed_set.count(linked_role) == 0)
                {
                    open_set.insert(linked_role);
                }
            }
        }

        open_set.erase(current_role);
        closed_set.insert(current_role);
    }
    return privilege_found;
}

/**
 * Check if address matches host pattern.
 * @param addr Subject address.
 * @param hostname Resolved hostname if any
 * @param entry User account entry. Host pattern may contain wildcards % and _.
 * @return Match result
 */
UserDatabase::MatchResult
UserDatabase::address_matches_host_pattern(const string& addr, const std::optional<string>& hostname,
                                           const UserEntry& entry) const
{
    // First, check the input address type. This affects how the comparison to the host pattern works.
    auto addrtype = parse_address_type(addr);
    // If host address form is unexpected, don't bother continuing.
    if (addrtype == AddrType::UNKNOWN)
    {
        // TODO: entry.username is not always the user trying to log in, as in some cases an anonymous
        // entry may be attempted. In any case, this error message should not happen.
        MXB_ERROR("Address '%s' of incoming user '%s' is not supported.",
                  addr.c_str(), entry.username.c_str());
        return MatchResult::NO;
    }

    auto& host_pattern = entry.host_pattern;
    // TODO: The result of pattern type parsing could be saved.
    auto patterntype = parse_pattern_type(host_pattern);
    if (patterntype == PatternType::UNKNOWN)
    {
        MXB_ERROR("Host pattern '%s' of user account '%s'@'%s' is not supported.",
                  host_pattern.c_str(), entry.username.c_str(), host_pattern.c_str());
        return MatchResult::NO;
    }

    auto like = [](const string& pattern, const string& str) {
        return mxq::sql_strlike(pattern.c_str(), str.c_str(), '\\') == 0;
    };

    auto matched = MatchResult::NO;
    if (patterntype == PatternType::MASK)
    {
        // Mask-type patterns only match ipv4 or ipv4-mapped clients.
        string effective_addr;
        if (addrtype == AddrType::IPV4)
        {
            effective_addr = addr;
        }
        else if (addrtype == AddrType::MAPPED)
        {
            effective_addr = addr.substr(addr.find_last_of(':') + 1);
        }

        if (!effective_addr.empty())
        {
            // The pattern is of type "base_ip/mask". The client ip should be accepted if
            // client_ip & mask == base_ip. All three parts need to be converted
            // to numbers.
            auto ip_to_addr = [](const string& ip, in_addr* out) -> bool {
                    return (inet_pton(AF_INET, ip.c_str(), out) == 1);
                };

            auto div_loc = host_pattern.find('/');
            string base_ip_str = host_pattern.substr(0, div_loc);
            string netmask_str = host_pattern.substr(div_loc + 1);
            in_addr address;
            in_addr base_ip;
            in_addr mask;
            if (ip_to_addr(effective_addr, &address) && ip_to_addr(base_ip_str, &base_ip)
                && ip_to_addr(netmask_str, &mask))
            {
                if ((address.s_addr & mask.s_addr) == base_ip.s_addr)
                {
                    matched = MatchResult::YES;
                }
            }
        }
    }
    else
    {
        if (like(host_pattern, addr))
        {
            matched = MatchResult::YES;
        }
        else if (addrtype == AddrType::MAPPED && like(host_pattern, addr.substr(addr.find_last_of(':') + 1)))
        {
            // The IPv4 part of the address matched.
            matched = MatchResult::YES;
        }
        else if (hostname.has_value())
        {
            // rDNS has been run. If rDNS failed (for whatever reason), skip this host pattern. Seems like
            // server does the same.
            if (!hostname->empty())
            {
                if (looks_like_ipv4(*hostname))
                {
                    MXB_WARNING("IP address '%s' has the hostname '%s' which looks like an IPv4 address.",
                                addr.c_str(), hostname->c_str());
                }
                else if (like(host_pattern, *hostname))
                {
                    matched = MatchResult::YES;
                }
            }
        }

        if (matched == MatchResult::YES || addrtype == AddrType::LOCALHOST)
        {
            // Pattern matched the IP address or this is a "localhost"-address. The latter never gets a name
            // lookup even if localhost would point somewhere else.
        }
        else if (!hostname.has_value() && patterntype == PatternType::HOSTNAME
                 && !mxs::Config::get().skip_name_resolve.get())
        {
            // Need a reverse lookup on the client address.
            matched = MatchResult::NEED_RDNS;
        }
    }

    return matched;
}

UserDatabase::AddrType UserDatabase::parse_address_type(const std::string& addr) const
{
    using mxb::Host;

    auto rval = AddrType::UNKNOWN;
    if (Host::is_valid_ipv4(addr))
    {
        rval = AddrType::IPV4;
    }
    else if (strcasecmp(addr.c_str(), "localhost") == 0)
    {
        rval = AddrType::LOCALHOST;
    }
    else
    {
        // The address could be IPv4 mapped to IPv6.
        const string mapping_prefix = ":ffff:";
        auto prefix_loc = addr.find(mapping_prefix);
        if (prefix_loc != npos)
        {
            auto ipv4part_loc = prefix_loc + mapping_prefix.length();
            if (addr.length() >= (ipv4part_loc + ipv4min_len))
            {
                // The part after the prefix should be a normal ipv4-address.
                string ipv4part = addr.substr(ipv4part_loc);
                if (Host::is_valid_ipv4(ipv4part))
                {
                    rval = AddrType::MAPPED;
                }
            }
        }

        // Finally, the address could be ipv6.
        if (rval == AddrType::UNKNOWN && Host::is_valid_ipv6(addr))
        {
            rval = AddrType::IPV6;
        }
    }
    return rval;
}

UserDatabase::PatternType UserDatabase::parse_pattern_type(const std::string& host_pattern) const
{
    using mxb::Host;
    // The host pattern must be assumed to be a hostname which requires reverse name resolution of the
    // client IP. The only cases where we can be certain that no reverse name lookup is necessary are if
    // the host pattern is a valid IPv4 or IPv6 address, a IPv4 or IPv6 netmask or if the pattern consists
    // only of wildcards.

    auto is_wc = [](char c) {
        return c == '%' || c == '_';
    };

    auto patterntype = PatternType::UNKNOWN;
    // First, check some common special cases.
    if (Host::is_valid_ipv4(host_pattern) || Host::is_valid_ipv6(host_pattern))
    {
        // No wildcards, just an address.
        patterntype = PatternType::ADDRESS;
    }
    else if (std::all_of(host_pattern.begin(), host_pattern.end(), is_wc))
    {
        // Pattern is composed entirely of wildcards.
        patterntype = PatternType::ADDRESS;
        // Could be hostname as well, but this would only make a difference with a pattern
        // like "________" or "__%___" where the resolved hostname is of correct length
        // while the address is not.
    }
    else
    {
        auto div_loc = host_pattern.find('/');
        if (div_loc != npos && (div_loc >= ipv4min_len) && host_pattern.length() > (div_loc + ipv4min_len))
        {
            // May be a base_ip/netmask-combination.
            string base_ip = host_pattern.substr(0, div_loc);
            string netmask = host_pattern.substr(div_loc + 1);
            if (Host::is_valid_ipv4(base_ip) && Host::is_valid_ipv4(netmask))
            {
                patterntype = PatternType::MASK;
            }
        }
    }

    if (patterntype == PatternType::UNKNOWN)
    {
        // An IPV4 address with a trailing wildcard like '192.168.1.%' would matche both the IPv4 address
        // 192.168.1.123 and the hostname '192.168.1.deadbeef'. MariaDB does not allow this since wildcards
        // that start with a part of an IPv4 address and end with a wildcard are nearly universally expected
        // to only match an IPv4 address and them matching hostnames is usually a nasty surprise.
        // Additionally, if the host pattern contains characters that cannot be in a valid hostname
        // (e.g. an IPv6 mask like '%::fe::%') the reverse name lookup can be skipped.
        if (looks_like_ipv4(host_pattern) || impossible_hostname(host_pattern))
        {
            patterntype = PatternType::ADDRESS;
        }
        else
        {
            patterntype = PatternType::HOSTNAME;
        }
    }
    return patterntype;
}

bool UserDatabase::equal_contents(const UserDatabase& rhs) const
{
    return m_users == rhs.m_users
           && m_database_wc_grants == rhs.m_database_wc_grants
           && m_database_grants == rhs.m_database_grants
           && m_roles_mapping == rhs.m_roles_mapping
           && m_database_names == rhs.m_database_names;
}

json_t* UserDatabase::users_to_json() const
{
    auto rval = json_array();
    for (auto& elem_outer : m_users)
    {
        for (auto& elem : elem_outer.second)
        {
            auto entry = json_pack("{s:s, s:s, s:s, s:b, s:b, s:b, s:b, s:s}",
                                   "user", elem.username.c_str(), "host", elem.host_pattern.c_str(),
                                   "plugin", elem.plugin.c_str(), "ssl", elem.ssl,
                                   "super_priv", elem.super_priv, "global_priv", elem.global_db_priv,
                                   "proxy_priv", elem.proxy_priv,
                                   "default_role", elem.default_role.cend());
            json_array_append_new(rval, entry);
        }
    }
    return rval;
}

bool UserDatabase::empty() const
{
    return m_users.empty();
}

void UserDatabase::add_database_name(const std::string& db_name)
{
    m_database_names.insert(db_name);
}

bool UserDatabase::check_database_exists(const std::string& db, bool case_sensitive_db) const
{
    bool rval = false;
    if (m_database_names.count(db))
    {
        rval = true;    // True for either mode.
    }
    else if (!case_sensitive_db)
    {
        // Check all values. TODO: Can probably optimize this using string ordering but nevermind for now.
        for (const auto& elem : m_database_names)
        {
            if (strcasecmp(elem.c_str(), db.c_str()) == 0)
            {
                rval = true;
                break;
            }
        }
    }
    return rval;
}

MariaDBUserCache::MariaDBUserCache(const MariaDBUserManager& master)
    : m_master(master)
{
}

UserEntryResult
MariaDBUserCache::find_user(const string& user, const string& requested_db,
                            const MYSQL_session* session) const
{
    auto userz = user.c_str();
    const string& ip = session->remote;
    auto ipz = ip.c_str();
    auto requested_dbz = requested_db.c_str();
    const auto& sett = session->user_search_settings;

    string eff_requested_db;    // Use the requested_db as given by user only for log messages.
    bool case_sensitive_db = true;
    switch (sett.listener.db_name_cmp_mode)
    {
    case UserDatabase::DBNameCmpMode::CASE_SENSITIVE:
        eff_requested_db = requested_db;
        break;

    case UserDatabase::DBNameCmpMode::LOWER_CASE:
        eff_requested_db = mxb::tolower(requested_db);
        case_sensitive_db = false;
        break;

    case UserDatabase::DBNameCmpMode::CASE_INSENSITIVE:
        eff_requested_db = requested_db;
        case_sensitive_db = false;
        break;
    }
    /**
     * The result from user account search. Even if the result is an authentication failure, a normal
     * authentication token exchange and check should be carried out to match how the server works.
     * This way, the client won't know the exact cause of failure without giving the correct password. */
    auto res = UserEntryResult();

    const char bad_db_fmt[] = "Found matching user entry '%s'@'%s' for client '%s'@'%s' but user tried to "
                              "access non-existing database '%s'.";
    // TODO: the user may be empty, is it ok to match normally in that case?

    // First try to find a normal user entry. If host pattern matching is disabled, match only username.
    auto find_res = sett.listener.match_host_pattern ? m_userdb->find_entry(user, ip, session->host) :
        m_userdb->find_entry(user);
    if (find_res.entry)
    {
        auto found = find_res.entry;
        res.entry = *found;
        // If trying to access a specific database, check if allowed.
        bool db_ok = true;
        if (!eff_requested_db.empty())
        {
            if (!m_userdb->check_database_exists(eff_requested_db, case_sensitive_db))
            {
                db_ok = false;
                res.type = UserEntryType::BAD_DB;
                MXB_INFO(bad_db_fmt,
                         found->username.c_str(), found->host_pattern.c_str(), userz, ipz,
                         requested_dbz);
            }
            else if (eff_requested_db == info_schema
                     || (!case_sensitive_db
                         && strcasecmp(eff_requested_db.c_str(), info_schema.c_str()) == 0))
            {
                // Accessing "information_schema", allow it.
            }
            else if (!m_userdb->check_database_access(*found, eff_requested_db, case_sensitive_db))
            {
                db_ok = false;
                res.type = UserEntryType::DB_ACCESS_DENIED;
                MXB_INFO("Found matching user entry '%s'@'%s' for client '%s'@'%s' but user does not have "
                         "access to database '%s'.",
                         found->username.c_str(), found->host_pattern.c_str(), userz, ipz,
                         requested_dbz);
            }
        }

        if (db_ok)
        {
            res.type = UserEntryType::USER_ACCOUNT_OK;
            MXB_INFO("Found matching user '%s'@'%s' for client '%s'@'%s' with sufficient privileges.",
                     found->username.c_str(), found->host_pattern.c_str(), userz, ipz);
        }
    }
    else if (!find_res.need_rdns && sett.listener.allow_anon_user)
    {
        // Try to find an anonymous entry. Such an entry has empty username and matches any client username.
        // If host pattern matching is disabled, any user from any host can log in if an anonymous
        // entry exists.
        auto anon_find_res = sett.listener.match_host_pattern ? m_userdb->find_entry("", ip, session->host) :
            m_userdb->find_entry("");

        if (anon_find_res.entry)
        {
            auto anon_found = anon_find_res.entry;
            res.entry = *anon_found;
            // For anonymous users, do not check database access as the final effective user is unknown.
            // Instead, check that the entry has a proxy grant.
            if (!eff_requested_db.empty()
                && !m_userdb->check_database_exists(eff_requested_db, case_sensitive_db))
            {
                res.type = UserEntryType::BAD_DB;
                MXB_INFO(bad_db_fmt,
                         anon_found->username.c_str(), anon_found->host_pattern.c_str(), userz, ipz,
                         requested_dbz);
            }
            else if (!anon_found->proxy_priv)
            {
                res.type = UserEntryType::ANON_PROXY_ACCESS_DENIED;
                MXB_INFO("Found matching anonymous user ''@'%s' for client '%s'@'%s' but user does not have "
                         "proxy privileges.",
                         anon_found->host_pattern.c_str(), userz, ipz);
            }
            else
            {
                res.type = UserEntryType::USER_ACCOUNT_OK;
                MXB_INFO("Found matching anonymous user ''@'%s' for client '%s'@'%s' with proxy grant.",
                         anon_found->host_pattern.c_str(), userz, ipz);
            }
        }
        else if (anon_find_res.need_rdns)
        {
            res.type = UserEntryType::NEED_NAMEINFO;
        }
    }
    else if (find_res.need_rdns)
    {
        res.type = UserEntryType::NEED_NAMEINFO;
    }

    // If "root" user is being accepted when not allowed, block it now.
    if (res.type == UserEntryType::USER_ACCOUNT_OK)
    {
        if (!sett.service.allow_root_user && user == "root")
        {
            res.type = UserEntryType::ROOT_ACCESS_DENIED;
            MXB_INFO("Client '%s'@'%s' blocked because '%s' is false.", userz, ipz, CN_ENABLE_ROOT_USER);
        }
    }
    // Finally, if user was not found, generate a dummy entry so that authentication can continue.
    // It will fail in the end regardless.
    else if (res.type == UserEntryType::USER_NOT_FOUND)
    {
        generate_dummy_entry(user, &res.entry);
    }
    return res;
}

void MariaDBUserCache::update_from_master()
{
    if (m_userdb_version < m_master.userdb_version())
    {
        // Master db has updated data, copy the shared pointer.
        auto db_info = m_master.get_user_database();
        m_userdb = move(db_info.user_db);
        m_userdb_version = db_info.version;
    }
}

bool MariaDBUserCache::can_update_immediately() const
{
    /**
     * The usercache can be updated (or is about to be updated) if
     * 1) The master database is ahead, meaning it's about to send the worker-message, or the message has
     * already been sent but the current worker hasn't picked it up yet.
     * 2) Or the minimum time between user updates has passed.
     */
    return m_userdb_version < m_master.userdb_version() || m_master.can_update_immediately();
}

int MariaDBUserCache::version() const
{
    return m_userdb_version;
}

void MariaDBUserCache::generate_dummy_entry(const std::string& user, mariadb::UserEntry* output) const
{
    // TODO: To match server behavior, this function should look at all the users, and select a plugin
    // based on the distribution of plugins used. The selection would need to be deterministic.
    // Worry about this later, the current version is ok in the usual case of mostly mysql_native_password.
    output->username = user;
    output->host_pattern = "%";
    output->plugin = mysql_default_auth;
}
