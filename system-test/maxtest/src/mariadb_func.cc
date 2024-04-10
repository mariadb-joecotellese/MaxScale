/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

/**
 * @file mariadb_func.cpp - basic DB interaction routines
 *
 * @verbatim
 * Revision History
 *
 * Date     Who     Description
 * 17/11/14 Timofey Turenko Initial implementation
 *
 * @endverbatim
 */


#include <maxtest/mariadb_func.hh>
#include <maxtest/test_dir.hh>
#include <ctype.h>
#include <sstream>
#include <maxbase/string.hh>

int set_ssl(MYSQL* conn)
{
    char client_key[1024];
    char client_cert[1024];
    char ca[1024];

    auto test_dir = mxt::SOURCE_DIR;
    snprintf(client_key, 1024, "%s/ssl-cert/client.key", test_dir);
    snprintf(client_cert, 1024, "%s/ssl-cert/client.crt", test_dir);
    snprintf(ca, 1024, "%s/ssl-cert/ca.crt", test_dir);

    return mysql_ssl_set(conn, client_key, client_cert, ca, NULL, NULL);
}

MYSQL* open_conn_db_flags(int port,
                          std::string ip,
                          std::string db,
                          std::string user,
                          std::string password,
                          unsigned long flag,
                          bool ssl)
{
    MYSQL* conn = mysql_init(NULL);

    if (conn == NULL)
    {
        fprintf(stdout, "Error: can't create MySQL-descriptor\n");
        return NULL;
    }

    if (ssl)
    {
        set_ssl(conn);
    }

    unsigned int timeout = 15;
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

    // MXS-2568: This fixes mxs1828_double_local_infile
    mysql_optionsv(conn, MYSQL_OPT_LOCAL_INFILE, (void*)"1");

    if (!mysql_real_connect(conn,
                            ip.c_str(),
                            user.c_str(),
                            password.c_str(),
                            db.c_str(),
                            port,
                            NULL,
                            flag))
    {
        fprintf(stdout,
                "Could not connect to %s:%d with user '%s' and password '%s', "
                "and default database '%s': %s\n",
                ip.c_str(), port, user.c_str(), password.c_str(), db.c_str(), mysql_error(conn));
    }
    return conn;
}

MYSQL* open_conn_db_timeout(int port,
                            std::string ip,
                            std::string db,
                            std::string user,
                            std::string password,
                            unsigned int timeout,
                            bool ssl)
{
    MYSQL* conn = mysql_init(NULL);

    if (conn == NULL)
    {
        fprintf(stdout, "Error: can't create MySQL-descriptor\n");
        return NULL;
    }

    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &timeout);
    mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT, &timeout);

    // MXS-2568: This fixes mxs1828_double_local_infile
    mysql_optionsv(conn, MYSQL_OPT_LOCAL_INFILE, (void*)"1");

    if (ssl)
    {
        set_ssl(conn);
    }

    if (!mysql_real_connect(conn,
                            ip.c_str(),
                            user.c_str(),
                            password.c_str(),
                            db.c_str(),
                            port,
                            NULL,
                            CLIENT_MULTI_STATEMENTS))
    {
        fprintf(stdout,
                "Could not connect to %s:%d with user '%s' and password '%s', "
                "and default database '%s': %s\n",
                ip.c_str(), port, user.c_str(), password.c_str(), db.c_str(), mysql_error(conn));
    }
    return conn;
}

int execute_query(MYSQL* conn, const char* format, ...)
{
    va_list valist;

    va_start(valist, format);
    int message_len = vsnprintf(NULL, 0, format, valist);
    va_end(valist);

    char sql[message_len + 1];

    va_start(valist, format);
    vsnprintf(sql, sizeof(sql), format, valist);
    va_end(valist);

    return execute_query_silent(conn, sql, false);
}

int execute_query_from_file(MYSQL* conn, FILE* file)
{
    int rc = -1;
    char buf[4096];

    if (fgets(buf, sizeof(buf), file))
    {
        char* nul = strchr(buf, '\0') - 1;

        while (isspace(*nul))
        {
            *nul-- = '\0';
        }

        char* ptr = buf;

        while (isspace(*ptr))
        {
            ptr++;
        }

        if (*ptr)
        {
            rc = execute_query_silent(conn, buf, false);
        }
    }
    else if (!feof(file))
    {
        printf("Failed to read file: %d, %s", errno, strerror(errno));
        rc = 1;
    }

    return rc;
}

int execute_query_silent(MYSQL* conn, const char* sql, bool silent)
{
    MYSQL_RES* res;
    if (conn != NULL)
    {
        if (mysql_query(conn, sql) != 0)
        {
            if (!silent)
            {
                int len = strlen(sql);
                printf("Error: can't execute SQL-query: %.*s\n", len < 60 ? len : 60, sql);
                printf("%s\n\n", mysql_error(conn));
            }
            return 1;
        }
        else
        {
            do
            {
                res = mysql_store_result(conn);
                mysql_free_result(res);
            }
            while (mysql_next_result(conn) == 0);
            return 0;
        }
    }
    else
    {
        if (!silent)
        {
            printf("Connection is broken\n");
        }
        return 1;
    }
}

int execute_query_check_one(MYSQL* conn, const char* sql, const char* expected)
{
    int r = 1;

    if (conn != NULL)
    {
        const int n_attempts = 3;

        for (int i = 0; i < n_attempts && r != 0; i++)
        {
            if (i > 0)
            {
                sleep(1);
            }

            if (mysql_query(conn, sql) != 0)
            {
                printf("Error: can't execute SQL-query: %s\n", sql);
                printf("%s\n\n", mysql_error(conn));
                break;
            }
            else
            {
                do
                {
                    MYSQL_RES* res = mysql_store_result(conn);

                    if (res)
                    {
                        if (mysql_num_rows(res) == 1)
                        {
                            MYSQL_ROW row = mysql_fetch_row(res);

                            if (row[0] != NULL)
                            {
                                if (strcmp(row[0], expected) == 0)
                                {
                                    r = 0;
                                    printf("First field is '%s' as expected\n", row[0]);
                                }
                                else
                                {
                                    printf("First field is '%s', but expected '%s'\n", row[0], expected);
                                }
                            }
                            else
                            {
                                printf("First field is NULL\n");
                            }
                        }
                        else
                        {
                            printf("Number of rows is not 1, it is %llu\n", mysql_num_rows(res));
                        }

                        mysql_free_result(res);
                    }
                }
                while (mysql_next_result(conn) == 0);
            }
        }
    }
    else
    {
        printf("Connection is broken\n");
    }

    return r;
}

int execute_query_affected_rows(MYSQL* conn, const char* sql, my_ulonglong* affected_rows)
{
    MYSQL_RES* res;
    if (conn != NULL)
    {
        if (mysql_query(conn, sql) != 0)
        {
            printf("Error: can't execute SQL-query: %s\n", sql);
            printf("%s\n\n", mysql_error(conn));
            return 1;
        }
        else
        {
            do
            {
                *affected_rows = mysql_affected_rows(conn);
                res = mysql_store_result(conn);
                mysql_free_result(res);
            }
            while (mysql_next_result(conn) == 0);
            return 0;
        }
    }
    else
    {
        printf("Connection is broken\n");
        return 1;
    }
}

int execute_query_num_of_rows(MYSQL* conn,
                              const char* sql,
                              my_ulonglong* num_of_rows,
                              unsigned long long* i)
{
    MYSQL_RES* res;
    my_ulonglong N;


    printf("%s\n", sql);
    if (conn != NULL)
    {
        if (mysql_query(conn, sql) != 0)
        {
            printf("Error: can't execute SQL-query: %s\n", sql);
            printf("%s\n\n", mysql_error(conn));
            * i = 0;
            return 1;
        }
        else
        {
            *i = 0;
            do
            {
                res = mysql_store_result(conn);
                if (res != NULL)
                {
                    N = mysql_num_rows(res);
                    mysql_free_result(res);
                }
                else
                {
                    N = 0;
                }
                num_of_rows[*i] = N;
                *i = *i + 1;
            }
            while (mysql_next_result(conn) == 0);
            return 0;
        }
    }
    else
    {
        printf("Connection is broken\n");
        * i = 0;
        return 1;
    }
}

int execute_stmt_num_of_rows(MYSQL_STMT* stmt, my_ulonglong* num_of_rows, unsigned long long* i)
{
    my_ulonglong N;

    /* This is debug hack; compatible only with t1 from t1_sql.h
     *  my_ulonglong k;
     *  MYSQL_BIND bind[2];
     *  my_ulonglong x1;
     *  my_ulonglong fl;
     *
     *  unsigned long length[2];
     *  my_bool       is_null[2];
     *  my_bool       error[2];
     *
     *  memset(bind, 0, sizeof(bind));
     *  bind[0].buffer = &x1;
     *  bind[0].buffer_type = MYSQL_TYPE_LONG;
     *  bind[0].length = &length[0];
     *  bind[0].is_null = &is_null[0];
     *  bind[0].error = &error[0];
     *
     *  bind[1].buffer = &fl;
     *  bind[1].buffer_type = MYSQL_TYPE_LONG;
     *  bind[1].length = &length[0];
     *  bind[1].is_null = &is_null[0];
     *  bind[1].error = &error[0];
     */

    if (mysql_stmt_execute(stmt) != 0)
    {
        printf("Error: can't execute prepared statement\n");
        printf("%s\n\n", mysql_stmt_error(stmt));
        * i = 0;
        return 1;
    }
    else
    {
        *i = 0;
        do
        {
            mysql_stmt_store_result(stmt);
            N = mysql_stmt_num_rows(stmt);
            /* This is debug hack; compatible only with t1 from t1_sql.h
             *  mysql_stmt_bind_result(stmt, bind);
             *  for (k = 0; k < N; k++)
             *  {
             *   mysql_stmt_fetch(stmt);
             *   printf("%04llu: x1 %llu, fl %llu\n", k, x1, fl);
             *  }
             */
            num_of_rows[*i] = N;
            *i = *i + 1;
        }
        while (mysql_stmt_next_result(stmt) == 0);
        return 0;
    }
    return 1;
}

int execute_query_count_rows(MYSQL* conn, const char* sql)
{
    int rval = -1;

    unsigned long long num_of_rows[1024];
    unsigned long long total;

    if (execute_query_num_of_rows(conn, sql, num_of_rows, &total) == 0)
    {
        rval = 0;

        for (unsigned int i = 0; i < total && i < 1024; i++)
        {
            rval += num_of_rows[i];
        }
    }

    return rval;
}

int get_conn_num(MYSQL* conn, std::string ip, std::string hostname, std::string db)
{
    MYSQL_RES* res;
    MYSQL_ROW row;
    unsigned long long int rows;
    unsigned long long int i;
    unsigned int conn_num = 0;
    const char* hostname_internal;

    if (ip == "127.0.0.1")
    {
        hostname_internal = "localhost";
    }
    else
    {
        hostname_internal = hostname.c_str();
    }

    if (conn != NULL)
    {
        if (mysql_query(conn, "show processlist;") != 0)
        {
            printf("Error: can't execute SQL-query: show processlist\n");
            printf("%s\n\n", mysql_error(conn));
            conn_num = 0;
        }
        else
        {
            res = mysql_store_result(conn);
            if (res == NULL)
            {
                printf("Error: can't get the result description\n");
                conn_num = -1;
            }
            else
            {
                mysql_num_fields(res);
                rows = mysql_num_rows(res);
                for (i = 0; i < rows; i++)
                {
                    row = mysql_fetch_row(res);
                    if ((row[2] != NULL ) && (row[3] != NULL))
                    {
                        if ((strcmp(strtok(row[2], ":"), ip.c_str()) == 0) && strstr(row[3], db.c_str()))
                        {
                            conn_num++;
                        }
                        else if (strstr(row[2], hostname_internal) && strstr(row[3], db.c_str()))
                        {
                            conn_num++;
                        }
                    }
                }
            }
            mysql_free_result(res);
        }
    }
    if (ip == "127.0.0.1")
    {
        // one extra connection is visible in the process list
        // output in case of local test
        // (when MaxScale is on the same machine as backends)
        conn_num--;
    }
    return conn_num;
}

int find_field(MYSQL* conn, const char* sql, const char* field_name, char* value)
{
    MYSQL_RES* res;
    MYSQL_ROW row;
    MYSQL_FIELD* field;
    unsigned int ret = 1;
    unsigned long long int filed_i = 0;
    unsigned long long int i = 0;
    if (conn != NULL)
    {
        if (mysql_query(conn, sql) != 0)
        {
            printf("Error: can't execute SQL-query: %s\n", sql);
            printf("%s\n\n", mysql_error(conn));
        }
        else
        {
            res = mysql_store_result(conn);
            if (res == NULL)
            {
                printf("Error: can't get the result description\n");
            }
            else
            {
                mysql_num_fields(res);
                while ((field = mysql_fetch_field(res)) && ret != 0)
                {
                    if (strstr(field->name, field_name) != NULL)
                    {
                        filed_i = i;
                        ret = 0;
                    }
                    i++;
                }
                if (mysql_num_rows(res) > 0)
                {
                    row = mysql_fetch_row(res);
                    sprintf(value, "%s", row[filed_i]);
                }
                else
                {
                    sprintf(value, "%s", "");
                    ret = 1;
                }
            }
            mysql_free_result(res);
            do
            {
                res = mysql_store_result(conn);
                mysql_free_result(res);
            }
            while (mysql_next_result(conn) == 0);
        }
    }
    return ret;
}

Result get_result(MYSQL* conn, std::string sql)
{
    Result rval;
    MYSQL_RES* res;

    if (mysql_query(conn, sql.c_str()) == 0 && (res = mysql_store_result(conn)))
    {
        MYSQL_ROW row = mysql_fetch_row(res);

        while (row)
        {
            std::vector<std::string> tmp;
            int n = mysql_num_fields(res);
            for (int i = 0; i < n; ++i)
            {
                tmp.push_back(row[i] ? row[i] : "NULL");
            }
            rval.push_back(tmp);

            row = mysql_fetch_row(res);
        }
        mysql_free_result(res);
    }
    else
    {
        printf("Error: Query failed: %s\n", mysql_error(conn));
    }

    return rval;
}

Row get_row(MYSQL* conn, std::string sql)
{
    Result res = get_result(conn, sql);
    return res.empty() ? Row {} :
           res[0];
}

int get_int_version(std::string version)
{
    std::istringstream str(version);
    int major = 0;
    int minor = 0;
    int patch = 0;
    char dot;

    str >> major >> dot >> minor >> dot >> patch;
    return major * 10000 + minor * 100 + patch;
}

bool Connection::connect()
{
    mysql_close(m_conn);
    m_conn = mysql_init(NULL);

    // MXS-2568: This fixes mxs1828_double_local_infile
    mysql_optionsv(m_conn, MYSQL_OPT_LOCAL_INFILE, (void*)"1");

    if (m_ssl)
    {
        set_ssl(m_conn);
    }

    if (!m_charset.empty())
    {
        mysql_optionsv(m_conn, MYSQL_SET_CHARSET_NAME, m_charset.c_str());
    }

    if (m_timeout)
    {
        unsigned int timeout = m_timeout;
        mysql_optionsv(m_conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
        mysql_optionsv(m_conn, MYSQL_OPT_READ_TIMEOUT, &timeout);
        mysql_optionsv(m_conn, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
    }

    for (auto kv : m_attrs)
    {
        mysql_optionsv(m_conn, MYSQL_OPT_CONNECT_ATTR_ADD, kv.first.c_str(), kv.second.c_str());
    }

    return mysql_real_connect(m_conn, m_host.c_str(), m_user.c_str(), m_pw.c_str(), m_db.c_str(), m_port,
                              NULL, m_options)
           && mysql_errno(m_conn) == 0;
}

bool Connection::change_db(const std::string& db)
{
    return mysql_select_db(m_conn, db.c_str()) == 0;
}

std::string Connection::pretty_rows(const std::string& q) const
{
    std::string rval;

    for (const auto& a : rows(q))
    {
        rval += mxb::join(a) + '\n';
    }

    return rval;
}
