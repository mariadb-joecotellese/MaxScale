/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-04-10
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * MXS-1804: request 16M-1 stmt_prepare command packet connect hang
 *
 * https://jira.mariadb.org/browse/MXS-1804
 */

#include <maxtest/testconnections.hh>

int sql_str_size(int sqlsize)
{
    char prefx[] = "select ''";
    return sqlsize - strlen(prefx) - 1;
}

void gen_select_sqlstr(char* sqlstr, unsigned int strsize, int sqlsize)
{
    strcpy(sqlstr, "select '");
    memset(sqlstr + strlen("select '"), 'f', strsize);
    sqlstr[sqlsize - 2] = '\'';
    sqlstr[sqlsize - 1] = '\0';
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    test.repl->execute_query_all_nodes("SET GLOBAL max_allowed_packet=67108860");
    int sqlsize = 16777215;
    int strsize = sql_str_size(sqlsize);

    char* sqlstr = (char*)malloc(sqlsize);
    gen_select_sqlstr(sqlstr, strsize, sqlsize);

    test.reset_timeout();
    test.maxscale->connect();

    MYSQL_STMT* stmt = mysql_stmt_init(test.maxscale->conn_rwsplit);
    test.expect(mysql_stmt_prepare(stmt, sqlstr, strlen(sqlstr)) == 0,
                "Prepare should not fail. Error: %s",
                mysql_stmt_error(stmt));
    mysql_stmt_close(stmt);
    free(sqlstr);

    return test.global_result;
}
