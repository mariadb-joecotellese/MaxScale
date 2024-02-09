/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#pragma once

const char* bug670_sql
    =
        "set autocommit=0;\
        use mysql;\
        set autocommit=1;\
        use test;\
        set autocommit=0;\
        use mysql;\
        set autocommit=1;\
        select user,host from user;\
        set autocommit=0;\
        use fakedb;\
        use test;\
        use mysql;\
        use dontuse;\
        use mysql;\
        drop table if exists t1;\
        commit;\
        use test;\
        use mysql;\
        set autocommit=1;\
        create table t1(id integer primary key);\
        insert into t1 values(5);\
        use test;\
        use mysql;\
        select user from user;\
        set autocommit=0;\
        set autocommit=1;\
        set autocommit=0;\
        insert into mysql.t1 values(7);\
        use mysql;\
        rollback work;\
        commit;\
        delete from mysql.t1 where id=7;\
        insert into mysql.t1 values(7);\
        select host,user from mysql.user;\
        set autocommit=1;\
        delete from mysql.t1 where id = 7; \
        select 1 as \"endof cycle\" from dual;\n";
