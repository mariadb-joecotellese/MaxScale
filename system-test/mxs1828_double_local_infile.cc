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

/**
 * MXS-1828: Multiple LOAD DATA LOCAL INFILE commands in one query cause a hang
 *
 * https://jira.mariadb.org/browse/MXS-1828
 */

#include <maxtest/testconnections.hh>
#include <fstream>

using namespace std;

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);

    const char* query = "LOAD DATA LOCAL INFILE './data.csv' INTO TABLE test.t1";
    const char* filename = "./data.csv";

    unlink(filename);
    ofstream file(filename);

    file << "1\n2\n3" << endl;

    test.reset_timeout();
    test.maxscale->connect();
    test.try_query(test.maxscale->conn_rwsplit, "CREATE OR REPLACE TABLE test.t1(id INT)");
    test.try_query(test.maxscale->conn_rwsplit, "%s;%s", query, query);

    test.try_query(test.maxscale->conn_rwsplit, "START TRANSACTION");
    Row row = get_row(test.maxscale->conn_rwsplit, "SELECT COUNT(*) FROM test.t1");
    test.try_query(test.maxscale->conn_rwsplit, "COMMIT");

    test.expect(!row.empty() && row[0] == "6",
                "Table should have 6 rows but has %s rows",
                row.empty() ? "no" : row[0].c_str());
    test.try_query(test.maxscale->conn_rwsplit, "DROP TABLE test.t1");
    test.maxscale->disconnect();

    unlink(filename);
    return test.global_result;
}
