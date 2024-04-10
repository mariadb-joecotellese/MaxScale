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

#include "test_base.hh"
#include <iostream>
#include <iomanip>
#include <maxtest/testconnections.hh>
#include <maxtest/galera_cluster.hh>

const size_t NUM_GALERAS = 4;

std::string replicating_from(Connection& conn)
{
    std::string addr;

    const auto& rows = conn.rows("SHOW SLAVE STATUS");
    if (!rows.empty() && rows[0].size() >= 2)
    {
        addr = rows[0][1];
    }

    return addr;
}

void block_galera_ip(TestConnections& test, const std::string& galera_ip)
{
    size_t i = 0;
    for (; i < NUM_GALERAS; ++i)
    {
        if (galera_ip == test.galera->ip(i))
        {
            break;
        }
    }

    if (i == NUM_GALERAS)
    {
        test.add_result(true, "Expected IP '%s' to be a galera node\n", galera_ip.c_str());
    }
    else
    {
        std::cout << "Blocking node " << i << " IP " << test.galera->ip(i) << std::endl;
        test.galera->block_node(i);
    }
}

void check_table(TestConnections& test, Connection& conn, int n)
{
    int m = 0;

    for (int i = 0; i < 30; i++)
    {
        auto result = conn.field("SELECT COUNT(*) FROM test.t1");
        m = atoi(result.c_str());

        if (n == m)
        {
            break;
        }
        else
        {
            sleep(1);
        }
    }

    test.expect(n == m, "test.t1 should have %d rows, but has %d rows.", n, m);
}

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    test.galera->connect();
    auto galera_ids = test.galera->get_all_server_ids_str();

    Connection rws = test.maxscale->rwsplit();
    test.expect(rws.connect(), "RWS connection should work: %s", rws.error());
    rws.query("FLUSH LOGS");
    auto gtid_pos = rws.field("SELECT @@gtid_binlog_pos, @@last_insert_id", 0);

    Connection pinloki = test.maxscale->readconn_master();
    test.expect(pinloki.connect(), "Pinloki connection should work: %s", pinloki.error());

    pinloki.query("STOP SLAVE");
    pinloki.query("SET @@global.gtid_slave_pos = '" + gtid_pos + "'");
    pinloki.query("START SLAVE");

    // Pick a regular replica and make it replicate from pinloki
    Connection pinloki_replica {test.repl->get_connection(2)};
    test.expect(pinloki_replica.connect(), "Regular replica connection should work: %s",
                pinloki_replica.error());

    std::cout << "pinloki_replica " << pinloki_replica.host() << std::endl;

    // and make it replicate from pinloki.
    pinloki_replica.query("STOP SLAVE");
    pinloki_replica.query("RESET SLAVE");
    pinloki_replica.query("SET @@global.gtid_slave_pos = '" + gtid_pos + "'");
    pinloki_replica.query(change_master_sql(pinloki.host().c_str(), pinloki.port()));
    pinloki_replica.query("START SLAVE");

    // Create a table via RWS (galera cluster) and insert one value
    rws.query("DROP TABLE if exists test.t1");
    test.expect(rws.query("CREATE TABLE test.t1(id INT)"), "CREATE failed: %s", rws.error());
    test.expect(rws.query("INSERT INTO test.t1 values(1)"), "INSERT 1 failed: %s", rws.error());

    // Check that things are as they should be.
    // The pinloki_replica should replicate from pinloki
    auto reg_repl_from = replicating_from(pinloki_replica);
    test.expect(reg_repl_from == pinloki.host().c_str(), "pinloki_replica should replicate from pinloki");

    // Reading test.t1 from pinloki_replica should have 1 row
    check_table(test, pinloki_replica, 1);

    auto pinloki_repl_from = replicating_from(pinloki);
    std::cout << "replicating_from(pinloki) = " << pinloki_repl_from << std::endl;
    std::cout << "replicating_from(pinloki_replica) = " << replicating_from(pinloki_replica) << std::endl;

    auto previous_ip = pinloki_repl_from;

    /** Block the node pinloki is replicating from */
    block_galera_ip(test, pinloki_repl_from);

    /** Make sure pinloki is now replicating from another node */
    for (int i = 0; i < 60; ++i)    // TODO, takes long, ~30s. What are the timeouts?
    {
        pinloki_repl_from = replicating_from(pinloki);
        std::cout << "replicating_from(pinloki) = " << pinloki_repl_from << std::endl;
        if (previous_ip != pinloki_repl_from)
        {
            break;
        }

        sleep(1);
    }

    test.expect(previous_ip != pinloki_repl_from,
                "pinloki should have started to replicate from another node");

    /** Reconnect, insert and check */
    test.expect(rws.connect(), "2nd RWS connection should work: %s", rws.error());
    test.expect(rws.query("INSERT INTO test.t1 values(2)"), "INSERT 2 failed: %s", rws.error());

    check_table(test, pinloki_replica, 2);

    return test.global_result;
}
