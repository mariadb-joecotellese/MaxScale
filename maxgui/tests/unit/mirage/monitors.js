/*
 * Copyright (c) 2020 MariaDB Corporation Ab
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
//TODO: Replace with helper function to auto generate it
const dummy_all_monitors = [
    {
        attributes: {
            module: 'mariadbmon',
            monitor_diagnostics: {
                master: 'server_0',
                master_gtid_domain_id: 0,
                primary: null,
                server_info: [
                    {
                        gtid_binlog_pos: '0-1000-9',
                        gtid_current_pos: '0-1000-9',
                        lock_held: null,
                        master_group: null,
                        name: 'server_0',
                        read_only: false,
                        server_id: 1000,
                        slave_connections: [],
                    },
                    {
                        gtid_binlog_pos: '0-1000-9',
                        gtid_current_pos: '0-1000-9',
                        lock_held: null,
                        master_group: null,
                        name: 'server_1_with_longgggggggggggggggggggggggggggggggggg_name',
                        read_only: false,
                        server_id: 1001,
                        slave_connections: [
                            {
                                connection_name: '',
                                gtid_io_pos: '0-1000-9',
                                last_io_error: '',
                                last_sql_error: '',
                                master_host: '127.0.0.1',
                                master_port: 4000,
                                master_server_id: 1000,
                                seconds_behind_master: 0,
                                slave_io_running: 'Yes',
                                slave_sql_running: 'Yes',
                            },
                        ],
                    },
                ],
                state: 'Idle',
            },
            parameters: {
                assume_unique_hostnames: true,
                auto_failover: true,
                auto_rejoin: true,
                backend_connect_attempts: 1,
                backend_connect_timeout: 3,
                backend_read_timeout: 3,
                backend_write_timeout: 3,
                cooperative_monitoring_locks: 'none',
                demotion_sql_file: null,
                detect_replication_lag: false,
                detect_stale_master: null,
                detect_stale_slave: null,
                detect_standalone_master: null,
                disk_space_check_interval: 0,
                disk_space_threshold: null,
                enforce_read_only_slaves: false,
                enforce_simple_topology: false,
                enforce_writable_master: false,
                events: 'all',
                failcount: 3,
                failover_timeout: 90,
                handle_events: true,
                ignore_external_masters: false,
                journal_max_age: 28800,
                maintenance_on_low_disk_space: true,
                master_conditions: 'primary_monitor_master',
                master_failure_timeout: 10,
                module: 'mariadbmon',
                monitor_interval: 2000,
                password: '*****',
                promotion_sql_file: null,
                replication_master_ssl: false,
                replication_password: null,
                replication_user: null,
                script: null,
                script_max_replication_lag: -1,
                script_timeout: 90,
                servers_no_promotion: null,
                slave_conditions: 'none',
                switchover_on_low_disk_space: false,
                switchover_timeout: 90,
                user: 'maxskysql',
                verify_master_failure: true,
            },
            state: 'Running',
            ticks: 4053,
        },
        id: 'Monitor',
        relationships: {
            servers: {
                data: [
                    {
                        id: 'server_0',
                        type: 'servers',
                    },
                    {
                        id: 'server_1_with_longgggggggggggggggggggggggggggggggggg_name',
                        type: 'servers',
                    },
                ],
            },
        },
        type: 'monitors',
    },
]
export const initAllMonitors = api => {
    dummy_all_monitors.forEach(monitor => api.create('monitor', monitor))
}
