/*
 * Copyright (c) 2020 MariaDB Corporation Ab
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
//TODO: Replace with helper function to auto generate it
const dummy_all_servers = [
    {
        attributes: {
            gtid_binlog_pos: '0-1000-9',
            gtid_current_pos: '0-1000-9',
            last_event: 'master_up',
            lock_held: null,
            master_group: null,
            master_id: -1,
            name: 'server_0',
            node_id: 1000,
            parameters: {
                address: '127.0.0.1',
                disk_space_threshold: null,
                extra_port: 0,
                monitorpw: null,
                monitoruser: null,
                persistmaxtime: '0ms',
                persistpoolmax: 0,
                port: 4000,
                priority: 0,
                proxy_protocol: false,
                rank: 'primary',
                socket: null,
                ssl: false,
                ssl_ca_cert: null,
                ssl_cert: null,
                ssl_cert_verify_depth: 9,
                ssl_cipher: null,
                ssl_key: null,
                ssl_verify_peer_certificate: false,
                ssl_verify_peer_host: false,
                ssl_version: 'MAX',
            },
            read_only: false,
            replication_lag: 0,
            server_id: 1000,
            slave_connections: [],
            state: 'Master, Running',
            state_details: null,
            statistics: {
                active_operations: 0,
                adaptive_avg_select_time: '0ns',
                connection_pool_empty: 0,
                connections: 0,
                max_connections: 0,
                max_pool_size: 0,
                persistent_connections: 0,
                response_time_distribution: {
                    read: {
                        distribution: [
                            {
                                count: 0,
                                time: '0.000001',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000010',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000100',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.001000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.010000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.100000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100000.000000',
                                total: 0.0,
                            },
                        ],
                        operation: 'read',
                        range_base: 10,
                    },
                    write: {
                        distribution: [
                            {
                                count: 0,
                                time: '0.000001',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000010',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000100',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.001000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.010000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.100000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100000.000000',
                                total: 0.0,
                            },
                        ],
                        operation: 'write',
                        range_base: 10,
                    },
                },
                reused_connections: 0,
                routed_packets: 0,
                total_connections: 0,
            },
            triggered_at: 'Mon, 20 Sep 2021 15:50:53 GMT',
            version_string: '10.4.15-MariaDB-1:10.4.15+maria~focal-log',
        },
        id: 'server_0',
        relationships: {
            monitors: {
                data: [
                    {
                        id: 'Monitor',
                        type: 'monitors',
                    },
                ],
            },
            services: {
                data: [
                    {
                        id: 'Read-Only-Service',
                        type: 'services',
                    },
                    {
                        id: 'Read-Write-Service',
                        type: 'services',
                    },
                ],
            },
        },
        type: 'servers',
    },
    {
        attributes: {
            gtid_binlog_pos: '0-1000-9',
            gtid_current_pos: '0-1000-9',
            last_event: 'slave_up',
            lock_held: null,
            master_group: null,
            master_id: 1000,
            name: 'server_1_with_longgggggggggggggggggggggggggggggggggg_name',
            node_id: 1001,
            parameters: {
                address: '127.0.0.1',
                disk_space_threshold: null,
                extra_port: 0,
                monitorpw: null,
                monitoruser: null,
                persistmaxtime: '0ms',
                persistpoolmax: 0,
                port: 4001,
                priority: 0,
                proxy_protocol: false,
                rank: 'primary',
                socket: null,
                ssl: false,
                ssl_ca_cert: null,
                ssl_cert: null,
                ssl_cert_verify_depth: 9,
                ssl_cipher: null,
                ssl_key: null,
                ssl_verify_peer_certificate: false,
                ssl_verify_peer_host: false,
                ssl_version: 'MAX',
            },
            read_only: false,
            replication_lag: 0,
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
            state: 'Slave, Running',
            state_details: null,
            statistics: {
                active_operations: 0,
                adaptive_avg_select_time: '0ns',
                connection_pool_empty: 0,
                connections: 0,
                max_connections: 0,
                max_pool_size: 0,
                persistent_connections: 0,
                response_time_distribution: {
                    read: {
                        distribution: [
                            {
                                count: 0,
                                time: '0.000001',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000010',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000100',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.001000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.010000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.100000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100000.000000',
                                total: 0.0,
                            },
                        ],
                        operation: 'read',
                        range_base: 10,
                    },
                    write: {
                        distribution: [
                            {
                                count: 0,
                                time: '0.000001',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000010',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000100',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.001000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.010000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.100000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100000.000000',
                                total: 0.0,
                            },
                        ],
                        operation: 'write',
                        range_base: 10,
                    },
                },
                reused_connections: 0,
                routed_packets: 0,
                total_connections: 0,
            },
            triggered_at: 'Mon, 20 Sep 2021 15:50:53 GMT',
            version_string: '10.4.15-MariaDB-1:10.4.15+maria~focal-log',
        },
        id: 'server_1_with_longgggggggggggggggggggggggggggggggggg_name',
        relationships: {
            monitors: {
                data: [
                    {
                        id: 'Monitor',
                        type: 'monitors',
                    },
                ],
            },
            services: {
                data: [
                    {
                        id: 'Read-Only-Service',
                        type: 'services',
                    },
                ],
            },
        },
        type: 'servers',
    },
    {
        attributes: {
            parameters: {
                address: '127.0.0.1',
                disk_space_threshold: null,
                extra_port: 0,
                monitorpw: null,
                monitoruser: null,
                persistmaxtime: '0ms',
                persistpoolmax: 0,
                port: 4002,
                priority: 0,
                proxy_protocol: false,
                rank: 'primary',
                socket: null,
                ssl: false,
                ssl_ca_cert: null,
                ssl_cert: null,
                ssl_cert_verify_depth: 9,
                ssl_cipher: null,
                ssl_key: null,
                ssl_verify_peer_certificate: false,
                ssl_verify_peer_host: false,
                ssl_version: 'MAX',
            },
            replication_lag: -1,
            state: 'Down',
            statistics: {
                active_operations: 0,
                adaptive_avg_select_time: '0ns',
                connection_pool_empty: 0,
                connections: 0,
                max_connections: 0,
                max_pool_size: 0,
                persistent_connections: 0,
                response_time_distribution: {
                    read: {
                        distribution: [
                            {
                                count: 0,
                                time: '0.000001',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000010',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000100',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.001000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.010000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.100000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100000.000000',
                                total: 0.0,
                            },
                        ],
                        operation: 'read',
                        range_base: 10,
                    },
                    write: {
                        distribution: [
                            {
                                count: 0,
                                time: '0.000001',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000010',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.000100',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.001000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.010000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '0.100000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '1000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '10000.000000',
                                total: 0.0,
                            },
                            {
                                count: 0,
                                time: '100000.000000',
                                total: 0.0,
                            },
                        ],
                        operation: 'write',
                        range_base: 10,
                    },
                },
                reused_connections: 0,
                routed_packets: 0,
                total_connections: 0,
            },
            version_string: '',
        },
        id: 'server_2',
        relationships: {},
        type: 'servers',
    },
]
export const initAllServers = api => {
    dummy_all_servers.forEach(server => api.create('server', server))
}
