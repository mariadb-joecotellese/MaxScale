# Server Resource

A server resource represents a backend database server.

[TOC]

## Resource Operations

The _:name_ in all of the URIs must be the name of a server in MaxScale.

### Get a server

```
GET /v1/servers/:name
```

Get a single server.

#### Response

`Status: 200 OK`

```javascript
{
    "data": {
        "attributes": {
            "gtid_binlog_pos": "0-3000-8",
            "gtid_current_pos": "0-3000-8",
            "last_event": "master_up",
            "lock_held": null,
            "master_group": null,
            "master_id": -1,
            "name": "server1",
            "node_id": 3000,
            "parameters": {
                "address": "127.0.0.1",
                "disk_space_threshold": null,
                "extra_port": 0,
                "max_routing_connections": 0,
                "monitorpw": null,
                "monitoruser": null,
                "persistmaxtime": "0ms",
                "persistpoolmax": 0,
                "port": 3000,
                "priority": 0,
                "private_address": null,
                "proxy_protocol": false,
                "rank": "primary",
                "replication_custom_options": null,
                "socket": null,
                "ssl": false,
                "ssl_ca": null,
                "ssl_cert": null,
                "ssl_cert_verify_depth": 9,
                "ssl_cipher": null,
                "ssl_key": null,
                "ssl_verify_peer_certificate": false,
                "ssl_verify_peer_host": false,
                "ssl_version": "MAX"
            },
            "read_only": false,
            "replication_lag": 0,
            "server_id": 3000,
            "slave_connections": [],
            "source": {
                "file": "/etc/maxscale.cnf",
                "type": "static"
            },
            "state": "Master, Running",
            "state_details": null,
            "statistics": {
                "active_operations": 0,
                "adaptive_avg_select_time": "0ns",
                "connection_pool_empty": 0,
                "connections": 1,
                "failed_auths": 0,
                "max_connections": 1,
                "max_pool_size": 0,
                "persistent_connections": 0,
                "response_time_distribution": {
                    "read": {
                        "distribution": [
                            {
                                "count": 0,
                                "time": "0.000001",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "0.000010",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "0.000100",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "0.001000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "0.010000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "0.100000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "1.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "10.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "100.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "1000.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "10000.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "100000.000000",
                                "total": 0.0
                            }
                        ],
                        "operation": "read",
                        "range_base": 10
                    },
                    "write": {
                        "distribution": [
                            {
                                "count": 0,
                                "time": "0.000001",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "0.000010",
                                "total": 0.0
                            },
                            {
                                "count": 1,
                                "time": "0.000100",
                                "total": 9.0147000000000003e-5
                            },
                            {
                                "count": 3,
                                "time": "0.001000",
                                "total": 0.00131908
                            },
                            {
                                "count": 0,
                                "time": "0.010000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "0.100000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "1.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "10.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "100.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "1000.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "10000.000000",
                                "total": 0.0
                            },
                            {
                                "count": 0,
                                "time": "100000.000000",
                                "total": 0.0
                            }
                        ],
                        "operation": "write",
                        "range_base": 10
                    }
                },
                "reused_connections": 0,
                "routed_packets": 4,
                "total_connections": 1
            },
            "triggered_at": "Fri, 05 Jan 2024 07:23:54 GMT",
            "uptime": 2372,
            "version_string": "10.6.15-MariaDB-1:10.6.15+maria~ubu2004-log"
        },
        "id": "server1",
        "links": {
            "self": "http://localhost:8989/v1/servers/server1/"
        },
        "relationships": {
            "monitors": {
                "data": [
                    {
                        "id": "MariaDB-Monitor",
                        "type": "monitors"
                    }
                ],
                "links": {
                    "related": "http://localhost:8989/v1/monitors/",
                    "self": "http://localhost:8989/v1/servers/server1/relationships/monitors/"
                }
            },
            "services": {
                "data": [
                    {
                        "id": "RW-Split-Router",
                        "type": "services"
                    },
                    {
                        "id": "Read-Connection-Router",
                        "type": "services"
                    }
                ],
                "links": {
                    "related": "http://localhost:8989/v1/services/",
                    "self": "http://localhost:8989/v1/servers/server1/relationships/services/"
                }
            }
        },
        "type": "servers"
    },
    "links": {
        "self": "http://localhost:8989/v1/servers/server1/"
    }
}
```

### Get all servers

```
GET /v1/servers
```

#### Response

Response contains a resource collection with all servers.

`Status: 200 OK`

```javascript
{
    "data": [
        {
            "attributes": {
                "gtid_binlog_pos": "0-3000-8",
                "gtid_current_pos": "0-3000-8",
                "last_event": "master_up",
                "lock_held": null,
                "master_group": null,
                "master_id": -1,
                "name": "server1",
                "node_id": 3000,
                "parameters": {
                    "address": "127.0.0.1",
                    "disk_space_threshold": null,
                    "extra_port": 0,
                    "max_routing_connections": 0,
                    "monitorpw": null,
                    "monitoruser": null,
                    "persistmaxtime": "0ms",
                    "persistpoolmax": 0,
                    "port": 3000,
                    "priority": 0,
                    "private_address": null,
                    "proxy_protocol": false,
                    "rank": "primary",
                    "replication_custom_options": null,
                    "socket": null,
                    "ssl": false,
                    "ssl_ca": null,
                    "ssl_cert": null,
                    "ssl_cert_verify_depth": 9,
                    "ssl_cipher": null,
                    "ssl_key": null,
                    "ssl_verify_peer_certificate": false,
                    "ssl_verify_peer_host": false,
                    "ssl_version": "MAX"
                },
                "read_only": false,
                "replication_lag": 0,
                "server_id": 3000,
                "slave_connections": [],
                "source": {
                    "file": "/etc/maxscale.cnf",
                    "type": "static"
                },
                "state": "Master, Running",
                "state_details": null,
                "statistics": {
                    "active_operations": 0,
                    "adaptive_avg_select_time": "0ns",
                    "connection_pool_empty": 0,
                    "connections": 1,
                    "failed_auths": 0,
                    "max_connections": 1,
                    "max_pool_size": 0,
                    "persistent_connections": 0,
                    "response_time_distribution": {
                        "read": {
                            "distribution": [
                                {
                                    "count": 0,
                                    "time": "0.000001",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.000010",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.000100",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.001000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.010000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.100000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100000.000000",
                                    "total": 0.0
                                }
                            ],
                            "operation": "read",
                            "range_base": 10
                        },
                        "write": {
                            "distribution": [
                                {
                                    "count": 0,
                                    "time": "0.000001",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.000010",
                                    "total": 0.0
                                },
                                {
                                    "count": 1,
                                    "time": "0.000100",
                                    "total": 9.0147000000000003e-5
                                },
                                {
                                    "count": 3,
                                    "time": "0.001000",
                                    "total": 0.00131908
                                },
                                {
                                    "count": 0,
                                    "time": "0.010000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.100000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100000.000000",
                                    "total": 0.0
                                }
                            ],
                            "operation": "write",
                            "range_base": 10
                        }
                    },
                    "reused_connections": 0,
                    "routed_packets": 4,
                    "total_connections": 1
                },
                "triggered_at": "Fri, 05 Jan 2024 07:23:54 GMT",
                "uptime": 2372,
                "version_string": "10.6.15-MariaDB-1:10.6.15+maria~ubu2004-log"
            },
            "id": "server1",
            "links": {
                "self": "http://localhost:8989/v1/servers/server1/"
            },
            "relationships": {
                "monitors": {
                    "data": [
                        {
                            "id": "MariaDB-Monitor",
                            "type": "monitors"
                        }
                    ],
                    "links": {
                        "related": "http://localhost:8989/v1/monitors/",
                        "self": "http://localhost:8989/v1/servers/server1/relationships/monitors/"
                    }
                },
                "services": {
                    "data": [
                        {
                            "id": "RW-Split-Router",
                            "type": "services"
                        },
                        {
                            "id": "Read-Connection-Router",
                            "type": "services"
                        }
                    ],
                    "links": {
                        "related": "http://localhost:8989/v1/services/",
                        "self": "http://localhost:8989/v1/servers/server1/relationships/services/"
                    }
                }
            },
            "type": "servers"
        },
        {
            "attributes": {
                "gtid_binlog_pos": "0-3001-12",
                "gtid_current_pos": "0-3001-12",
                "last_event": "lost_slave",
                "lock_held": null,
                "master_group": null,
                "master_id": 3000,
                "name": "server2",
                "node_id": 3001,
                "parameters": {
                    "address": "127.0.0.1",
                    "disk_space_threshold": null,
                    "extra_port": 0,
                    "max_routing_connections": 0,
                    "monitorpw": null,
                    "monitoruser": null,
                    "persistmaxtime": "0ms",
                    "persistpoolmax": 0,
                    "port": 3001,
                    "priority": 0,
                    "private_address": null,
                    "proxy_protocol": false,
                    "rank": "primary",
                    "replication_custom_options": null,
                    "socket": null,
                    "ssl": false,
                    "ssl_ca": null,
                    "ssl_cert": null,
                    "ssl_cert_verify_depth": 9,
                    "ssl_cipher": null,
                    "ssl_key": null,
                    "ssl_verify_peer_certificate": false,
                    "ssl_verify_peer_host": false,
                    "ssl_version": "MAX"
                },
                "read_only": false,
                "replication_lag": -1,
                "server_id": 3001,
                "slave_connections": [
                    {
                        "connection_name": "",
                        "gtid_io_pos": "",
                        "last_io_error": "",
                        "last_sql_error": "",
                        "master_host": "127.0.0.1",
                        "master_port": 3000,
                        "master_server_id": 3000,
                        "seconds_behind_master": null,
                        "slave_io_running": "No",
                        "slave_sql_running": "No",
                        "using_gtid": "No"
                    }
                ],
                "source": {
                    "file": "/etc/maxscale.cnf",
                    "type": "static"
                },
                "state": "Running",
                "state_details": null,
                "statistics": {
                    "active_operations": 0,
                    "adaptive_avg_select_time": "0ns",
                    "connection_pool_empty": 0,
                    "connections": 0,
                    "failed_auths": 0,
                    "max_connections": 1,
                    "max_pool_size": 0,
                    "persistent_connections": 0,
                    "response_time_distribution": {
                        "read": {
                            "distribution": [
                                {
                                    "count": 0,
                                    "time": "0.000001",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.000010",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.000100",
                                    "total": 0.0
                                },
                                {
                                    "count": 1,
                                    "time": "0.001000",
                                    "total": 0.00037632399999999998
                                },
                                {
                                    "count": 0,
                                    "time": "0.010000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.100000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100000.000000",
                                    "total": 0.0
                                }
                            ],
                            "operation": "read",
                            "range_base": 10
                        },
                        "write": {
                            "distribution": [
                                {
                                    "count": 0,
                                    "time": "0.000001",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.000010",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.000100",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.001000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.010000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "0.100000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "1000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "10000.000000",
                                    "total": 0.0
                                },
                                {
                                    "count": 0,
                                    "time": "100000.000000",
                                    "total": 0.0
                                }
                            ],
                            "operation": "write",
                            "range_base": 10
                        }
                    },
                    "reused_connections": 0,
                    "routed_packets": 1,
                    "total_connections": 1
                },
                "triggered_at": "Fri, 05 Jan 2024 07:24:07 GMT",
                "uptime": 2372,
                "version_string": "10.6.15-MariaDB-1:10.6.15+maria~ubu2004-log"
            },
            "id": "server2",
            "links": {
                "self": "http://localhost:8989/v1/servers/server2/"
            },
            "relationships": {
                "monitors": {
                    "data": [
                        {
                            "id": "MariaDB-Monitor",
                            "type": "monitors"
                        }
                    ],
                    "links": {
                        "related": "http://localhost:8989/v1/monitors/",
                        "self": "http://localhost:8989/v1/servers/server2/relationships/monitors/"
                    }
                },
                "services": {
                    "data": [
                        {
                            "id": "RW-Split-Router",
                            "type": "services"
                        },
                        {
                            "id": "Read-Connection-Router",
                            "type": "services"
                        }
                    ],
                    "links": {
                        "related": "http://localhost:8989/v1/services/",
                        "self": "http://localhost:8989/v1/servers/server2/relationships/services/"
                    }
                }
            },
            "type": "servers"
        }
    ],
    "links": {
        "self": "http://localhost:8989/v1/servers/"
    }
}
```

### Create a server

```
POST /v1/servers
```

Create a new server by defining the resource. The posted object must define at
least the following fields.

* `data.id`
  * Name of the server

* `data.type`
  * Type of the object, must be `servers`

* `data.attributes.parameters.address` OR `data.attributes.parameters.socket`
  * The [`address`](../Getting-Started/Configuration-Guide.md#address) or
    [`socket`](../Getting-Started/Configuration-Guide.md#socket) to use. Only
    one of the fields can be defined.

* `data.attributes.parameters.port`
  * The [`port`](../Getting-Started/Configuration-Guide.md#port) to use. Needs
    to be defined if the `address` field is defined.

The following is the minimal required JSON object for defining a new server.

```javascript
{
    "data": {
        "id": "server3",
        "type": "servers",
        "attributes": {
            "parameters": {
                "address": "127.0.0.1",
                "port": 3003
            }
        }
    }
}
```

The relationships of a server can also be defined at creation time. This allows
new servers to be created and immediately taken into use.

```javascript
{
    "data": {
        "id": "server4",
        "type": "servers",
        "attributes": {
            "parameters": {
                "address": "127.0.0.1",
                "port": 3002
            }
        },
        "relationships": {
            "services": {
                "data": [
                    {
                        "id": "RW-Split-Router",
                        "type": "services"
                    },
                    {
                        "id": "Read-Connection-Router",
                        "type": "services"
                    }
                ]
            },
            "monitors": {
                "data": [
                    {
                        "id": "MySQL-Monitor",
                        "type": "monitors"
                    }
                ]
            }
        }
    }
}
```

Refer to the [Configuration Guide](../Getting-Started/Configuration-Guide.md)
for a full list of server parameters.

#### Response

Server created:

`Status: 204 No Content`

Invalid JSON body:

`Status: 400 Bad Request`

### Update a server

```
PATCH /v1/servers/:name
```

The request body must be a valid JSON document representing the modified
server.

### Modifiable Fields

In addition to the server
[parameters](../Getting-Started/Configuration-Guide.md#server-1), the _services_
and _monitors_ fields of the _relationships_ object can be modified. Removal,
addition and modification of the links will change which service and monitors
use this server.

For example, removing the first value in the _services_ list in the
_relationships_ object from the following JSON document will remove the
_server1_ from the service _RW-Split-Router_.

Removing a service from a server is analogous to removing the server from the
service. Both unlink the two objects from each other.

Request for `PATCH /v1/servers/server1` that modifies the address of the server:

```javascript
{
    "data": {
        "attributes": {
            "parameters": {
                "address": "192.168.0.123"
            }
        }
    }
}
```

Request for `PATCH /v1/servers/server1` that modifies the server relationships:

```javascript
{
    "data": {
        "relationships": {
            "services": {
                "data": [
                    { "id": "Read-Connection-Router", "type": "services" }
                ]
            },
            "monitors": {
                "data": [
                    { "id": "MySQL-Monitor", "type": "monitors" }
                ]
            }
        }
    }
}
```

If parts of the resource are not defined (e.g. the `attributes` field in the
above example), those parts of the resource are not modified. All parts that are
defined are interpreted as the new definition of those part of the resource. In
the above example, the `relationships` of the resource are completely redefined.

#### Response

Server modified:

`Status: 204 No Content`

Invalid JSON body:

`Status: 400 Bad Request`

### Update server relationships

```
PATCH /v1/servers/:name/relationships/:type
```

The _:type_ in the URI must be either _services_, for service
relationships, or _monitors_, for monitor relationships.

The request body must be a JSON object that defines only the _data_ field. The
value of the _data_ field must be an array of relationship objects that define
the _id_ and _type_ fields of the relationship. This object will replace the
existing relationships of the particular type from the server.

The following is an example request and request body that defines a single
service relationship for a server.

```
PATCH /v1/servers/my-db-server/relationships/services

{
    data: [
          { "id": "my-rwsplit-service", "type": "services" }
    ]
}
```

All relationships for a server can be deleted by sending an empty array as the
_data_ field value. The following example removes the server from all services.

```
PATCH /v1/servers/my-db-server/relationships/services

{
    data: []
}
```

#### Response

Server relationships modified:

`Status: 204 No Content`

Invalid JSON body:

`Status: 400 Bad Request`

### Destroy a server

```
DELETE /v1/servers/:name
```

A server can only be deleted if it is not used by any services or
monitors.

This endpoint also supports the `force=yes` parameter that will unconditionally
delete the server by first unlinking it from all services and monitors that use
it.

#### Response

Server is destroyed:

`Status: 204 No Content`

Server is in use:

`Status: 400 Bad Request`

### Set server state

```
PUT /v1/servers/:name/set
```

This endpoint requires that the `state` parameter is passed with the
request. The value of `state` must be one of the following values.

|Value      | State Description                |
|-----------|----------------------------------|
|master     | Server is a Master               |
|slave      | Server is a Slave                |
|maintenance| Server is put into maintenance   |
|running    | Server is up and running         |
|synced     | Server is a Galera node          |
|drain      | Server is drained of connections |

For example, to set the server _db-server-1_ into maintenance mode, a request to
the following URL must be made:

```
PUT /v1/servers/db-server-1/set?state=maintenance
```

This endpoint also supports the `force=yes` parameter that will cause all
connections to the server to be closed if `state=maintenance` is also set. By
default setting a server into maintenance mode will cause connections to be
closed only after the next request is sent.

The following example forcefully closes all connections to server _db-server-1_
and sets it into maintenance mode:

```
PUT /v1/servers/db-server-1/set?state=maintenance&force=yes
```

#### Response

Server state modified:

`Status: 204 No Content`

Missing or invalid parameter:

`Status: 400 Bad Request`

### Clear server state

```
PUT /v1/servers/:name/clear
```

This endpoint requires that the `state` parameter is passed with the
request. The value of `state` must be one of the values defined in the
_set_ endpoint documentation.

#### Response

Server state modified:

`Status: 204 No Content`

Missing or invalid parameter:

`Status: 400 Bad Request`
