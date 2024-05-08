# Diff - router for comparing servers

[TOC]

## Overview

The `diff`-router, hereafter referred to as _Diff_,
compares the behaviour of one MariaDB server version
to that of another.

Diff will send the workload both to the server currently
being used - called _main_ - and to another server - called
_other_ - whose behaviour needs to be assessed.

The responses from _main_ are returned to the client, without
waiting for the responses from _other_. The responses from _other_
are only compared to the responses from _main_, with discrepancies
in content or execution time subsequently logged.

Although Diff is a normal MaxScale router that can be
configured manually, typically it is created using commands
provided by the router itself. As its only purpose is to
compare the behaviour of different servers, it is only
meaningful to start it provided certain conditions are
fulfilled and those conditions are easily ensured using
the router itself.

## Setup

The behaviour and usage of Diff is most easily explained
using an example.

Consider the following simple configuration that only includes
the very essential for the example.
```
[MyServer1]
type=server
address=192.168.1.2
port=3306

[MyService]
type=service
router=readwritesplit
servers=MyServer1
...
```
There is a service `MyService` that uses a single server `MyServer1`,
which, for this example, is assumed to run MariaDB 10.5.

Suppose that the server should be upgraded to 11.2 and we want
to find out whether there would be some issues with that.

### Prerequisites

In order to use Diff for comparing the behaviour of MariaDB 10.5
and MariaDB 11.2, the following steps must be taken.

   * Install MariaDB 11.2 on a host that performance wise is
     similar to the host on which MariaDB 10.5 is running.
   * Configure the MariaDB 11.2 server to replicate from the
     MariaDB 10.5 server.
   * Create a server entry for the MariaDB 11.2 server in
     the MaxScale configuration.

The created entry could be something like:
```
[MariaDB_112]
type=server
address=192.168.1.3
port=3306
protocol=mariadbbackend
```

With these steps Diff is ready to be used.

### Running Diff

#### Prepare
```
usr/bin/maxctrl call command diff create DiffMyService MyService MyServer1 MariaDB_112
{
    "status": "Diff service 'DiffMyService' created. Server 'MariaDB_112' ready to be evaluated."
}
```
With this command, preparations for comparing the server `MariaDB_112`
against the server 'MyServer1' of the service `MyService` will be made.
At this point it will be checked in what kind of replication relationship
`MariaDB_112` is with respect to `MyServer1`.  If the steps in
[prerequisites](#prerequisites) were followed, it will be detected that
`MariaDB_112` replicates from `MyServer1`.

If everything seems to be in order, the service `DiffMyService` will be
created. Settings such as _user_ and _password_ that are needed by the
service `DiffMyService` will be copied from `MyService`.

Using maxctrl we can check that the service indeed has been created.
```
$ maxctrl list services
┌───────────────┬────────────────┬─────────────┬───────────────────┬────────────────────────┐
│ Service       │ Router         │ Connections │ Total Connections │ Targets                │
├───────────────┼────────────────┼─────────────┼───────────────────┼────────────────────────┤
│ MyService     │ readwritesplit │ 0           │ 0                 │ MyServer1              │
├───────────────┼────────────────┼─────────────┼───────────────────┼────────────────────────┤
│ DiffMyService │ diff           │ 0           │ 0                 │ MyServer1, MariaDB_112 │
└───────────────┴────────────────┴─────────────┴───────────────────┴────────────────────────┘
```

Now the comparison can be started.

#### Start
```
maxctrl call command diff start DiffMyService
{
    "sessions": {
        "suspended": 0,
        "total": 0
    },
    "state": "synchronizing",
    "sync_state": "suspending_sessions"
}
```
When Diff is started, it performs the following steps:

   1. All sessions of `MyService` are suspended.
   1. In the `MyService` service, the server target `MyServer1`
      is replaced with `DiffMyService`.
   1. The replication from `MyServer1` to `MariaDB_112` is stopped.
   1. The sessions are restarted, which will cause existing connections
      to `MyServer1` to be closed and new ones to be created, via Diff,
      to both `MyServer1` and `MariaDB_112`.
   1. The sessions are resumed, which means that the client traffic
      will continue.

In the first step, all sessions that are idle will immediately
be suspended, which simply means that nothing is read from
the client socket. Sessions that are waiting for a response
from the server and sessions that have an active transaction
continue to run. Immediately when a session becomes idle,
it is suspended.

Once all sessions have been suspended, the service is rewired.
In the case of `MyService` above, it means that the target
`MyServer1` is replaced with `DiffMyService`. That is, requests
that earlier were sent to `MyServer1`, will, once the sessions
are resumed, be sent to `DiffMyService`, which sends them forward
to both `MyServer1` and `MariaDB_112`.

Restarting the sessions means that the direct connections to
`MyServer1` will be closed and equivalent ones created via the
service `DiffMyService`, which will also create connections
to `MariaDB_112`.

When the sessions are resumed, client requests will again be
processed, but they will now be routed via `DiffMyService`.

With maxctrl we can can check that MyServer has been rewired.
```
$ maxctrl list services
┌───────────────┬────────────────┬─────────────┬───────────────────┬────────────────────────┐
│ Service       │ Router         │ Connections │ Total Connections │ Targets                │
├───────────────┼────────────────┼─────────────┼───────────────────┼────────────────────────┤
│ MyService     │ readwritesplit │ 0           │ 0                 │ DiffMyService          │
├───────────────┼────────────────┼─────────────┼───────────────────┼────────────────────────┤
│ DiffMyService │ diff           │ 0           │ 0                 │ MyServer1, MariaDB_112 │
└───────────────┴────────────────┴─────────────┴───────────────────┴────────────────────────┘
```
The target of `MyService` is `DiffMyService` instead of `MyServer1`
that it used to be.

The output object tells the current state.
```
{
    "sessions": {
        "suspended": 0,
        "total": 0
    },
    "state": "synchronizing",
    "sync_state": "suspending_sessions"
}
```
The `sessions` object shows how many sessions there are in total
and how many that currently are suspended. Since there were no
existing sessions in this example, they are both 0.

The `state` shows what Diff is currently doing. `synchronizing`
means that it is in the process of changing `MyService` to use
`DiffMyService`. `sync_state` shows that it is currently in the
process of suspending sessions.

#### Status

When Diff has been started, its current status can be checked with the
command `status`. The output is the same as what was returned when
Diff was started.
```
maxctrl call command diff status DiffMyService
{
    "sessions": {
        "suspended": 0,
        "total": 0
    },
    "state": "comparing",
    "sync_state": "not_applicable"
}
```
The state is now `comparing`, which means that everything is ready
and clients can connect in normal fashion.

#### Summary

While Diff is running, it is possible at any point to request
a summary.

```
usr/bin/maxctrl call command diff summary DiffMyService
OK
```
The summary consists of two files, one for the _main_ server and
one for the _other_ server. The files are written to a subdirectory
with the same name as the Diff service, which is created in the
subdirectory `diff` in the data directory of MaxScale.

Assuming the data directory is the default `/var/lib/maxscale`,
the directory would in this example be
`/var/lib/maxscale/diff/DiffMyService`.

The names of the files will be the server name, concatenated with a
timestamp. In this example, the names of the files could be:
```
MyServer1_2024-05-07_140323.json
MariaDB_112_2024-05-07_140323.json
```

The visualization of the results is done using the
[maxvisualize](#visualizing) program.

#### Stop

The comparison can stopped with the command `stop`.
```
maxctrl call command diff stop DiffMyService
{
    "sessions": {
        "suspended": 0,
        "total": 0
    },
    "state": "stopping",
    "sync_state": "suspending_sessions"
}
```
Stopping Diff reverses the effect of starting it:

   1. All sessions are suspended.
   1. In the service, 'DiffMyService' is replaced with 'MyServer1'.
   1. The sessions are restarted.
   1. The sessions are resumed.

As the sessions have to be suspended, it may take a while
before the operation has completed. The status can be checked with
the 'status' command.

#### Destroy

As the final step, the command `destroy` can be called to
destroy the service.
```
maxctrl call command diff destroy DiffMyService
OK
```

## Visualizing

The visualization itself is done with the `maxvisualize` program,
which is part of the Capture functionality. The visualization will
open up a browser window to show the visualization.

If no browser opens up, the visualization URL is also printed into
the command line which by default should be http://localhost:8866/.
```
maxvisualize baseline-summary.json comparison-summary.json
```

## Mode

Diff can run in a read-only or read-write mode and the mode is
deduced from the replication relationship between _main_ and
_other_.

If _other_ replicates from _main, it is assumed that _main_ is
the primary. In this case Diff will, when started, stop the
replication from _main_ to _other_. When the comparison ends
Diff will, depending on the value of
[reset_replication](#reset_replication)
either reset the replication from _main_ to _other_ or leave
the situation as it is.

If _other_ and _main_ replicates from a third seriver, it is
assumed _main_ is a replica. In this case, Diff will, when
started, leave the replication as it is and do nothing when
the comparison ends.

If the replication relationship between _main_ and _other_
is anything else, Diff will refuse to start.

## Configuration Parameters

### `main`

- **Type**: server
- **Mandatory**: Yes
- **Dynamic**: No

The main target from which results are returned to the client. Must be
a server and must be one of the servers listed in
[targets](../Getting-Started/Configuration-Guide.md#targets).

If the connection to the main target cannot be created or is lost
mid-session, the client connection will be closed.

### `service`

- **Type**: service
- **Mandatory**: Yes
- **Dynamic**: No

Specifies the service Diff will modify.

### `explain`

- **Type**: [enum](#enumerations)
- **Mandatory**: No
- **Dynamic**: Yes
- **Values**: `none`, `other`, `both'
- **Default**: `both`

Specifies whether a request should be EXPLAINed on only _other_,
both _other_ and _main_ or neither.

### `explain_entries`

- **Type**: non-negative integer
- **Mandatory**: No
- **Dynamic**: Yes
- **Default**: 2

Specifies how many times at most a particular canonical statement
is EXPLAINed during the period specified by
[explain_period](#explain_period).

### `explain_period`

- **Type**: [duration](../Getting-Started/Configuration-Guide.md#duration)
- **Mandatory**: No
- **Dynamic**: Yes
- **Default**: 15m

Specifies the length of the period during which at most
[explain_entries](#explain_entries) number of EXPLAINs are executed
for a statement.

### `max_request_lag`

- **Type**: non-negative integer
- **Mandatory**: No
- **Dynamic**: Yes
- **Default**: 10

Specifies the maximum number of requests _other_ may be lagging
behind _main_ before the execution of SELECTs against _other_
are skipped to bring it back in line with _main_.

### `on_error`

- **Type**: [enum](#enumerations)
- **Mandatory**: No
- **Dynamic**: Yes
- **Values**: `close`, `ignore`
- **Default**: `ignore`

Specifies whether an error from _other_, will cause the session to
be closed. By default it will not.

### `percentile`

- **Type**: count
- **Mandatory**: No
- **Dynamic**: Yes
- **Min**: 1
- **Max**: 100
- **Default**: 99

Specifies the percentile of sampels that will be considered when
calculating the width and number of bins of the histogram.

### `qps_window`

- **Type**: [duration](../Getting-Started/Configuration-Guide.md#duration)
- **Mandatory**: No
- **Dynamic**: No
- **Default**: 15m

Specifies the size of the sliding window during which QPS is calculated
and stored. When a [summary](#summary) is requested, the QPS information
will also be saved.

### `report`

- **Type**: [enum](#enumerations)
- **Mandatory**: No
- **Dynamic**: Yes
- **Values**: `always`, `on_discrepancy`
- **Default**: `on_discrepancy`

Specifies when the results of executing a statement on _other_ and _main_
should be logged; always or when there is a significant difference.

### `reset_replication`

- **Type**: [boolean](../Getting-Started/Configuration-Guide.md#booleans)
- **Mandatory**: No
- **Dynamic**: Yes
- **Default**: `true`

If Diff has started in read-write mode and the value of
`reset_replication` is `true`, when the comparison ends
it will execute the following on _other_:
```
RESET SLAVE
START SLAVE
```
If Diff has started in read-only mode, the value of `reset_replication`
will be ignored.

Note that since Diff writes updates directly to both _main_ and
_other_ there is no guarantee that it will be possible to simply
start the replication. Especially not if `gtid_strict_mode`
is on.

### `retain_faster_statements`

- **Type**: non-negative integer
- **Mandatory**: No
- **Dynamic**: Yes
- **Default**: 5

Specifies the number of faster statements that are retained in memory.
The statements will be saved in the summary when the comparison ends,
or when Diff is explicitly instructed to do so.

### `retain_slower_statements`

- **Type**: non-negative integer
- **Mandatory**: No
- **Dynamic**: Yes
- **Default**: 5

Specifies the number of slower statements that are retained in memory.
The statements will be saved in the summary when the comparison ends,
or when Diff is explicitly instructed to do so.

## Reporting

### Log

When Diff starts it will create a directory `diff` in MaxScale's
data directory (typically `/var/lib/maxscale`). Under that it
will create a directory whose name is the same as that of the
service specified in [service](#service). In the example above,
it would be `/var/lib/maxscale/diff/MyService`.

In that directory it will create a file whose name is formed
from the name of _main_, the name of _other_ and a timestamp.
In the example above, it could be
`MyServer1_MariaDB_112_2024-02-15T15-28-38.json`.

Each line (here expanded for readability) will look like:
```
{
  "id": 1,
  "session": 1,
  "command": "COM_QUERY",
  "query": "select @@version_comment limit 1",
  "results": [
    {
      "target": "MyServer1",
      "checksum": "0f491b37",
      "rows": 1,
      "warnings": 0,
      "duration": 257805,
      "type": "resultset",
      "explain": {
        "query_block": {
          "select_id": 1,
          "table": {
            "message": "No tables used"
          }
        }
      }
    },
    {
      "target": "MariaDB_112",
      "checksum": "0f491b37",
      "rows": 1,
      "warnings": 0,
      "duration": 170043,
      "type": "resultset",
      "explain": {
        "query_block": {
          "select_id": 1,
          "table": {
            "message": "No tables used"
          }
        }
      }
    }
  ]
}
```
The meaning of the fields are as follows:

* **id**: Running number, increases for each query.
* **session**: The session id.
* **command**: The protocol packet type.
* **query**: The SQL of the query.
* **results**: Array of results.
   * **target**: The server the result relates to.
   * **checksum**: The checksum of the result.
   * **rows**: How many rows were returned.
   * **warning**: The number of warnings.
   * **duration**: The execution duration in nanonseconds.
   * **type**: What type of result `resultset`, `ok` or `error`.
   * **explain**: The result of `EXPLAIN FORMAT=JSON statement`.

### Summary

When Diff is stopped, it will write a summary to the same
directory as the log. The summary will be written to file
whose name is `Summary-' followed by a timestamp.

## Limitations

Diff is currently not capable of adapting to any changes made in
the cluster configuration. For instance, if Diff starts up in
read-only mode and _main_ is subsequently made _primary_, Diff
will not sever the replication from _main_ to _other_. The result
will be that _other_ receives the same writes twice; once via the
replication from the server it is replicating from and once when
Diff executes the same writes.
