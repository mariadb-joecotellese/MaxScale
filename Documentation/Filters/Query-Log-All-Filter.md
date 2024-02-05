# Query Log All Filter

[TOC]

## Overview

The Query Log All (QLA) filter logs query content. Logs are written to a file in
CSV format. Log elements are configurable and include the time submitted and the
SQL statement text, among others.

## Configuration

A minimal configuration is below.
```
[MyLogFilter]
type=filter
module=qlafilter
filebase=/tmp/SqlQueryLog

[MyService]
type=service
router=readconnroute
servers=server1
user=myuser
password=mypasswd
filters=MyLogFilter
```

## Log Rotation

The `qlafilter` logs can be rotated by executing the `maxctrl rotate logs`
command. This will cause the log files to be reopened when the next message is
written to the file. This applies to both unified and session type logging.

## Filter Parameters

The QLA filter has one mandatory parameter, `filebase`, and a number of optional
parameters. These were introduced in the 1.0 release of MariaDB MaxScale.

### `filebase`

The basename of the output file created for each session. A session index is
added to the filename for each written session file. For unified log files,
*.unified* is appended. This is a mandatory parameter.

```
filebase=/tmp/SqlQueryLog
```

### `match`, `exclude` and `options`

These
[regular expression settings](../Getting-Started/Configuration-Guide.md#standard-regular-expression-settings-for-filters)
limit which queries are logged.

```
match=select.*from.*customer.*where
exclude=^insert
options=case,extended
```

### `user` and `source`

These optional parameters limit logging on a session level. If `user` is
defined, only the sessions with a matching client username are logged. If
`source` is defined, only sessions with a matching client source address are
logged.

```
user=john
source=127.0.0.1
```

### `user_match`

- **Type**: [regex](../Getting-Started/Configuration-Guide.md#regular-expressions)
- **Mandatory**: No
- **Dynamic**: Yes

Only log queries from users that match this pattern. If the `user` parameter is
used, the value of `user_match` is ignored.

Here is an example pattern that matches the users `alice` and `bob`:

```
user_match=/(^alice$)|(^bob$)/
```

### `user_exclude`

- **Type**: [regex](../Getting-Started/Configuration-Guide.md#regular-expressions)
- **Mandatory**: No
- **Dynamic**: Yes

Exclude all queries from users that match this pattern. If the `user` parameter
is used, the value of `user_exclude` is ignored.

Here is an example pattern that excludes the users `alice` and `bob`:

```
user_exclude=/(^alice$)|(^bob$)/
```

### `source_match`

- **Type**: [regex](../Getting-Started/Configuration-Guide.md#regular-expressions)
- **Mandatory**: No
- **Dynamic**: Yes

Only log queries from hosts that match this pattern. If the `source` parameter
is used, the value of `source_match` is ignored.

Here is an example pattern that matches the loopback interface as well as the
address `192.168.0.109`:

```
source_match=/(^127[.]0[.]0[.]1)|(^192[.]168[.]0[.]109)/
```

### `source_exclude`

- **Type**: [regex](../Getting-Started/Configuration-Guide.md#regular-expressions)
- **Mandatory**: No
- **Dynamic**: Yes

Exclude all queries from hosts that match this pattern. If the `source`
parameter is used, the value of `source_exclude` is ignored.

Here is an example pattern that excludes the loopback interface as well as the
address `192.168.0.109`:

```
source_exclude=/(^127[.]0[.]0[.]1)|(^192[.]168[.]0[.]109)/
```

### `log_type`

The type of log file to use. The default value is _session_.

|Value   | Description                    |
|--------|--------------------------------|
|session |Write to session-specific files |
|unified |Use one file for all sessions   |
|stdout  |Same as unified, but to stdout  |

```
log_type=session
```

The log types can be combined, e.g. setting `log_type=session,stdout`
will write both session-specific files, and all sessions to stdout.

### `log_data`

Type of data to log in the log files. The parameter value is a comma separated
list of the following elements. By default the _date_, _user_ and _query_
options are enabled.

| Value             | Description                                            |
| --------          |--------------------------------------------------------|
| service           | Service name                                           |
| session           | Unique session id (ignored for session files)          |
| date              | Timestamp                                              |
| user              | User and hostname of client                            |
| reply_time        | Duration from client query to first server reply       |
| total_reply_time  | Duration from client query to last server reply (v6.2) |
| query             | The SQL of the query if it contains it                 |
| default_db        | The default (current) database                         |
| num_rows          | Number of rows in the result set (v6.2)                |
| reply_size        | Number of bytes received from the server (v6.2)        |
| transaction       | BEGIN, COMMIT and ROLLBACK (v6.2)                      |
| transaction_time  | The duration of a transaction (v6.2)                   |
| num_warnings      | Number of warnings in the server reply (v6.2)          |
| error_msg         | Error message from the server (if any) (v6.2)          |
| server            | The server where the query was routed (if any) (v22.08)|
| command           | The protocol command that was executed (v24.02)        |

```
log_data=date, user, query, total_reply_time
```

The durations *reply_time* and *total_reply_time* are by default in milliseconds,
but can be specified to be in microseconds using *duration_unit*.

The log entry is written when the last reply from the server is received.
Prior to version 6.2 the entry was written when the query was received from
the client, or if *reply_time* was specified, on first reply from the server.

**NOTE** The *error_msg* is the raw message from the server. Even if *use_canonical_form*
is set the error message may contain user defined constants. For example:

```
MariaDB [test]> select secret from T where x password="clear text pwd";
ERROR 1064 (42000): You have an error in your SQL syntax; check the manual
that corresponds to your MariaDB server version for the right syntax to
use near 'password="clear text pwd"' at line 1
```

Starting with MaxScale 24.02, the `query` parameter now correctly logs
the execution of binary protocol commands as SQL
([MXS-4959](https://jira.mariadb.org/browse/MXS-4959)). The execution of
batched statements (COM_STMT_BULK_LOAD) used by some connectors is not
logged.

### `duration_unit`

The unit for logging a duration. The unit can be `milliseconds` or `microseconds`.
The abbreviations `ms` for milliseconds and `us` for microseconds are also valid.
The default is `milliseconds`.
This option is available as of MaxScale version 6.2.

```
duration_unit=microseconds
```

### `use_canonical_form`

When this option is true the canonical form of the query is logged. In the
canonical form all user defined constants are replaced with question marks.
The default is false, i.e. log the sql as is.
This option is available as of MaxScale version 6.2.

```
use_canonical_form=true
```

### `flush`

Flush log files after every write. The default is false.

```
flush=true
```

### `append`

Append new entries to log files instead of overwriting them. The default is
true.
**NOTE**: the default was changed from false to true, as of the following
versions: 2.4.18, 2.5.16 and 6.2.

```
append=true
```

### `separator`

Default value is "," (a comma). Defines the separator string between elements of
a log entry. The value should be enclosed in quotes.

```
separator=" | "
```

### `newline_replacement`

Default value is " " (one space). SQL-queries may include line breaks, which, if
printed directly to the log, may break automatic parsing. This parameter defines
what should be written in the place of a newline sequence (\r, \n or \r\n). If
this is set as the empty string, then newlines are not replaced and printed as
is to the output. The value should be enclosed in quotes.

```
newline_replacement=" NL "
```

## Limitations

- Trailing parts of SQL queries that are larger than 16MiB are not
  logged. This means that the log output might contain truncated SQL.

- Batched execution using COM_STMT_BULK_EXECUTE is not converted into
  their textual form. This is done due to the large volumes of data that
  are usually involved with batched execution.

## Examples

### Example 1 - Query without primary key

Imagine you have observed an issue with a particular table and you want to
determine if there are queries that are accessing that table but not using the
primary key of the table. Let's assume the table name is PRODUCTS and the
primary key is called PRODUCT_ID. Add a filter with the following definition:

```
[ProductsSelectLogger]
type=filter
module=qlafilter
match=SELECT.*from.*PRODUCTS .*
exclude=WHERE.*PRODUCT_ID.*
filebase=/var/logs/qla/SelectProducts

[Product-Service]
type=service
router=readconnroute
servers=server1
user=myuser
password=mypasswd
filters=ProductsSelectLogger
```

The result of using this filter with the service used by the application would
be a log file of all select queries querying PRODUCTS without using the
PRODUCT_ID primary key in the predicates of the query. Executing `SELECT * FROM
PRODUCTS` would log the following into `/var/logs/qla/SelectProducts`:
```
07:12:56.324 7/01/2016, SELECT * FROM PRODUCTS
```
