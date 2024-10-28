---
layout: page
title: Run a ksqlDB Statement
tagline: ksql endpoint
description: Use the `/ksql` resource to run a sequence of ksqlDB statements
keywords: ksqldb
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-rest-api/ksql-endpoint.html';
</script>

The `/ksql` resource runs a sequence of SQL statements. All statements,
except those starting with SELECT and PRINT, can be run on this endpoint. To run
SELECT and PRINT statements use the `/query` endpoint.

!!! note
      If you use the SET or UNSET statements to assign query properties by
      using the REST API, the assignment is scoped only to the current
      request. In contrast, SET and UNSET assignments in the ksqlDB CLI persist
      throughout the CLI session.

## POST /ksql

:   Run a sequence of SQL statements.

## JSON Parameters

- **ksql** (string): A semicolon-delimited sequence of SQL statements to run.
- **streamsProperties** (map): Property overrides to run the statements with.
  Refer to the
  [Configuration Parameter Reference](../../../operate-and-deploy/installation/server-config/config-reference)
  for details on properties that can be set.
- **streamsProperties[``property-name``]** (string): The value of the property
- named by ``property-name``. Both the value and ``property-name`` should be
  strings.
- **sessionVariables** (map): Optional. Starting from 0.18, the parameter `sessionVariables` accepts
  a map of string variable names and values of any type as initial variable substitution values. See
  [ksqlDB Variable Substitution](../../../how-to-guides/substitute-variables) for more information
  on variable substitution.
- **commandSequenceNumber** (long): Optional. If specified, the statements will
  not be run until all existing commands up to and including the specified
  sequence number have completed. If unspecified, the statements are run
  immediately. When a command is processed, the result object contains its
  sequence number.

The response JSON is an array of result objects. The result object contents
depend on the statement that it is returning results for. The following
sections detail the contents of the result objects by statement.

## Common Fields

The following fields are common to all responses.

Response JSON Object:

- **statementText** (string): The SQL statement whose result is being returned.
- **warnings** (array): A list of warnings about conditions that may be unexpected
  by the user, but don't result in failure to execute the statement.
- **warnings[i].message** (string): A message detailing the condition being warned on.

**CREATE, DROP, TERMINATE**

Response JSON Object:

- **commandId** (string): A string that identifies the requested operation. You can
  use this ID to poll the result of the operation using the status endpoint.
- **commandStatus.status** (string): One of QUEUED, PARSING, EXECUTING, TERMINATED,
  SUCCESS, or ERROR.
- **commandStatus.message** (string): Detailed message regarding the status of the
  execution statement.
- **commandSequenceNumber** (long): The sequence number of the requested operation
  in the command queue, or -1 if the operation was unsuccessful.

**LIST STREAMS, SHOW STREAMS**

Response JSON Object:

- **streams** (array): List of streams.
- **streams[i].name** (string): The name of the stream.
- **streams[i].topic** (string): The topic backing the stream.
- **streams[i].format** (string): The serialization format of the data in the
  stream. One of JSON, AVRO, PROTOBUF, or DELIMITED.
- **streams[i].type** (string): The source type. Always returns `STREAM`.

**LIST TABLES, SHOW TABLES**

Response JSON Object:

- **tables** (array): List of tables.
- **tables[i].name** (string): The name of the table.
- **tables[i].topic** (string): The topic backing the table.
- **tables[i].format** (string): The serialization format of the data in the
  table. One of JSON, AVRO, PROTOBUF, or DELIMITED.
- **tables[i].type** (string): The source type. Always returns `TABLE`.
- **tables[i].isWindowed** (boolean): `true` if the table provides windowed results; otherwise, `false`.

**LIST QUERIES, SHOW QUERIES**

Response JSON Object:

- **queries** (array): List of queries.
- **queries[i].queryString** (string): The text of the statement that started the query.
- **queries[i].sinks** (string): The streams and tables being written to by the query.
- **queries[i].id** (string): The query ID.

**LIST PROPERTIES, SHOW PROPERTIES**

Response JSON Object:

- **properties** (map): The ksqlDB server query properties.
- **properties[``property-name``]** (string): The value of the property named by
  ``property-name``.

**DESCRIBE**

Response JSON Object:

- **sourceDescription.name** (string): The name of the stream or table.
- **sourceDescription.readQueries** (array): The queries reading from the stream
  or table.
- **sourceDescription.writeQueries** (array): The queries writing into the stream
  or table
- **sourceDescription.fields** (array): A list of field objects that describes each
  field in the stream/table.
- **sourceDescription.fields[i].name** (string): The name of the field.
- **sourceDescription.fields[i].schema** (object): A schema object that describes
  the schema of the field.
- **sourceDescription.fields[i].schema.type** (string): The type the schema
  represents. One of INTEGER, BIGINT, BOOLEAN, BYTES, DOUBLE, STRING, TIMESTAMP, TIME, DATE, MAP, ARRAY, or
  STRUCT.
- **sourceDescription.fields[i].schema.memberSchema** (object): A schema object.
  For MAP and ARRAY types, contains the schema of the map values and array
  elements, respectively. For other types this field is not used and its value
  is undefined.
- **sourceDescription.fields[i].schema.fields** (array): For STRUCT types, contains
  a list of field objects that describes each field within the struct. For other
  types this field is not used and its value is undefined.
- **sourceDescription.type** (string): STREAM or TABLE
- **sourceDescription.key** (string): The name of the key column.
- **sourceDescription.timestamp** (string): The name of the timestamp column.
- **sourceDescription.format** (string): The serialization format of the data in
  the stream or table. One of JSON, AVRO, PROTOBUF, or DELIMITED.
- **sourceDescription.topic** (string): The topic backing the stream or table.
- **sourceDescription.extended** (boolean): A boolean that indicates whether this
  is an extended description.
- **sourceDescription.statistics** (string): A string that contains statistics
  about production and consumption to and from the backing topic (extended only).
- **sourceDescription.errorStats** (string): A string that contains statistics about
  errors producing and consuming to and from the backing topic (extended only).
- **sourceDescription.replication** (int): The replication factor of the backing
  topic (extended only).
- **sourceDescription.partitions** (int): The number of partitions in the backing
  topic (extended only).

**EXPLAIN**

Response JSON Object:

- **queryDescription.statementText** (string): The ksqlDB statement for which the query being explained is running.
- **queryDescription.fields** (array): A list of field objects that describes each field in the query output.
- **queryDescription.fields[i].name** (string): The name of the field.
- **queryDescription.fields[i].schema** (object): A schema object that describes the schema of the field.
- **queryDescription.fields[i].schema.type** (string): The type the schema represents. One of INTEGER, BIGINT, BOOLEAN, BYTES, DOUBLE, STRING, TIMESTAMP, TIME, DATE, MAP, ARRAY, or STRUCT.
- **queryDescription.fields[i].schema.memberSchema** (object): A schema object. For MAP and ARRAY types, contains the schema of the map values and array elements, respectively. For other types this field is not used and its value is undefined.
- **queryDescription.fields[i].schema.fields** (array): For STRUCT types, contains a list of field objects that describes each field within the struct. For other types this field is not used and its value is undefined.
- **queryDescription.sources** (array): The streams and tables being read by the query.
- **queryDescription.sources[i]** (string): The name of a stream or table being read from by the query.
- **queryDescription.sinks** (array): The streams and tables being written to by the query.
- **queryDescription.sinks[i]** (string): The name of a stream or table being written to by the query.
- **queryDescription.executionPlan** (string): The query execution plan.
- **queryDescription.topology** (string): The Kafka Streams topology that the query is running.
- **overriddenProperties** (map): The property overrides that the query is running with.

## Errors

If ksqlDB fails to execute a statement, it returns a response with an error
status code (4xx/5xx). Even if an error is returned, the server may have been
able to successfully execute some statements in the request. In this case, the
response includes the ``error_code`` and ``message`` fields, a ``statementText``
field with the text of the failed statement, and an ``entities`` field that
contains an array of result objects:

Response JSON Object:

- **statementText** (string): The text of the SQL statement where the error occurred.
- **entities** (array): Result objects for statements that were successfully executed by the server.

The ``/ksql`` endpoint may return the following error codes in the ``error_code`` field:

- 40001 (BAD_STATEMENT): The request contained an invalid SQL statement.
- 40002 (QUERY_ENDPOINT): The request contained a statement that should be issued to the ``/query`` endpoint.

## Examples

### Example curl command

```bash
curl --http1.1 \
     -X "POST" "http://<ksqldb-host-name>:8088/ksql" \
     -H "Accept: application/vnd.ksql.v1+json" \
     -H "Content-Type: application/json" \
     -d $'{
  "ksql": "LIST STREAMS;",
  "streamsProperties": {}
}'
```

### Example request

```http
POST /ksql HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json

{
  "ksql": "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home'; CREATE STREAM pageviews_alice AS SELECT * FROM pageviews_original WHERE userid='alice';",
  "streamsProperties": {
    "ksql.streams.auto.offset.reset": "earliest"
  }
}
```

### Example response

```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ksql.v1+json

[
  {
    "statementText":"CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home';",
    "commandId":"stream/PAGEVIEWS_HOME/create",
    "commandStatus": {
      "status":"SUCCESS",
      "message":"Stream created and running"
    },
    "commandSequenceNumber":10
  },
  {
    "statementText":"CREATE STREAM pageviews_alice AS SELECT * FROM pageviews_original WHERE userid='alice';",
    "commandId":"stream/PAGEVIEWS_ALICE/create",
    "commandStatus": {
      "status":"SUCCESS",
      "message":"Stream created and running"
    },
    "commandSequenceNumber":11
  }
]
```

## Coordinate Multiple Requests

To submit multiple, interdependent requests, there are two options. The
first is to submit them as a single request, similar to the example
request above:

```http
POST /ksql HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json

{
  "ksql": "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home'; CREATE TABLE pageviews_home_count AS SELECT userid, COUNT(*) FROM pageviews_home GROUP BY userid EMIT CHANGES;"
}
```

The second method is to submit the statements as separate requests and
incorporate the interdependency by using `commandSequenceNumber`. Send
the first request:

```http
POST /ksql HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json

{
  "ksql": "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home' EMIT CHANGES;"
}
```

Make note of the `commandSequenceNumber` returned in the response:

```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ksql.v1+json

[
  {
    "statementText":"CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home' EMIT CHANGES;",
    "commandId":"stream/PAGEVIEWS_HOME/create",
    "commandStatus": {
      "status":"SUCCESS",
      "message":"Stream created and running"
    },
    "commandSequenceNumber":10
  }
]
```

Provide this `commandSequenceNumber` as part of the second request,
indicating that this request should not execute until after command
number 10 has finished executing:

```http
POST /ksql HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json

{
  "ksql": "CREATE TABLE pageviews_home_count AS SELECT userid, COUNT(*) FROM pageviews_home GROUP BY userid EMIT CHANGES;",
  "commandSequenceNumber":10
}
```

