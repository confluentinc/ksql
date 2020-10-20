---
layout: page
title: Run a query and stream back the output
tagline: query endpoint
description: The `/query` resource lets you stream the output records of a `SELECT` statement
keywords: ksqlDB, query, select
---

The `/query` resource lets you stream the output records of a `SELECT`
statement via a chunked transfer encoding. The response is streamed back
until the `LIMIT` specified in the statement is reached, or the client
closes the connection. If no `LIMIT` is specified in the statement, then
the response is streamed until the client closes the connection.

## POST /query

:   Run a ``SELECT`` statement and stream back the results.

## JSON Parameters:

- **ksql** (string): The SELECT statement to run.
- **streamsProperties** (map): Property overrides to run the statements with.
  Refer to the [Config Reference](../../operate-and-deploy/installation/server-config/config-reference.md)
  for details on properties that you can set.
- **streamsProperties**[``property-name``] (string): The value of the property named by ``property-name``. Both the value and ``property-name`` should be strings.

Each response chunk is a JSON object with the following format:

Response JSON Object:

- **header** (object): Information about the result.
    - **header.queryId**: (string): the unique id of the query. This can be useful when debugging. 
    For example, when looking in the logs or processing log for errors or issues.
    - **header.key**: (string)(since v2): the list of key columns, if the result is a table. 
    This defines the schema for the data returned later in **row.key**. 
    - **header.schema**: (string): the list of columns being returned. This defines the schema for 
    the data returned later in **row.columns**.  
- **row** (object): A single row being returned. This will be null if an error is being returned.
    - **row.key** (array): If the data being returned is a table, the primary key of the row.
    The key may be one or more values than uniquely identify the row. The schema of the key was 
    already supplied in **header.key**. 
    Updates with the same key _replace_ previous values for the row.
    - **row.columns** (array): The values of the columns requested. The schema of the columns was
    already supplied in **header.schema**.
    - **row.tombstone** (boolean)(since v2): the row is a deletion of a previously row.
    The **row.key** field contains the unique key of the row that has been deleted.
    Prior to v2 of the API, tombstones were not returned as part of the response.
- **finalMessage** (string): If this field is non-null, it contains a final message from the server.
    This signifies successful completion of the query.  
    No additional rows will be returned and the server will end the response.
- **errorMessage** (string): If this field is non-null, an error has been encountered while running 
    the statement. 
    This signifies unsuccessful completion of the query.
    No additional rows are returned and the server will end the response.

## Examples 

### Example curl command

```bash
curl -X "POST" "http://<ksqldb-host-name>:8088/query" \
     -H "Accept: application/vnd.ksql.v2+json" \
     -d $'{
  "ksql": "SELECT * FROM USERS;",
  "streamsProperties": {}
}'

```

### Example request

```http
POST /query HTTP/1.1
Accept: application/vnd.ksql.v2+json
Content-Type: application/vnd.ksql.v2+json

{
  "sql": "SELECT * FROM pageviews;",
  "streamsProperties": {
    "ksql.streams.auto.offset.reset": "earliest"
  }
}
```

### Example stream response

If the query result is a stream, the response doesn't include the **row.key** or

**row.tombstone** fields, because streams don't have primary keys.


```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ksql.v2+json
Transfer-Encoding: chunked

...
{"header":{"queryId":"_confluent_id_19",schema":"`ROWTIME` BIGINT, `NAME` STRING, `AGE` INT"}}
{"row":{"columns":[1524760769983,"1",1524760769747,"alice","home"]}}
...
```

### Example table response

If the query result is a table, the response includes the primary key of each row in

the **row.key** field. Rows that are deleted from the result table are identified by the **row.tombstone** 

field.

```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ksql.v2+json
Transfer-Encoding: chunked

...
{"header":{"queryId":"_confluent_id_34",key":"`ID BIGINT`",schema":"`ROWTIME` BIGINT, `NAME` STRING, `AGE` INT"}}
{"row":{"key":[10],"columns":[1524760769983,"alice",10]}},
{"row":{"key":[10],"tombstone":true}}
...
```

!!! note
    Media type `application/vnd.ksql.v1+json` does not populate **row.key** or return tombstone

rows.
