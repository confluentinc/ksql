---
layout: page
title: Run a query and stream back the output
tagline: query endpoint
description: The `/query` resource lets you stream the output records of a `SELECT` statement
keywords: ksqlDB, query, select
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-rest-api/query-endpoint.html';
</script>

The `/query` resource lets you stream the output records of a `SELECT`
statement via a chunked transfer encoding. The response is streamed back
until the `LIMIT` specified in the statement is reached, or the client
closes the connection. If no `LIMIT` is specified in the statement, then
the response is streamed until the client closes the connection.

Append `EMIT CHANGES` to specify a push query with continuous output.

!!! note
      This endpoint was proposed to be deprecated as part of 
      [KLIP-15](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-15-new-api-and-client.md)
      in favor of the new `HTTP/2` [`/query-stream`](/developer-guide/ksqldb-rest-api/streaming-endpoint).
      The deprecation itself is not yet scheduled, but if you are able to use `HTTP/2`,
      you are recommended to favor `/query-stream`.

## POST /query

:   Run a ``SELECT`` statement and stream back the results.

## JSON Parameters:

- **ksql** (string): The SELECT statement to run.
- **streamsProperties** (map): Property overrides to run the statements with.
  Refer to the [Config Reference](/reference/server-configuration)
  for details on properties that you can set.
- **streamsProperties**[``property-name``] (string): The value of the property named by ``property-name``. Both the value and ``property-name`` should be strings.

Each response chunk is a JSON object with the following format:

Response JSON Object:

- **header** (object): Information about the result.
    - **header.queryId**: (string): the unique id of the query. This can be useful when debugging. 
    For example, when looking in the logs or the ksql processing log for errors or issues.
    - **header.schema**: (string): the list of columns being returned. This defines the schema for 
    the data returned later in **row.columns**.  
- **row** (object): A single row being returned. This will be null if an error is being returned.
    - **row.columns** (array): The values of the columns requested. The schema of the columns was
    already supplied in **header.schema**.
    - **row.tombstone** (boolean): Whether the row is a deletion of a previous row.
    It is recommended that you include all columns within the primary key in the projection
    so that you can determine _which_ previous row was deleted.
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
curl --http1.1 \
     -X "POST" "http://<ksqldb-host-name>:8088/query" \
     -H "Accept: application/vnd.ksql.v1+json" \
     -d $'{
  "ksql": "SELECT * FROM USERS EMIT CHANGES;",
  "streamsProperties": {}
}'

```

### Example request

```http
POST /query HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json

{
  "ksql": "SELECT * FROM pageviews EMIT CHANGES;",
  "streamsProperties": {
    "ksql.streams.auto.offset.reset": "earliest"
  }
}
```

### Example stream response

If the query result is a stream, the response will not include the **row.tombstone** field, as
streams don't have primary keys or the concept of a deletion.


```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ksql.v1+json
Transfer-Encoding: chunked

...
{"header":{"queryId":"_confluent_id_19",schema":"`ROWTIME` BIGINT, `NAME` STRING, `AGE` INT"}}
{"row":{"columns":[1524760769983,"1",1524760769747,"alice","home"]}}
...
```

### Example table response

If the query result is a table, the response may include deleted rows, as identified by the
**row.tombstone** field.

Rows within the table are identified by their primary key. Rows may be inserted, updated or deleted.
Rows without the **tombstone** field set indicate an upserts, (inserts or updates).
Rows with the  **tombstone** field set indicate a delete.

While it is not a requirement to include the primary key columns within the query's projection, any
use-case that is attempting to materialize the table, or has a requirement to be able to 
correlate later updates and deletes to previous rows, will generally need all primary key columns
in the projection.

In the example response below, the `ID` column is the primary key of the table. The second row in 
the response deletes the first.

```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ksql.v1+json
Transfer-Encoding: chunked

...
{"header":{"queryId":"_confluent_id_34",schema":"`ROWTIME` BIGINT, `NAME` STRING, `ID` INT"}}
{"row":{"columns":[1524760769983,"alice",10]}},
{"row":{"columns":[null,null,10],"tombstone":true}}
...
```
