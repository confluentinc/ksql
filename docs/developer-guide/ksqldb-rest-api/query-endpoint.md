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

- **row** (object): A single row being returned. This will be null if an error is being returned.
- **row.columns** (array): The values contained in the row.
- **row.columns[i]** (?): The value contained in a single column for the row. The value type depends on the type of the column.
- **finalMessage** (string): If this field is non-null, it contains a final message from the server. No additional rows will be returned and the server will end the response.
- **errorMessage** (string): If this field is non-null, an error has been encountered while running the statement. No additional rows are returned and the server will end the response.

## Examples 

### Example curl command

```bash
curl -X "POST" "http://<ksqldb-host-name>:8088/query" \
     -d $'{
  "ksql": "SELECT * FROM USERS;",
  "streamsProperties": {}
}'

```

### Example request

```http
POST /query HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json

{
  "sql": "SELECT * FROM pageviews;",
  "streamsProperties": {
    "ksql.streams.auto.offset.reset": "earliest"
  }
}
```

### Example response

```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ksql.v1+json
Transfer-Encoding: chunked

...
{"row":{"columns":[1524760769983,"1",1524760769747,"alice","home"]},"errorMessage":null}
...
```

