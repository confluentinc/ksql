---
layout: page
title: HTTP Streaming API
tagline: streaming endpoints
description: The HTTP Streaming API lets you execute pull or push queries and stream inserts to the server
keywords: ksqlDB, query, insert, select
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-rest-api/streaming-endpoint.html';
</script>

!!! note

    - These endpoints are used by the ksqlDB Java client. If you are using Java you might want
      to use the Java client rather than using this API directly.
    - These endpoints are only available when using HTTP 2.

### Executing pull or push queries

The request method is a POST.

Send requests to the `/query-stream` endpoint.

The body of the request is a JSON object UTF-8 encoded as text, containing the arguments for the
operation. Newlines have been added here for the sake of clarity, but the actual JSON must not contain
 unescaped newlines.

```
{
"sql": "select * from foo", <----- the SQL of the query to execute
"properties": {             <----- Optional properties for the query
    "prop1": "val1",
    "prop2": "val2"
   }
}

```

The endpoint produces responses with two possible content types: `application/json` and
`application/vnd.ksqlapi.delimited.v1`. To specify the content type, set the `Accept`
header in the request. The default is `application/vnd.ksqlapi.delimited.v1`.

In the case of a successful query, if the content type is `application/vnd.ksqlapi.delimited.v1`,
the results are returned as a header JSON object followed by zero or more JSON arrays
that are delimited by newlines. Newline-delimited formats are easy to parse by clients and don't require
a streaming JSON parser on the client in the case that intermediate results need to be output.

```
{
"queryId", "xyz123",                          <---- unique ID, provided for push queries only
"columnNames":["col", "col2", "col3"],        <---- the names of the columns
"columnTypes":["BIGINT", "STRING", "BOOLEAN"] <---- The types of the columns
}
```

Followed by zero or more JSON arrays:

```
[123, "blah", true]
[432, "foo", true]
[765, "whatever", false]
```

If you prefer to receive the entire response as valid JSON, request the
content type `application/json`. In this case you receive the results as a single JSON
array, as shown in the following example. Newlines have been added for clarity and the response body
won't contain newlines.

```
[
{
"queryId": "xyz123",                          <---- unique ID, provided for push queries only
"columnNames":["col", "col2", "col3"],        <---- the names of the columns
"columnTypes":["BIGINT", "STRING", "BOOLEAN"] <---- The types of the columns
},
[123, "blah", true],
[432, "foo", true],
[765, "whatever", false]
]
```

Starting from 0.18, variable substitution can be applied by passing a map of variables and
definitions to the `sessionVariables` argument of the request body and referencing variables by
enclosing them in `${}`. For example, the following requests are equivalent:

```json
{
"sql": "SELECT profileId AS ${name} FROM riderLocations EMIT CHANGES;", 
"sessionVariables": {
    "name": "user"
   }
}
```

```json
{
"sql": "SELECT profileId AS user FROM riderLocations EMIT CHANGES;"
}
```

### Terminating queries

You can terminate push queries explicitly in the client by making a request to this endpoint.

The request method is POST.

Send requests to the `/close-query` endpoint.

The body of the request is a JSON object UTF-8 encoded as text, containing the id of the 
query to close. Newlines have been added here for the sake of clarity but the actual JSON must not
contain newlines.

```
{
"queryId": "xyz123" <----- the ID of the query to terminate
}

```

### Running print statements

The `/query-stream` endpoint also
supports [print statements](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/print/).

Similar to pull and push queries, the request method is a POST and the request body is a JSON object
UTF-8 encoded as text, containing the arguments for the
operation. Newlines have been added here for the sake of clarity, but the actual JSON must not
contain
unescaped newlines.

```
{
"sql": "print `my-topic-123` limit 3;", <----- the SQL of the query to execute
}

```

The endpoint **only supports `application/vnd.ksqlapi.delimited.v1` as a content type for the print statement**. If
you specify another content type in the `Accept` header, you'll receive **406 Not
Acceptable**.

In the case of a successful query, you should see records from the requested topic delimited by newlines.

```
rowtime: 2022/12/02 17:22:02.556 Z, key: 18, value: {\"ordertime\":1497014222380,\"orderid\":18,\"itemid\":\"Item_184\",\"address\":{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041}}, partition: 0
rowtime: 2022/12/02 17:22:04.556 Z, key: 19, value: {\"ordertime\":1497014224380,\"orderid\":19,\"itemid\":\"Item_185\",\"address\":{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041}}, partition: 0
rowtime: 2022/12/02 17:22:08.556 Z, key: 20, value: {\"ordertime\":1497014228380,\"orderid\":20,\"itemid\":\"Item_185\",\"address\":{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041}}, partition: 0
```

### Inserting rows into an existing stream

This endpoint allows you to insert rows into an existing ksqlDB stream. The stream must have
already been created in ksqlDB.

The request method is a POST.

Send requests to the `/inserts-stream` endpoint.

The body of the request is a JSON object UTF-8 encoded as text, containing the arguments for the
operation. Newlines have been added for clarity, but the actual JSON must not contain newlines.

```
{
"target": "my-stream" <----- The name of the KSQL stream to insert into
}

```

The stream name is case insensitive. 

Followed by zero or more JSON objects representing the values to insert:

```
{
"col1" : "val1",
"col2": 2.3,
"col3", true
}
```
Each JSON object is separated by a newline.

To terminate the insert stream the client must end the request.

An acks is written to the response when each row has been
committed successfully to the underlying topic. Rows are committed in the order they are provided.
Each ack in the response is a JSON object, separated by newlines:

```
{"status":"ok","seq":0}
{"status":"ok","seq":2}
{"status":"ok","seq":1}
{"status":"ok","seq":3}
```

A successful ack contains a `status` field with value `ok`.

All ack responses also contain a `seq` field with a 64-bit signed integer value. This number
corresponds to the sequence of the insert on the request. The first send has sequence `0`, the second
`1`, the third `2`, etc. It allows the client to correlate the ack to the corresponding send.

In case of error, an error response (see below) is sent. For an error response for a send, the
`seq` field is included. 

!!!note
    
    Acks can be returned in a different sequence compared with the order in
    which inserts were submitted. 

## Example curl command

```bash
curl -X "POST" "http://<ksqldb-host-name>:8088/query-stream" \
     -d $'{
  "sql": "SELECT * FROM PAGEVIEWS EMIT CHANGES;",
  "streamsProperties": {}
}'
```
