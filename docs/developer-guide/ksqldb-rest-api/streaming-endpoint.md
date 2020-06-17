---
layout: page
title: HTTP Streaming API
tagline: streaming endpoints
description: The HTTP Streaming API lets you execute pull or push queries and stream inserts to the
server
keywords: ksqlDB, query, insert, select
---

Please note: These endpoints are used by the ksqlDB Java client. If you are using Java you might want
to consider using the Java client rather than using this API directly.

These endpoints are only available when using HTTP 2.

### Executing pull or push queries

The request method will be a POST.

Requests will be made to the endpoint "/query-stream".

The body of the request is a JSON object UTF-8 encoded as text, containing the arguments for the
operation (newlines have been added here for the sake of clarity but the real JSON must not contain
 unescaped newlines)

````
{
"sql": "select * from foo", <----- the SQL of the query to execute
"properties": {             <----- Optional properties for the query
    "prop1": "val1",
    "prop2": "val2"
   }
}

````

The endpoint produces responses with two possible content types `application/json` and
`application/vnd.ksqlapi.delimited.v1`. To choose one or the other you should set the `Accept`
header in the request. The default is `application/vnd.ksqlapi.delimited.v1`.

In the case of a successful query, if the content type is `application/vnd.ksqlapi.delimited.v1`
then the results are returned as a header JSON object followed by zero or more JSON arrays
delimited by newlines. Newline delimited formats are easy to parse by clients, and don't require
a streaming JSON parser on the client in the case that intermediate results need to be output.

````
{
"queryId", "xyz123",                          <---- unique ID of the query, used when terminating the query
"columnNames":["col", "col2", "col3"],        <---- the names of the columns
"columnTypes":["BIGINT", "STRING", "BOOLEAN"] <---- The types of the columns
}
````

Followed by zero or more JSON arrays:

````
[123, "blah", true]
[432, "foo", true]
[765, "whatever", false]
````

If you prefer to receive the entire response as valid JSON then you can request
content type `application/json`. In which case you will receive the results, as a single JSON
array, for example: (newlines have been added for clarity, the response body
won't contain newlines).

````
[
{
"queryId", "xyz123",                          <---- unique ID of the query, used when terminating the query
"columnNames":["col", "col2", "col3"],        <---- the names of the columns
"columnTypes":["BIGINT", "STRING", "BOOLEAN"] <---- The types of the columns
},
[123, "blah", true]
[432, "foo", true]
[765, "whatever", false]
]
````

### Terminating queries

Push queries can be explicitly terminated by the client by making a request to this endpoint

The request method will be a POST.

Requests will be made to the endpoint "/close-query"

The body of the request is a JSON object UTF-8 encoded as text, containing the id of the 
query to close. (newlines have been added here for the sake of clarity but the real JSON must not
contain newlines)

````
{
"queryId": "xyz123", <----- the ID of the query to terminate
}

````
 
### Streaming inserts

The request method will be a POST.

Requests will be made to the endpoint "/insert-stream".

The body of the request is a JSON object UTF-8 encoded as text, containing the arguments for the
operation (newlines have been added for clarity, the real JSON must not contain newlines):

````
{
"target": "my-stream" <----- The name of the KSQL stream to insert into
}

````

Followed by zero or more JSON objects representing the values to insert:

````
{
"col1" : "val1",
"col2": 2.3,
"col3", true
}
````
Each JSON object will be separated by a new line.

To terminate the insert stream the client should end the request.

Acks will be written to the response when each row has been
successfully committed to the underlying topic. Rows are committed in the order they are provided.
Each ack in the response is a JSON object, separated by newlines:

````
{"status":"ok","seq":0}
{"status":"ok","seq":2}
{"status":"ok","seq":1}
{"status":"ok","seq":3}
````

A successful ack will contain a field `status` with value `ok`.

All ack responses also contain a field `seq` with a 64 bit signed integer value. This number
corresponds to the sequence of the insert on the request. The first send has sequence `0`, the second
`1`, the third `2`, etc. It allows the client to correlate the ack to the corresponding send.

In case of error, an error response (see below) will be sent. For an error response for a send, the
`seq` field will also be included. 

Please note that acks can be returned in a different sequence to which the inserts were submitted. 
