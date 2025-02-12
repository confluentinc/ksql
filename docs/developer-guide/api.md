---
layout: page
title: ksqlDB HTTP API Reference
tagline: Run queries over HTTP
description: Learn how to communicate with ksqlDB by using HTTP
---

- [Execute a statement (/ksql endpoint)](ksqldb-rest-api/ksql-endpoint.md)
- [Run a query (/query endpoint)](ksqldb-rest-api/query-endpoint.md)
- [Run push and pull queries (/query-stream endpoint)](ksqldb-rest-api/streaming-endpoint.md)
- [Terminate a cluster (/ksql/terminate endpoint)](ksqldb-rest-api/terminate-endpoint.md)
- [Introspect query status (/status endpoint)](ksqldb-rest-api/status-endpoint.md)
- [Introspect server status (/info endpoint)](ksqldb-rest-api/info-endpoint.md)
- [Introspect cluster status (/clusterStatus endpoint)](ksqldb-rest-api/cluster-status-endpoint.md)
- [ Get the validity of a property (/is_valid_property)](ksqldb-rest-api/is_valid_property-endpoint.md)

REST Endpoint
-------------

The default HTTP API endpoint is `http://0.0.0.0:8088/`.

Change the server configuration that controls the HTTP API endpoint by
setting the `listeners` parameter in the ksqlDB server config file. For
more info, see [listeners](../reference/server-configuration.md#listeners).
To configure the endpoint to use HTTPS, see
[Configuring Listener for SSL encryption](../operate-and-deploy/installation/server-config/security.md#configuring-listener-for-ssl-encryption).

Content Types
-------------

The ksqlDB HTTP API uses content types for requests and responses to
indicate the serialization format of the data and the API version.

Your request should specify this serialization
format and version in the `Accept` header, for example:

```
Accept: application/vnd.ksql.v1+json
```

The less specific `application/json` content type is also permitted.
However, this is only for compatibility and ease of use, and you should
use the versioned value where possible. `application/json` maps to the latest
versioned content type, meaning the response may change after upgrading the server to 
a later version.

The server also supports content negotiation, so you may include
multiple, weighted preferences:

```
Accept: application/vnd.ksql.v1+json; q=0.9, application/json; q=0.5
```

For example, content negotiation is useful when a new version of the API
is preferred, but you are not sure if it is available yet.

Here's an example request that returns the results from the
`LIST STREAMS` command:

```bash
curl -X "POST" "http://localhost:8088/ksql" \
     -H "Accept: application/vnd.ksql.v1+json" \
     -d $'{
  "ksql": "LIST STREAMS;",
  "streamsProperties": {}
}'
```

Here's an example request that retrieves streaming data from
`TEST_STREAM`:

```bash
curl -X "POST" "http://localhost:8088/query" \
     -H "Accept: application/vnd.ksql.v1+json" \
     -d $'{
  "ksql": "SELECT * FROM TEST_STREAM EMIT CHANGES;",
  "streamsProperties": {}
}'
```

A `PROTOBUF` content type where the rows are serialized in the `PROTOBUF` format
is also supported for querying the `/query` and `/query-stream` endpoints.
You can specify this serialization format in the `Accept` header:
```
Accept: application/vnd.ksql.v1+protobuf
```
The following example shows a curl command that issues a Pull query on a table called `CURRENTLOCATION`
with the `PROTOBUF` content type:
```bash
curl -X "POST" "http://localhost:8088/query" \
     -H "Accept: application/vnd.ksql.v1+protobuf" \
     -d $'{
  "ksql": "SELECT * FROM CURRENTLOCATION;",
  "streamsProperties": {}
}'
```
Response:
```json
[{"header":{"queryId":"query_1655152127973","schema":"`PROFILEID` STRING KEY, `LA` DOUBLE, `LO` DOUBLE","protoSchema":"syntax = \"proto3\";\n\nmessage ConnectDefault1 {\n  string PROFILEID = 1;\n  double LA = 2;\n  double LO = 3;\n}\n"}},
{"row":{"protobufBytes":"CggxOGY0ZWE4NhF90LNZ9bFCQBmASL99HYRewA=="}},
{"row":{"protobufBytes":"Cgg0YTdjN2I0MRFAE2HD07NCQBnM7snDQoVewA=="}},
{"row":{"protobufBytes":"Cgg0YWI1Y2JhZBGKsOHplbJCQBmMSuoENIVewA=="}},
{"row":{"protobufBytes":"Cgg0ZGRhZDAwMBHNO07RkeRCQBk9m1Wfq5lewA=="}},
{"row":{"protobufBytes":"Cgg4YjZlYWU1ORFtxf6ye7JCQBmMSuoENIVewA=="}},
{"row":{"protobufBytes":"CghjMjMwOWVlYxGUh4Va0+RCQBn0/dR46ZpewA=="}}]
```
The `protoSchema` field in the `header` corresponds to the content of a `.proto` file that the proto compiler uses at build time. 
Use the `protoSchema` field to deserialize the `protobufBytes` into `PROTOBUF` messages.

Provide the `--basic` and `--user` options if basic HTTPS authentication is
enabled on the cluster, as shown in the following command.

```bash hl_lines="3"
curl -X "POST" "https://localhost:8088/ksql" \
     -H "Accept: application/vnd.ksql.v1+json" \
     --basic --user "<API key>:<secret>" \
     -d $'{
  "ksql": "LIST STREAMS;",
  "streamsProperties": {}
}'
```

Errors
------

All API endpoints use a standard error message format for any requests
that return an HTTP status indicating an error (any 4xx or 5xx
statuses):

```http
HTTP/1.1 <Error Status>
Content-Type: application/json

{
    "error_code": <Error code>
    "message": <Error Message>
}
```

Some endpoints may include additional fields that provide more context
for handling the error.
