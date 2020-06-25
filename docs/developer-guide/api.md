---
layout: page
title: ksqlDB HTTP API Reference
tagline: Run queries over HTTP
description: Learn how to communicate with ksqlDB by using HTTP
---

- [Get the Status of a ksqlDB Server (/info endpoint)](ksqldb-rest-api/info-endpoint.md)
- [Run a ksqlDB Statement (/ksql endpoint)](ksqldb-rest-api/ksql-endpoint.md)
- [Execute a push or pull query and stream the results back (/query-stream endpoint)](ksqldb-rest-api/streaming-endpoint.md)
- [Stream inserts to the server (/inserts-stream endpoint)](ksqldb-rest-api/streaming-endpoint.md)
- [Run A Pull or Push Query And Stream Back The Chunked Output (/query endpoint)](ksqldb-rest-api/query-endpoint.md)
- [Get the Status of a CREATE, DROP, or TERMINATE statement (/status endpoint)](ksqldb-rest-api/status-endpoint.md)
- [Terminate a Cluster (/ksql/terminate endpoint)](ksqldb-rest-api/terminate-endpoint.md)

REST Endpoint
-------------

The default HTTP API endpoint is `http://0.0.0.0:8088/`.

Change the server configuration that controls the HTTP API endpoint by
setting the `listeners` parameter in the ksqlDB server config file. For
more info, see [listeners](../operate-and-deploy/installation/server-config/config-reference.md#listeners).
To configure the endpoint to use HTTPS, see
[Configure ksqlDB for HTTPS](../operate-and-deploy/installation/server-config/security.md#configure-ksqldb-for-https).

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
use the versioned value if possible.

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
     -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
     -d $'{
  "ksql": "LIST STREAMS;",
  "streamsProperties": {}
}'
```

Here's an example request that retrieves streaming data from
`TEST_STREAM`:

```bash
curl -X "POST" "http://localhost:8088/query" \
     -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
     -d $'{
  "ksql": "SELECT * FROM TEST_STREAM EMIT CHANGES;",
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
