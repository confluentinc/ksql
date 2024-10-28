---
layout: page
title: Get the status of a CREATE, DROP, or TERMINATE
tagline: status endpoint
description: The `/status` resource returns the current state of statement execution
keywords: ksqlDB, status, create, drop, terminate
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-rest-api/status-endpoint.html';
</script>

CREATE, DROP, and TERMINATE statements return an object that indicates
the current state of statement execution. A statement can be in one of
the following states:

-   QUEUED, PARSING, EXECUTING: The statement was accepted by the server
    and is being processed.
-   SUCCESS: The statement was successfully processed.
-   ERROR: There was an error processing the statement. The statement
    was not executed.
-   TERMINATED: The query started by the statement was terminated. Only
    returned for `CREATE STREAM|TABLE AS SELECT`.

If a CREATE, DROP, or TERMINATE statement returns a command status with
state QUEUED, PARSING, or EXECUTING from the `/ksql` endpoint, you can
use the `/status` endpoint to poll the status of the command.


GET /status/(string:commandId)

:  Get the current command status for a CREATE, DROP, or TERMINATE statement.

Parameters:

- **commandId** (string): The command ID of the statement. This ID is returned by the /ksql endpoint.

Response JSON Object:

- **status** (string): One of QUEUED, PARSING, EXECUTING, TERMINATED, SUCCESS, or ERROR.
- **message** (string): Detailed message regarding the status of the execution statement.

**Example request**

```http
GET /status/stream/PAGEVIEWS/create HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json
```

**Example response**

```http
HTTP/1.1 200 OK
Content-Type application/vnd.ksql.v1+json

{
  "status": "SUCCESS",
  "message":"Stream created and running"
}
```

