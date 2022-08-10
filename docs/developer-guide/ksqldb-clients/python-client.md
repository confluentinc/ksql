---
layout: page
title: Python Client for ksqlDB
tagline: Python client for ksqlDB
description: Send requests to ksqlDB from your Python app
keywords: ksqlDB, python, client
---

The Python client enables sending requests easily to a ksqlDB server
from within your Python application, as an alternative to using the [REST API](../api.md).
The client only supports pull and push queries.

!!! tip
    [View the Python client API documentation](python-client-api/index.html)

The client sends queries to the [`/query-stream` endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#executing-pull-or-push-queries).
The client is compatible only with ksqlDB deployments that are on version 0.xx.0 or later.

Use the Python client to:

- [Receive query results one row at a time asynchronously (stream_query_async())](#stream-query-async)
- [Receive query results in a single batch asynchronously (execute_query_async())](#execute-query-async)

Getting Started
---------------
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install `confluent-ksqldb-python`.

```bash
pip install confluent-ksqldb-python
```

Receive query results one row at a time (stream_query_async())<a name="stream-query-async"></a>
-----------------------------------------------------------------------------------------------
The `stream_query_async()` method enables client apps to receive query results one row at a time asynchronously.

You can use this method to issue both push and pull queries, but the usage pattern is better for push queries.
For pull queries, consider using the [`execute_query_async()`](#execute-query-async)
method instead.

### Example Usage ###
```python
import asyncio

from ksqldb_confluent.client import Client
from ksqldb_confluent.row import Row
from ksqldb_confluent.streamed_query_result import StreamedQueryResult


async def main():
    #Initialize the client with the host and port
    client: Client = Client(host='localhost', port=8088)
    
    """
    Execute a Push query.
     
    client.stream_query_async() is appropriate for queries which may not run 
    to immediate completion, so this works well for both pull and push queries.
    """
    result: StreamedQueryResult = await client.stream_query_async('select * from RATINGS EMIT CHANGES;')
    print(f'Got result query id: {result.query_id}')
    
    while not result.is_complete():
        # Poll for a row
        row: Row = await result.poll_async()
        print(f'Row: {row}')
    
    # Close the client and dispose of any resources that it uses
    await client.close()
    
if __name__ == "__main__":
    asyncio.run(main())
```

Receive query results in a single batch (execute_query_async())<a name="execute-query-async"></a>
-------------------------------------------------------------------------------------------------

The `execute_query_async()` method enables client apps to receive query results as a single batch
that's returned when the query completes.

This method is suitable for both pull queries and for terminating push queries,
for example, queries that have a `LIMIT` clause. For non-terminating push queries,
use the [`stream_query_async()`](#stream-query-async) method instead.

### Example Usage ###
```python
import asyncio

from ksqldb_confluent.client import Client
from ksqldb_confluent.batched_query_result import BatchedQueryResult

async def main():
    #Initialize the client with the host and port
    client: Client = Client(host='localhost', port=8088)
    
    """
    Execute a Pull query. 
    
    client.execute_query_async() is appropriate for queries which can run to 
    completion in a batch, namely pull queries and push queries with LIMIT statements.
    """
    result: BatchedQueryResult = await client.execute_query_async('select * from RATINGS LIMIT 3;')
    print(f'Got result query id: {result.query_id}')
    
    # Print the rows received
    for row in result.rows:
        print(f'Row: {row}')
    
    
    # Close the client and dispose of any resources that it uses
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```