---
layout: page
title: Python Client for ksqlDB
tagline: Python client for ksqlDB
description: Send requests to ksqlDB from your Python app
keywords: ksqlDB, python, client
---

ksqlDB has a Python client that enables sending requests easily to a ksqlDB server
from within your Python application, as an alternative to using the [REST API](../api.md).
The client supports pull and push queries. Future versions may include the
creation of streams and tables, as well as admin operations.

The Github project lives [here](https://github.com/confluentinc/confluent-ksqldb-python). The PyPi package lives [here](https://pypi.org/project/confluent-ksqldb/).

!!! tip
    [View the Python client API documentation](python-client-api/index.html)

The client sends queries to the [`/query-stream` endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#executing-pull-or-push-queries).
The client is compatible only with ksqlDB deployments that are on version 0.24.0 or later.

Use the Python client to:

- [Receive query results one row at a time asynchronously (stream_query_async())](#stream-query-async)
- [Receive query results in a single batch asynchronously (execute_query_async())](#execute-query-async)

Getting Started
---------------
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install `confluent-ksqldb`.

```bash
pip install confluent-ksqldb
```

Receive query results one row at a time (stream_query_async())<a name="stream-query-async"></a>
-----------------------------------------------------------------------------------------------
The `stream_query_async()` method enables client apps to receive query results one row at a time asynchronously using the built in `await` mechanism of Python's `asyncio`.

You can use this method to issue both push and pull queries, but the usage pattern is better for push queries.
For pull queries, consider using the [`execute_query_async()`](#execute-query-async)
method instead.

By default, push queries return only newly arriving rows. To start from the beginning of the stream or table,
set the `auto.offset.reset` property to `earliest`.

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

Connect to a {{ site.ccloud }} ksqlDB cluster <a name="connect-to-cloud"></a>
-----------------------------------------------------------------------------

Use the following code snippet to connect your Python client to a hosted ksqlDB
cluster in {{ site.ccloud }}.

```python
client: Client = Client(host='<ksqlDB-endpoint>', port=<port>, username='<ksqlDB-API-key>', password='<ksqlDB-API-secret>')
```

Get the API key and endpoint URL from your {{ site.ccloud }} cluster.

- For the API key, see
  [Create an API key for Confluent Cloud ksqlDB](https://docs.confluent.io/cloud/current/cp-component/ksqldb-ccloud-cli.html#create-an-api-key-for-ccloud-ksql-cloud-through-the-ccloud-cli).
- For the endpoint, run the `ccloud ksql app list` command. For more information,
  see [Access a ksqlDB application in Confluent Cloud with an API key](https://docs.confluent.io/cloud/current/cp-component/ksqldb-ccloud-cli.html#access-a-ksql-cloud-application-in-ccloud-with-an-api-key).
