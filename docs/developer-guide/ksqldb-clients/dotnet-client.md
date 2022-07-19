---
layout: page
title: .NET Client for ksqlDB
tagline: .NET client for ksqlDB
description: Send requests to ksqlDB from your .NET app
keywords: ksqlDB, .NET, dotnet, client
---

ksqlDB has a .NET client that enables sending requests easily to a ksqlDB server
from within your .NET application, as an alternative to using the [REST API](../api.md).
The client supports pull and push queries. Future versions may include the
creation of streams and tables, as well as admin operations.

!!! tip
[View the .NET client API documentation](dotnet-client-api/api/Confluent.KsqlDb.html)

The client sends requests using HTTP 1.1 with pull and push queries served by
the [`/query-stream` endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#executing-pull-or-push-queries),
The client is compatible only with ksqlDB deployments that are on version 0.24.0 or later.

Use the .NET client to:

- [Receive query results one row at a time (StreamQueryAsync())](#stream-query)
- [Receive query results in a single batch (ExecuteQueryAsync())](#execute-query)

Getting Started
---------------

Start by creating a `.csproj` for your C# application:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net6.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Confluent.KsqlDb" Version="0.1.0" />
  </ItemGroup>

</Project>
```

Create your example app at `ExampleSolution/ExampleApp/ExampleApp.cs`:

```csharp
using Confluent.KsqlDb;

Console.WriteLine("Test application!");

IClient client = ClientBuilder
    .NewBuilder()
    .SetHost("localhost")
    .SetPort(8088)
    .SetUseTls(false)
    .Build();

...

client.Close();

```

You can use the `ClientBuilder` class to connect your CSharp client to
{{ site.ccloud }}. For more information, see
[Connect to a {{ site.ccloud }} ksqlDB cluster](#connect-to-cloud).

Receive query results one row at a time (StreamQueryAsync())<a name="stream-query"></a>
----------------------------------------------------------------------------------

The `StreamQueryAsync()` method enables client apps to receive query results one row at a time,
asynchronously using the built in `await` mechanism.

You can use this method to issue both push and pull queries, but the usage pattern is better for push queries.
For pull queries, consider using the [`ExecuteQueryAsync()`](#execute-query)
method instead.

Query properties can be passed as an optional second argument along with a cancellation token 
as an optional third argument. For more information, see the
[client API reference](dotnet-client-api/api/Confluent.KsqlDb.IClient.html#Confluent_KsqlDb_IClient_StreamQueryAsync_System_String_CancellationToken_).

By default, push queries return only newly arriving rows. To start from the beginning of the stream or table,
set the `auto.offset.reset` property to `earliest`.

```csharp
public interface IClient {

  /// <summary>
  /// Executes a query. Note that this is limited to pull and push queries and does not cover
  /// other statement type commands.
  /// 
  /// This is appropriate for queries which may not run to immediate completion, so this works well
  /// for both pull and push queries.
  /// </summary>
  /// <param name="sql">The sql query to run</param>
  /// <param name="properties">The properties to send along with the query</param>
  /// <param name="cancellationToken">The cancellation token for stopping the query</param>
  /// <returns>A task for the result</returns>
  Task<IStreamedQueryResult> StreamQueryAsync(string sql, IDictionary<string, object> properties = null,
          CancellationToken cancellationToken = default);
  ...
  
}
```

### Example Usage ###

Due to C#'s built in async/await functionality, we can write async code without a lot of the
complexity that in other clients.  All of our API methods return [Task](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1) objects and can be awaited.

To consume records one-at-a-time, use the `PollAsync()` method on the query result object.
To check for successful completion, the property `IsComplete` can be used.  If a failure
has occurred, the property `IsFailed` is true and the next call to `PollAsync()` will throw an exception.

```csharp
IClient client = ClientBuilder
    .NewBuilder()
    .SetHost("localhost")
    .SetPort(8088)
    .SetUseTls(false)
    .Build();

IStreamedQueryResult result = await client.StreamQueryAsync(
    "SELECT * FROM MY_STREAM EMIT CHANGES;");
    
await Console.Out.WriteLineAsync("Got result query id " + result.QueryId);
await Console.Out.WriteLineAsync("Schema: " + result.Schema);

while (!result.IsComplete)
{
    IRow row = await result.PollAsync();
    if (row == null)
    {
        continue;
    }
  
    await Console.Out.WriteLineAsync("Row " + row);
}

client.Close();
```

Because all of these methods return a [Task](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1),
you can use any number of methods and properties to block the current thread to
get the result, including reading the `task.Result` property or using the various
`task.Wait(...)` methods.

Receive query results in a single batch (executeQuery())<a name="execute-query"></a>
------------------------------------------------------------------------------------

The `ExecuteQueryAsync()` method enables client apps to receive query results as a single batch
that's returned when the query completes.

This method is suitable for both pull queries and for terminating push queries,
for example, queries that have a `LIMIT` clause. For non-terminating push queries,
use the [`StreamQueryAsync()`](#stream-query) method instead.

Query properties can be passed as an optional second argument along with a cancellation token
as an optional third argument. For more information, see the 
[client API reference](dotnet-client-api/api/Confluent.KsqlDb.IClient.html#Confluent_KsqlDb_IClient_ExecuteQueryAsync_System_String_CancellationToken_).

By default, push queries return only newly arriving rows. To start from the beginning of the stream or table,
set the `auto.offset.reset` property to `earliest`.

```csharp
  public interface IClient
  {
      /// <summary>
      /// Executes a query. Note that this is limited to pull and push queries and does not cover
      /// other statement type commands.
      /// 
      /// Also, this is appropriate for queries which can run to completion in a batch, namely pull
      /// queries and push queries with limit statements. Push queries without limit statements will
      /// never complete.
      /// </summary>
      /// <param name="sql">The sql query to run</param>
      /// <param name="properties">The properties to send along with the query</param>
      /// <param name="cancellationToken">The cancellation token for stopping the query</param>
      /// <returns>A task for the result</returns>
      Task<IBatchedQueryResult> ExecuteQueryAsync(string sql, IDictionary<string, object> properties = null, 
          CancellationToken cancellationToken = default);
          
      ...
  }
```

### Example Usage ###

```csharp
client = ClientBuilder
    .NewBuilder()
    .SetHost("localhost")
    .SetPort(8088)
    .SetUseTls(false)
    .Build();

IBatchedQueryResult result = await client.ExecuteQueryAsync("select * from RATINGS_BY_USER;");

await Console.Out.WriteLineAsync("Got result query id:" + result.QueryId);
await Console.Out.WriteLineAsync("Schema: " + result.Schema);

foreach (IRow row in result.Rows)
{
    await Console.Out.WriteLineAsync("Row " + row);
}
```

Similar to the streaming version, because all of these methods return a
[Task](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1),
you can use any number of methods and properties to block the current thread to
get the result, including reading the `task.Result` property or using the various
`task.Wait(...)` methods.

Connect to a {{ site.ccloud }} ksqlDB cluster <a name="connect-to-cloud"></a>
-----------------------------------------------------------------------------

Use the following code snippet to connect your Java client to a hosted ksqlDB
cluster in {{ site.ccloud }}.

```csharp
ClientOptions options = ClientOptions.create()
   .SetBasicAuthCredentials("<ksqlDB-API-key>", "<ksqlDB-API-secret>")
   .SetHost("<ksqlDB-endpoint>")
   .SetPort(443)
   .SetUseTls(true);
```

Get the API key and endpoint URL from your {{ site.ccloud }} cluster.

- For the API key, see
  [Create an API key for Confluent Cloud ksqlDB](https://docs.confluent.io/cloud/current/cp-component/ksqldb-ccloud-cli.html#create-an-api-key-for-ccloud-ksql-cloud-through-the-ccloud-cli).
- For the endpoint, run the `ccloud ksql app list` command. For more information,
  see [Access a ksqlDB application in Confluent Cloud with an API key](https://docs.confluent.io/cloud/current/cp-component/ksqldb-ccloud-cli.html#access-a-ksql-cloud-application-in-ccloud-with-an-api-key).
