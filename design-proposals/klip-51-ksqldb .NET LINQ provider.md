# KLIP 51 - ksqlDB LINQ provider

**Author**: Tomas Fabian (tomasfabian) | 
**Release Target**: ksqldb 0.14.0 | 
**Status**: _In Development_ | 
**Discussion**: TBD
           
The aim of this package is to provide a compile type safe [LINQ Provider](https://docs.microsoft.com/en-us/dynamics365/fin-ops-core/dev-itpro/dev-tools/linq-provider-c) for ksqlDB push queries generation and subscription similarly to in process [Rx.NET](https://github.com/dotnet/reactive) or [Rx.Java](https://github.com/ReactiveX/RxJava) etc., but executed server side. Only the observations will be accomplished locally.

## Motivation and background

kafka.dotnet.ksqldb enables you to easily create a ksqldb push query provider that offers an IQbservable<T> for clients to query ksqldb using reactive LINQ in .NET 5 or .NET Core 3.1.


To avoid heavy weight reflection this project treats code as data with the help of [expression trees](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/expression-trees/)

## What is in scope

- A new client is being implemented in C# and will support execution and streaming of push queries with type safe LINQ syntax.
- [Proof of concept project](https://github.com/tomasfabian/Kafka.DotNet.ksqlDB/tree/main/Samples)

## What is not in scope

[ksqldb-api-client](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/api/io/confluent/ksql/api/client/Client.html) is not in the scope of this project. 
Only the executeQuery part is relevant for the above mentioned LINQ provider. 

This LINQ provider could be used for type safe push queries not covered in [KLIP 41 - ksqlDB .NET Client](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-41-ksqldb-.net-client.md) or later .NET ksqldb-api-client could be plugged into this project as the http query-stream provider. Since it is not available at the time of writing I created a custom implementation.

## Value/Return

Increased development productivity in dotnet environment using C# with api comparable to pull versions like [LINQ to Entity Framework](https://docs.microsoft.com/en-us/ef/core/querying/) for SQL SERVER, [LINQ to OData](https://docs.microsoft.com/en-us/odata/client/query-options) for OData web services
or in process [reactive extensions](http://rxwiki.wikidot.com/101samples).

## Public APIS
Nuget package manager:
```
Install-Package Kafka.DotNet.ksqlDB
```

```C#
using Kafka.DotNet.ksqlDB.KSql.Linq;
using Kafka.DotNet.ksqlDB.KSql.Query;
using Kafka.DotNet.ksqlDB.KSql.Query.Context;
using Kafka.DotNet.ksqlDB.KSql.Query.Context.Options;
using Kafka.DotNet.ksqlDB.KSql.Query.Options;
using Kafka.DotNet.ksqlDB.KSql.RestApi.Parameters;
using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Example
{
  public class Program
  {
    private static async Task Main() {
      var ksqlDbUrl = @"http:\\localhost:8088";

      var contextOptions = new KSqlDbContextOptionsBuilder()
        .UseKSqlDb(ksqlDbUrl)
        .SetupQueryStream(options =>
        {
          options.Properties[QueryParameters.AutoOffsetResetPropertyName] = AutoOffsetReset.Earliest.ToString().ToLower();
        })
        .Options;

      await using var context = new KSqlDBContext(contextOptions);

      using var disposable = context.CreateQueryStream<Tweet>()
        .Where(p => p.Message != "Hello world" || p.Id == 1)
        .Where(p => p.RowTime >= 1510923225000) //AND RowTime >= 1510923225000
        .Select(l => new { l.Id, l.Message, l.RowTime })
        .Take(2)     
        .ToObservable() // client side processing starts here lazily after subscription
        .Delay(TimeSpan.FromSeconds(2)) // IObservable extensions
        .ObserveOn(TaskPoolScheduler.Default)
        .Subscribe(tweetMessage =>
        {
          Console.WriteLine($"{nameof(Tweet)}: {tweetMessage.Id} - {tweetMessage.Message}");
          Console.WriteLine();
        }, error => { Console.WriteLine($"Exception: {error.Message}"); }, () => Console.WriteLine("Completed"));

      Console.ReadKey();
    }

    public class Tweet : Record
    {
      public int Id { get; set; }
      public string Message { get; set; }
    }
  }
}
```

C# code is translated into an Abstract Syntax Tree and the following KSQL is generated:
```KSQL
SELECT Id, Message, RowTime 
FROM Tweets
WHERE Message != 'Hello world' OR Id = 1 AND RowTime >= 1510923225000 
EMIT CHANGES 
LIMIT 2;
```

Supported are also aggregations and base type functions, windows and joins.

[Projects location](https://github.com/tomasfabian/Kafka.DotNet.ksqlDB)

## Design

```C#
  public interface IQbservable
  {
    Type ElementType { get; }

    Expression Expression { get; }

    IKSqlQbservableProvider Provider { get; }
  }

  public interface IQbservable<out T> : IQbservable
  {
    IDisposable Subscribe(IObserver<T> observer);
  }
```

This interface could be later inherited from IObservable to achieve parity with all possible [extension methods](https://docs.microsoft.com/en-us/previous-versions/dotnet/reactive-extensions/hh212048(v=vs.103)). Since there is a symmetry this IQbservable provider could be later converted ToObservable and continue with not supported operations locally with Rx.NET

<img src="https://sec.ch9.ms/ecn/content/images/WhatHowWhere.jpg" />

## Test plan

[Unit tests project](https://github.com/tomasfabian/Kafka.DotNet.ksqlDB/tree/main/Tests/Kafka.DotNet.ksqlDB.Tests) - work in progress 

[Integration tests project](https://github.com/tomasfabian/Kafka.DotNet.ksqlDB/tree/main/Tests/Kafka.DotNet.ksqlDB.IntegrationTests) - work in progress

## Documentation Updates
[Wiki](https://github.com/tomasfabian/Joker/wiki/Kafka.DotNet.ksqlDB---push-queries-LINQ-provider) - work in progress