# KLIP 45 - ksqlDB LINQ provider

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
- [Proof of concept project](https://github.com/tomasfabian/Joker/tree/master/Samples/Kafka/Kafka.DotNet.ksqlDB.Sample)

## What is not in scope

[ksqldb-api-client](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/api/io/confluent/ksql/api/client/Client.html) is not in the scope of this project. 
Only the executeStream part is relevant for the above mentioned LINQ provider. 

This LINQ provider could be used for type safe push queries not covered in [KLIP 41 - ksqlDB .NET Client](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-41-ksqldb-.net-client.md) or later .NET ksqldb-api-client could be plugged into this project as the http query-stream provider. Since it is not available at the time of writing I created a custom implementation.

## Value/Return

Increased development productivity in dotnet environment using C# with api comparable to pull versions like [LINQ to Entity Framework](https://docs.microsoft.com/en-us/ef/core/querying/) for SQL SERVER, [LINQ to OData](https://docs.microsoft.com/en-us/odata/client/query-options) for OData web services
or in process [reactive extensions](http://rxwiki.wikidot.com/101samples).

## Public APIS

```
Install-Package Kafka.DotNet.ksqlDB -Version 0.1.0-alpha
```

```C#
var ksqlDbUrl = @"http:\\localhost:8088";

using var disposable = new KQueryStreamSet<Tweet>(new QbservableProvider(ksqlDbUrl))
  .Where(p => p.Message != "Hello world" || p.Id == 1)
  .Select(l => new { l.Message, l.Id })
  .Take(2)
  .Subscribe(tweetMessage =>
  {
    Console.WriteLine($"{nameof(Tweet)}: {tweetMessage.Id} - {tweetMessage.Message}");
  }, error => { Console.WriteLine($"Exception: {error.Message}"); }, () => Console.WriteLine("Completed"));
```

[Projects location](https://github.com/tomasfabian/Joker/tree/master/Joker.Kafka)

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

[Unit tests project](https://github.com/tomasfabian/Joker/tree/master/Tests/Joker.Kafka.Tests) - work in progress 


## Documentation Updates
[Wiki](https://github.com/tomasfabian/Joker/wiki/Kafka.DotNet.ksqlDB---push-queries-LINQ-provider) - work in progress