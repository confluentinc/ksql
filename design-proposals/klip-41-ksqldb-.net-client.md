# KLIP 41 - ksqlDB .NET Client

**Author**: Alex Basiuk (@alex-basiuk) | 
**Release Target**: TBD | 
**Status**: _In Discussion_ | 
**Discussion**: TBD

**tl;dr:** _Create a ksqlDB client for .NET which is on par with the existing Java client.
The .NET community will benefit from the this project. The goal is to provide first class developer experience and 
simplify writing event streaming applications in .NET._

## Motivation and background

Basically, it's similar to [KLIP 15](./klip-15-new-api-and-client.md).
It should lower the barrier for .NET developers and increase adoption of ksqlDB by the community.
The library should offer the same feature set as the Java library and should follow common .NET conventions and patterns.  

## What is in scope

* A new client implemented in C#.
* Initially, the client will be targeted at .NET Core 3 and .NET Framework 5. Support of older versions will be added later.
* The client will support execution and streaming of queries (both push and pull), inserting of rows into streams, DDL operations and admin operations
such as list and describe.
* The client will utilise C# [async streams](https://docs.microsoft.com/en-us/dotnet/csharp/tutorials/generate-consume-asynchronous-stream) feature.
* The client will also support a direct / synchronous interaction style.
* High-quality documentation and examples.

## What is not in scope

No plans to provide more functional API for F#. However, it will be possible to use the client in F#.

## Value/Return

Ideas of event sourcing / CQRS / DDD style apps are popular in the .NET community but ksqlDB is often perceived as a Java-specific technology.
A native client will make it easier to try and hopefully adopt ksqlDB by .NET developers.

## Public APIS

N/A

## Design

* The client will target multiple [target frameworks](https://docs.microsoft.com/en-us/dotnet/standard/frameworks).
Initially it will be netcoreapp3.0 and net5.0. Support of older versions e.g. netstandard2.0 will be added at later stage.
* The client will provide an API based on async streams for streaming and async methods for the rest.
* The client will support injectable [Polly policies](https://github.com/App-vNext/Polly) to enable users to use custom resilience policies. However, out of the box, it should use sensible default retry policies.
* The client will expose convenience builder extension methods for ASP.NET Core. It should be easy to add and configure the client in ASP.NET Core idiomatic way. 
  But the mechanism will be optional. Users won't be required to use it or another DI. 
* The client will have minimal dependencies on third party libraries.
* The client will follow performance best practices.
* The client will be thread-safe.

## Test plan

Unit and integration test will be mandatory. Performance benchmarks are desirable but unlikely be the first priority.
Ideally, tests should be executed on both Windows and Linux platforms.

## LOEs and Delivery Milestones
TBD

## Documentation Updates

* High quality documentation is required.
* Example applications showing how to use the client in a real app.

## Compatibility Implications

N/A

## Security Implications

The client will support all protocols supported by the ksqlDB REST API.
