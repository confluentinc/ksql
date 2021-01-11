# KLIP 44 - ksqlDB Go (Golang) Client

**Author**: Vance Longwill (@VanceLongwill) |
**Release Target**: TBD | 
**Status**: _In Discussion_ | 
**Discussion**: TBD

**tl;dr:** Create an idiomatic ksqlDB client for Go. The client should make it easier for Go developers to
          adopt ksqlDB by providing a familiar & flexible way to interact with ksqlDB in a similar way to other
          databases.
          
           
## Motivation and background

Simplify and standardise using ksqlDB in Go. The current support is nonexistent and adoption is low in the Go community. Creating a 
easy to use lib would benefit developers looking to utilise both Kafka & ksqlDB.

## What is in scope

* A new client implemented in Go which offers largely the same features as the Java client.
* Support for different access patterns:
  - A configurable HTTP client.
  - A driver for the Go standard library's `database/sql` package, allowing ksqlDB to be accessed in a way similar to any other SQL database.
  - Support for synchronous & streamed queries in both cases.
* High quality documentation & examples

## What is not in scope
 
> Additional options that are important to expose include: - Support for TLS-enabled ksqlDB servers - Support for mutual-TLS-enabled ksqlDB servers - Support for ksqlDB servers with HTTP basic authentication enabled.
- This will be configurable by an option which allows the user to specify a custom `*http.Client`

## Value/Return

Using ksqlDB in Go applications currently requires writing your own HTTP client. Standardisation of this would improve
developer experience and encourage adoption. At the moment there is also no Go support for Kafka streams, which means ksqlDB 
is the only viable option for querying Kafka messages. In addition to this, there are currently 3+ Kafka Go clients, each with 
its own pros/cons. For event sourced/event driven apps, ksqlDB might be a way out of this fragmentation.

Including a `database/sql` driver would allow Go developers to make use of the many compatible libraries for converting data, building queries, mocking and more 
as well as offering an idiomatic API.

## Public APIS

N/A

## Design

* Create a HTTP client layer which takes care of marshalling and unmarshalling responses from the ksqlDB API.
* A custom `*http.Client` should also be injectable.
* The responsibility for auth, retrying requests will be placed with the user.
* Based on the underlying HTTP client layer, create an adapter which fulfills interfaces from the standard library's `database/sql/driver` package.
* Include helpers for the most common options and parameters, with room to expand on this in the future.


## Test plan

Mostly unit testing with some integration tests and some complete runnable examples.

## LOEs and Delivery Milestones

TBD

## Compatibility Implications

N/A


## Security Implications

The client will support all protocols supported by the ksqlDB REST API.
