# Concepts

| [Overview](/docs#ksql-documentation) |[Quick Start](/docs/quickstart#quick-start) | Concepts | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  | [Roadmap](/docs/roadmap.md#roadmap) | 
|---|----|-----|----|----|----|----|----|


**Table of Contents**
- [Components](#components)
- [Terminology](#terminology)
- [Modes of operation](#modes-of-operation)

# Components
The main components of KSQL are CLI, engine, and the REST interface.

#### CLI
Provides a familiar interface, designed users of MySQL, Postgres, etc.

#### Engine
Performs the data processing.

#### REST interface
Enables an engine to receive instructions from the CLI.

# Terminology
When using KSQL, the following terminology is used.

#### Stream

A stream is an unbounded sequence of structured data ("facts").  For example, we could have a stream of financial transactions such as "Alice sent $100 to Bob, then Charlie sent $50 to Bob".  Facts in a stream are immutable, which means new facts can be inserted to a stream, but existing facts can never be updated or deleted.  Streams can be created from a Kafka topic or derived from existing streams and tables.

#### Table

A table is a view of a stream, or another table, and represents a collection of evolving facts.  For example, we could have a table that contains the latest financial information such as "Bob’s current account balance is $150".  It is the equivalent of a traditional database table but enriched by streaming semantics such as windowing.  Facts in a table are mutable, which means new facts can be inserted to the table, and existing facts can be updated or deleted.  Tables can be created from a Kafka topic or derived from existing streams and tables.


# Modes of operation

## Standalone mode
In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![Standalone mode](/docs/img/standalone-mode.png)

Here's an overview of running KSQL in standalone mode:

- Starts a CLI, an Engine, and a REST server all in the same JVM
- Ideal for laptop development
	-  Use with default settings:

	   ```bash
	   ./bin/ksql-cli local
	   ```	

	-  Use with custom settings:

	   ```bash
	   ./bin/ksql-cli local --properties-file foo/bar/ksql.properties
	   ```

## Client-server mode
In client-server mode, you can run a pool of KSQL servers on remote machines, VMs, or containers.  The CLI then connects to these remote KSQL servers over HTTP.

![Client-server mode](/docs/img/client-server.png)

Here's an overview of running KSQL in client-server mode:

- Start any number of server nodes
	-  Use with default settings:

	   ```bash
	   ./bin/ksql-server-start
	   ```

	-  Use with custom settings:

	   ```bash
	   ./bin/ksql-server-start --properties-file foo.properties
	   ```
- Start any number of CLIs, specifying a server address as `remote` endpoint

  ```bash
  ./bin/ksql-cli remote http://server:8090
  ```

- All KSQL servers (and their engines) share the work of processing KSQL queries that are submitted to them.
    - To add processing capacity, start more KSQL servers (scale out).  You can do this during live operations.
    - To remove processing capacity, stop some of the running KSQL servers.  You can do this during live operations.
      The remaining KSQL servers will automatically take over the processing work of the stopped servers.  Make sure
      that at least one KSQL server is running, otherwise your queries will not be executed any longer.

<!--
## Application mode
In application mode, you can put your KSQL queries in a file and share across your Kafka Streams instances.

![Application mode](/docs/img/application-mode.png)

Here's an overview of running KSQL in application mode:

- Start an engine instance and pass a file of KSQL statements to run, for example:

  ```bash
  ./bin/ksql-node --query-file=foo/bar.sql
  ```
  or

  ```bash
  ./bin/ksql-node --properties-file ksql.properties --query-file=foo/bar.sql
  ```
- This mode is ideal for streaming-ETL application deployment, for example, you can version-control your queries as code.
- All engines share the work, for example, instances of the same KSQL app. You can scale up or down without restarting.
 
## Embedded mode
In embedded mode, you can write KSQL code inside of your streams Java app, using the KSQL context object inside of your application. The KSQL code will run inside the individual application instances. For more information, see [this example](/ksql-examples/src/main/java/io/confluent/ksql/embedded/EmbeddedKsql.java).

-->

