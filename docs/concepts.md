# Concepts

| [Overview](/docs/) |[Quick Start](/docs/quickstart#quick-start-guide) | Concepts | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  | [Roadmap](/docs/roadmap.md#roadmap) | [Demo](/docs/demo.md#demo) |
|---|----|-----|----|----|----|----|----|


**Table of Contents**
- [Components](#components)
- [Terminology](#terminology)
- [Modes of operation](#modes-of-operation)

# Components
The main components of KSQL are CLI, engine, and the REST interface.

**CLI**
Provides a familiar interface, designed users of MySQL, Postgres, etc.

**Engine**
Runs the Kafka Streams topologies.

**REST interface**
Enables an engine to receive instructions from the CLI.

# Terminology 
When using KSQL, the following terminology is used.

**Stream**
A stream is an unbounded sequence of structured values that are stored in a [Kafka topic](https://kafka.apache.org/documentation/#intro_topics). The structure of the values is specified in a schema. In Kafka streams vocabulary, a KSQL stream is a [KStream](http://docs.confluent.io/current/streams/concepts.html?highlight=kstream#kstream) plus a schema. 

**Table**
A table in KSQL is finite, where the bounds are defined by the size of the key space. The key space is an evolving collection of structured values, where the structure of the values is specified in a schema. These values are stored in a changelog topic in Kafka. In Kafka Streams vocabulary, a KSQL table is a [KTable](http://docs.confluent.io/current/streams/concepts.html?highlight=ktable#ktable) plus a schema.

# Modes of operation

#### Standalone mode
In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![Standalone mode](/docs/img/standalone-mode.png)

Here's an overview of running KSQL in standalone mode:

- Starts a CLI, an Engine, and a REST server all in the same JVM
- Ideal for laptop development
	-  Use with default settings:

	   ```bash
	   .bin/ksql-cli local
	   ```	

	-  Use with custom settings:

	   ```bash
	   .bin/ksql-cli local --properties-file foo/bar/ksql.properties
	   ```

#### Client-server mode
In client-server mode, you can run a pool of KSQL servers on remote machines, VMs, or containers and the CLI connects to them over HTTP.

![Client-server mode](/docs/img/client-server.png)

Here's an overview of running KSQL in client-server mode:

- Start any number of server nodes
	-  Use with default settings:

	   ```bash
	   .bin/ksql-server-start
	   ```	

	-  Use with custom settings:

	   ```bash
	   .bin/ksql-server-start --properties-file foo.properties
	   ```
- Start any number of CLIs, specifying a server address as `remote` endpoint
  
  ```bash
  .bin/ksql-cli remote http://server:8090
  ```

- All engines share the work, for example, instances of the same KStreams apps. You can scale up or down without restarting.

#### Application mode
In application mode, you can put your KSQL queries in a file and share across your Kakfa Streams instances. 

![Application mode](/docs/img/application-mode.png)

Here's an overview of running KSQL in application mode:

- Start an engine instance and pass a file of KSQL statements to run, for example:

  ```bash
  .bin/ksql-node --query-file=foo/bar.sql
  ```
  or

  ```bash
  .bin/ksql-node --properties-file ksql.properties --query-file=foo/bar.sql
  ```
- This mode is ideal for streaming-ETL application deployment, for example, you can version-control your queries as code.
- All engines share the work, for example, instances of the same KStreams app. You can scale up or down without restarting.
 
#### Embedded mode
In embedded mode, you can write KSQL code inside of your streams Java app, using the KSQL context object inside of your application. The KSQL code will run inside the individual application instances. For more information, see [this example](/ksql-examples/src/main/java/io/confluent/ksql/embedded/EmbeddedKsql.java).

