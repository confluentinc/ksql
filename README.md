# ![rocket](ksql-rocket.png)  KSQL 

*A Streaming SQL Engine for Apache Kafka™ from Confluent*

**DEVELOPER PREVIEW**

KSQL is an open source streaming SQL engine that implements continuous, interactive queries against Apache Kafka™. It allows you to query, read, write, and process data in Apache Kafka in real-time, at scale using SQL commands. 

KSQL does not require proficiency with a programming language such as Java or Go, and it does not require you to install and manage a separate processing cluster technology. As such, it opens up the world of stream processing to a broader set of users and applications than ever before.

This release is a DEVELOPER PREVIEW which is free and open-source from Confluent under the Apache 2.0 license.
KSQL consists of a client and a server component. The client is a command line interface (CLI) similar to the CLIs of MySQL or PostgreSQL. The server, of which you can run one or many instances, executes those queries for you.

You can use KSQL in stand-alone mode and/or in client-server mode.

In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![alt text](https://user-images.githubusercontent.com/2977624/29090610-f4b11096-7c34-11e7-8a63-85c9ead22bc3.png)

In client-server mode, you can run a pool of KSQL servers on remote machines, VMs, or containers and the CLI connects to them over HTTP.

![alt text](https://user-images.githubusercontent.com/2977624/29090617-fab5e930-7c34-11e7-9eee-0554192854d5.png)

# Installation

**Prerequisites:**

- [Maven](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/downloads)
- Java: Minimum version 1.7. 

1.  Clone the Confluent KSQL repository:

	```bash
	git clone https://github.com/confluentinc/ksql
	```

1.  Navigate to the KSQL directory:

	```bash
	cd ksql
	```

1.  Compile the KSQL code:

	```bash
	mvn clean install
	```

1.  Start KSQL by running the compiled jar file ksql-cli/target/ksql-cli-1.0-SNAPSHOT-standalone.jar. Use the local argument for the developer preview. This starts the KSQL engine locally.

	```bash
	java -jar ksql-cli/target/ksql-cli-1.0-SNAPSHOT-standalone.jar local
	```

	When this command completes, you should see the KSQL prompt:

	```bash
	                       ======================================
	                       =      _  __ _____  ____  _          =
	                       =     | |/ // ____|/ __ \| |         =
	                       =     | ' /| (___ | |  | | |         =
	                       =     |  <  \___ \| |  | | |         =
	                       =     | . \ ____) | |__| | |____     =
	                       =     |_|\_\_____/ \___\_\______|    =
	                       =                                    =
	                       = Streaming Query Language for Kafka =
	Copyright 2017 Confluent Inc.                         

	CLI v0.0.1, Server v0.0.1 located at http://localhost:9098

	Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

	ksql> 
	```

# Documentation
You can [find the KSQL documentation here](/docs/). 

# Getting Help
---
If you need help or have questions, you have several options:
* Ask a question in the #ksql channel in our public [Confluent Community Slack](https://confluent.typeform.com/to/GxTHUD). Account registration is free and self-service.
* Create a [ticket](https://github.com/confluentinc/ksql) in our issue [tracker](https://github.com/confluentinc/ksql).
* Join the [Confluent google group](https://groups.google.com/forum/#!forum/confluent-platform).

# Contributing to KSQL
---
*This section contains information about how to contribute code and documentation, etc.*

To build KSQL locally:

```sh
$ git clone https://github.com/confluentinc/ksql.git
$ cd ksql
$ mvn clean package
```

# License
---
The project is licensed under the Apache License, version 2.0.

*Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/)*

