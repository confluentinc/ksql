# KSQL - The Streaming SQL Engine for Apache Kafka

*A Streaming SQL Engine for Apache Kafka from Confluent*

KSQL is an open source streaming SQL engine that implements continuous, interactive queries against Apache Kafka™. It allows you to query, read, write, and process data in Apache Kafka in real-time, at scale using SQL commands. 

KSQL does not require proficiency with a programming language such as Java or Go, and it does not require you to install and manage a separate processing cluster technology. As such, it opens up the world of stream processing to a broader set of users and applications than ever before.

KSQL consists of a client and a server component.  The client is a command line interface (CLI) similar to the CLIs of MySQL or PostgreSQL. The server, of which you can run one or many instances, executes those queries for you.

> *Important: This release is a *developer preview* and is free and open-source from Confluent under the Apache 2.0 license.* 

- [Quick Start Guide](#quick-start-guide)
- [Installation](#installation)
- [Documentation](#documentation)
- [Join the Community](#join-the-community)
- [Contributing](#contributing)
- [License](#license)

# Quick Start Guide
If you are ready to see the power of KSQL, try the [KSQL Quick Start](/docs/quickstart/)! The quick start configures a single instance in a lightweight Docker container or in a Kafka cluster. It demonstrates a simple workflow using KSQL to write streaming queries against data in Kafka.

# Installation

**Prerequisites:**

- [Maven](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/downloads)
- A Kafka cluster
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
	mvn clean install -Dmaven.test.skip=true
	```

1.  Start KSQL by running the compiled jar file ksql-cli/target/ksql-cli-4.0.0-SNAPSHOT-standalone.jar. Use the local argument for the developer preview. This starts the KSQL engine locally.

	```bash
	./bin/ksql-cli local
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

# Join the Community
Whether you need help, want to contribute, or are just looking for the latest news, you can find out how to [connect with your fellow Confluent community members here](https://www.confluent.io/contact-us-thank-you/).

* Ask a question in the #ksql channel in our public [Confluent Community Slack](https://confluent.typeform.com/to/GxTHUD). Account registration is free and self-service.
* Create a [ticket](https://github.com/confluentinc/ksql) in our issue [tracker](https://github.com/confluentinc/ksql).
* Join the [Confluent google group](https://groups.google.com/forum/#!forum/confluent-platform).

# Contributing
Contributions to the code, examples, documentation, etc, are very much appreciated. For more information, see the [contribution guidelines](/docs/contributing.md).

- Report issues directly in [this GitHub project](https://github.com/confluentinc/ksql/issues).

# License
The project is licensed under the Apache License, version 2.0.

*Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/)*

