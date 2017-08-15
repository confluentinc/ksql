# Installation

| [Overview](/docs/overview.md) | Installation | [Quick Start Guide](/docs/installation.md) | [Syntax Reference](/docs/syntax-reference.md) | [Examples](/docs/examples.md) | [FAQ](/docs/faq.md)  |
|----------|--------------|-------------|------------------|------------------|------------------|

You can quickly install KSQL in your environment. 

You can use KSQL in stand-alone mode or in client-server mode.

In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![alt text](https://user-images.githubusercontent.com/2977624/29090610-f4b11096-7c34-11e7-8a63-85c9ead22bc3.png)

In client-server mode, you can run a pool of KSQL servers on remote machines, VMs, or containers and the CLI connects to them over HTTP.

![alt text](https://user-images.githubusercontent.com/2977624/29090617-fab5e930-7c34-11e7-9eee-0554192854d5.png)

**Prerequisites:**

- [Maven](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/downloads) and [Confluent KSQL repository](https://github.com/confluentinc/ksql)
- A Kafka cluster
- Java: Minimum version 1.7. 

1.  Navigate to the KSQL root directory and compile the KSQL code:

	```bash
	mvn clean install
	```

	When this command completes, the output should resemble:

	```bash
	...
	[INFO] BUILD SUCCESS
	[INFO] ------------------------------------------------------------------------
	[INFO] Total time: 02:56 min
	[INFO] Finished at: 2017-08-10T15:25:02-07:00
	[INFO] Final Memory: 168M/1755M
	[INFO] ------------------------------------------------------------------------
	```

1.  Start KSQL by running the compiled JAR file. Use the local argument for the developer preview. This starts the KSQL engine locally.

	```bash
	java -jar ksql-cli/target/ksql-cli-0.1-SNAPSHOT-standalone.jar local
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


# Next steps
[Try the quickstart](#Quickstart-Guide)!