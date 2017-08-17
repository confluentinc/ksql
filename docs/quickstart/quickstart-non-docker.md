Non-Docker Setup for KSQL
=========================

**Table of Contents**

> local
> :   

This part of the quick start will guide you through the steps to setup a
Kafka cluster and start KSQL for non-Docker environments. Once you
complete these steps, you can start using KSQL to query the Kafka
cluster.

Start a Kafka cluster
---------------------

Do not run KSQL against a production cluster, since KSQL is in developer
preview.

You will need to download and install a Kafka cluster on your local
machine. This cluster consists of a single Kafka broker along with a
single-node ZooKeeper ensemble and an optional single Schema Registry
instance.

1.  Install Oracle Java JRE or JDK \>= 1.7 on your local machine
2.  Download and install Confluent Platform 3.3.0, which includes a
    Kafka broker, ZooKeeper, Schema Registry, REST Proxy, and Kafka
    Connect.
    [Install](http://docs.confluent.io/current/installation.html)
    Confluent Platform directly onto a Linux server.
3.  If you installed Confluent Platform via tar or zip, change into the
    installation directory. The paths and commands used throughout this
    quick start assume that your are in this installation directory:

4.  Start the Confluent Platform using the new Confluent CLI (part of
    the free Confluent Open Source distribution). ZooKeeper is listening
    on `localhost:2181`, Kafka broker is listening on `localhost:9092`,
    and Confluent Schema Registry is listening on `localhost:8081`.

Start KSQL
----------

1.  Clone the Confluent KSQL repository:

2.  Change into the KSQL directory:

3.  Compile the code:

4.  Start KSQL. Use the `local` argument for the developer preview KSQL
    release because it starts the KSQL engine locally.

5.  Refer to the steps below to generate data to the Kafka cluster.

Produce topic data
------------------

The main KSQL quick start page \<quickstart.rst\> assumes you have run at
least the following three steps to produce data to two Kafka topics
`pageviews` and `users` in your Kafka cluster. So if you're not using
Docker, when automatically generates this data, you have to do these
steps manually

​1. Assuming you have already completed the steps above to compile the
KSQL code, verify that you have a compiled `jar` file
`ksql-examples/target/ksql-examples-0.1-SNAPSHOT-standalone.jar` for
data generation.

2.  Produce Kafka data to a topic `pageviews` using the provided data
    generator. The following example continuously generates data with a
    value in DELIMITED format

3.  Produce Kafka data to a topic `users` using the provided data
    generator. The following example continuously generates data with a
    value in Json format

    \$ java -jar
    ksql-examples/target/ksql-examples-0.1-SNAPSHOT-standalone.jar
    quickstart=users format=json topic=users maxInterval=10000

At this point you may return to the [main KSQL quick start
page](quickstart.rst#create-a-stream-and-table) to start querying the
Kafka cluster. If you would like to do additional testing with topic
data produced from the commandline tools:

4.  You can produce Kafka data with the Kafka commandline
    `kafka-console-producer`. The following example generates data with
    a value in DELIMITED format

5.  The following example generates data with a value in Json format

