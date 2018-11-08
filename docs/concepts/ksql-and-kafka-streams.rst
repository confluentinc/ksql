.. _ksql-and-kafka-streams:

KSQL and Kafka Streams
######################


The streaming SQL engine for Apache Kafka® to write real-time applications in
a SQL-like query language.

Apache Kafka® library to write real-time applications and microservices in Java and Scala

And remember: They can also be used together!


New to streaming and Kafka
To quicken and broaden the adoption & value of Kafka in your organization
Prefer an interactive experience with UI and CLI
Prefer SQL to writing code in Java or Scala
Use cases include enriching data; joining data sources; filtering, transforming, and masking data;  identifying anomalous events
Use case is naturally expressible through SQL, with optional help from User Defined Functions as “get out jail free” card

Want the power of Kafka Streams but you are not on the JVM: use the KSQL REST API from Python, Go, C#, JavaScript, shell

KSQL is usually not yet a good fit for:
BI reports & ad-hoc querying, queries with random access patterns (because no indexes, no native JDBC)



Prefer writing and deploying JVM applications like Java and Scala; e.g. due to people skills, tech environment
Use case is not naturally expressible through SQL, e.g. finite state machines
Building microservices
Must integrate with external services, or use 3rd-party libraries (but KSQL UDFs may help)
To customize or fine-tune a use case, e.g. with Kafka Streams’ Processor API; examples: custom join variants, probabilistic counting at very large scale with Count-Min Sketch
Need for queryable state, which is not yet supported by KSQL



streams
You write...
KSQL statements
JVM applications
UI included for human interaction
Yes, in Confluent Enterprise
No
CLI included for human interaction
Yes
No
Data formats
Avro, JSON, CSV (today)
Any data format, including
Avro, JSON, CSV, Protobuf, XML
REST API included
Yes
No, but you can DIY
Runtime included
Yes, the KSQL server
Not needed, applications run as standard JVM processes
Queryable state
Not yet
Yes


https://docs.google.com/presentation/d/1W31dIAR9eyDN6_ab0VZCtYGQMJy8Sa95egNC41Ta0CQ/edit#slide=id.g3f888324d2_0_67

