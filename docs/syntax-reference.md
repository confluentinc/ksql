


CLI commands
++++++++++++
Commands are non-SQL statements such as setting a property or adding a resource. 

clear
  Clears the current CLI contents
exit | CTRL-D
  Exit the CLI process.
help
  Display inline help for all of the CLI commands.
history
  Show a history of your commands entered during the current CLI session.
output [JSON|TABULAR]
  Sets the display format for interactive query results. Entering just `output` without any options will display the currently-set value.
set 'foo' = 'bar'
  Set the value of a session variable, which will be passed through to the Kafka Streams process(es) executing any subsequent queries during this session. Can be used to tune various producer and consumer settings. One option very useful during iterative development of new queries on limited data may be ``SET 'auto.offset.reset' = 'earliest'`` which will force subsequent queries to read their input topics from the earliest available offsets (as opposed to from the tail, which is the default). Entering just `SET` without any arguments will display the list of all currently-set session variables.
show properties 
  TODO this one is a bit weird - needs a semi-colon, and is kinda dml but kinda not - where best to put it?
unset 'foo'
  Remove the value of any session variable previously set via the `SET` command.
version
  Display the software version specifics.
server [http://my.server.name:8090] (Remote Cli Only)
  Without arguments will display the address of the KSQL server to which this CLI is currently connected. With server specification it drops any current server connection and replaces it with one to the specified server address.

DDL statements
++++++++++++++
All KSQL statements should be terminated with a semi-colon. If desired, use a back-slash ('\\') to indicate continuation on the next line.

DESCRIBE `stream-or-table`

SHOW | LIST TOPICS

SHOW | LIST STREAMS

SHOW | LIST TABLES

SHOW QUERIES

TERMINATE `query-id`

CREATE STREAM stream_name (  { column_name data_type} [, ...] ) WITH ( property_name = expression [, ...] );

    The supported column data types currently are BOOELAN(BOOL), INTEGER(INT), BIGINT(LONG), DOUBLE, VARCHAR (STRING), ARRAY<ArrayType> (Only in JSON) and MAP<VARCHAR, ValueType> (Only in JSON).

    In addition to the defined columns in the statement, KSQL adds two implicit columns to every stream, ROWKEY and ROWTIME, which represent the corresponding Kafka message key and message timestamp.

  The possible properties to set in the WITH clause:
* KAFKA_TOPIC: The name of the kafka topic that this streams is built upon. The topic should already exist in kafka. This is a required property.
* VALUE_FORMAT: Specifies the format in which the value in the topic that data is serialized in. Currently, KSQL supports json, delimited. This is a required property.
* KEY: The name of the column that is the key.
* TIMESTAMP: The name of the column that will be used as the timestamp. This can be used to define the event time.

Example
::
  ksql> CREATE STREAM pageview (viewtime bigint, userid varchar, pageid varchar) WITH (value_format = 'json', kafka_topic='pageview_topic_json');


CREATE TABLE table_name (  { column_name data_type} [, ...] ) WITH ( property_name = expression [, ...] );
  Define a new KSQL table with the specified columns and properties.
  The supported column data types currently are BOOELAN(BOOL), INTEGER(INT), BIGINT(LONG), DOUBLE, VARCHAR (STRING), ARRAY<ArrayType> (Only in JSON) and MAP<VARCHAR, ValueType> (Only in JSON).
  
  In addition to the defined columns in the statement, KSQL adds two implicit columns to every table, ROWKEY and ROWTIME, which represent the corresponding Kafka message key and message timestamp.

  The possible properties to set in the WITH clause:
* KAFKA_TOPIC: The name of the kafka topic that this streams is built upon. The topic should already exist in kafka. This is a required property.
* VALUE_FORMAT: Specifies the format in which the value in the topic that data is serialized in. Currently, KSQL supports json, delimited. This is a required property.
* TIMESTAMP: The name of the column that will be used as the timestamp.

Example
::

  ksql> CREATE TABLE users (usertimestamp bigint, userid varchar, gender varchar, regionid varchar) WITH (value_format = 'json', kafka_topic='user_topic_json'); 


DML statements
++++++++++++++
SELECT
  Selects rows from a KSQL stream or table. The result of this statement will be printed out in the console. To stop the continuous query in the CLI press Ctrl+C.
::

  SELECT `select_expr` [, ...] 
  FROM `from_item` [, ...]
  [ WINDOW `window_expression` ]
  [ WHERE `condition` ]
  [ GROUP BY `grouping expression` ]
  [ HAVING `having_expression` ]

where `from_item` is one of the following:

``table_name [ [ AS ] alias]``

``from_item LEFT JOIN from_item ON join_condition``

The WINDOW clause is used to define a window for aggregate queries. Currently KSQL supports the following WINDOW types:
* TUMBLING 
  The TUMBLING window needs a size parameter.

Example
::

  ksql> SELECT * FROM orders WHERE orderunits > 5 ;

* HOPPING
  The HOPPING window is a fixed sized, (possibly) overlapping window. User need to set two values for a HOPPING window, size and advance interval. The following is an example query with hopping window.

Example
::

  ksql> SELECT ITEMID, SUM(arraycol[0]) FROM ORDERS window HOPPING ( size 20 second, advance by 5 second) GROUP BY ITEMID;

* SESSION
  SESSION windows are used to aggregate key-based events into so-called sessions. The SESSION window needs the session inactivity gap size. 

Example
::

  ksql> SELECT ITEMID, SUM(arraycol[0]) FROM ORDERS window SESSION (20 second) GROUP BY ITEMID;

CREATE STREAM AS SELECT
  Create a new KSQL stream along with the corresponding kafka topic and stream the result of the SELECT query into the topic.  
::

  CREATE STREAM `stream_name`
  [WITH ( `property_name = expression` [, ...] )] 
  AS SELECT  `select_expr` [, ...] 
  FROM `from_item` [, ...] 
  [ WHERE `condition` ] 
  [PARTITION BY `column_name`]
 
The WITH section can be used to set the properties for the result KSQL topic. The properties that can be set are as follows:
* KAFKA_TOPIC: The name of KSQL topic and the corresponding kafka topic associated with the new KSQL stream. If not set the name of the stream will be used as default.

* FORMAT: Specifies the format in which the result topic data is serialized in. Currently, KSQL supports json, avro and csv. If not set the same format of the input stream will be used.

* AVROSCHEMAFILE: The path to write the avro schema file for the result. If the output format is avro, avroschemafile will be set. If not set the generated schema file will be written to "/tmp/" folder with the name of the stream as the file name.

* PARTITIONS: The number of partitions in the sink stream.

* REPLICATIONS: The replication factor for the sink stream.

* TIMESTAMP: The name of the column that will be used as the timestamp. This can be used to define the event time.

CREATE TABLE AS SELECT
  Create a new KSQL table along with the corresponding KSQL topic and kafka topic and stream the result of the SELECT query into the topic.  
::

  CREATE TABLE `stream_name` 
  [WITH ( `property_name = expression` [, ...] )] 
  AS SELECT  `select_expr` [, ...] 
  FROM `from_item` [, ...] 
  [ WHERE `condition` ]
  [ GROUP BY `grouping expression` ] 
  [ HAVING `having_expression` ]

The WITH section can be used to set the properties for the result KSQL topic. The properties that can be set are as the following:

* KAFKA_TOPIC: The name of KSQL topic and the corresponding kafka topic associated with the new KSQL stream. If not set the name of the stream will be used as default.

* FORMAT: Specifies the format in which the result topic data is serialized in. Currently, KSQL supports json, avro and csv. If not set the same format of the input stream will be used.

* AVROSCHEMAFILE: The path to write the avro schema file for the result. If the output format is avro, avroschemafile will be set. If not set the generated schema file will be written to "/tmp/" folder with the name of the stream as the file name.

* PARTITIONS: The number of partitions in the sink stream.

* REPLICATIONS: The replication factor for the sink stream.


Scalar Functions
++++++++++++++++
KSQL provides a set of internal functions that can use used in query expressions. The following is the list of the currently available functions:

=========  ======================  =======================================================
Function   Example                 Description            
=========  ======================  =======================================================
LCASE      LCASE(col1)             Convert a string to lower case  
UCASE      UCASE(col1)             Convert a string to upper case  
SUBSTRING  SUBSTRING(col1, 2, 5)   Return the substring with the start and end indices	
CONCAT     CONCAT(col1, '_hello')  Concatenate two strings	
TRIM       TRIM(col1)              Trim the spaces from the beginning and end of a string
LEN	       LEN(col1)               The length of a string
ABS        ABS(col1)               The absolute value of a value
CEIL       CEIL(col1)              The ceiling of a value
FLOOR      FLOOR(col1)             The floor of a value
ROUND      ROUND(col1)             Round a value to the nearest integral value
RANDOM     RANDOM()                Return a random value between 0 and 1.0	
=========  ======================  =======================================================

Aggregate Functions
++++++++++++++++
KSQL provides a set of internal aggregate functions that can use used in query expressions. The following is the list of the currently available aggregate functions:

=========  ======================  =======================================================
Function   Example                 Description            
=========  ======================  =======================================================
COUNT      COUNT(col1)             Count the number of rows
SUM        SUM(col1)               Sums the column values
MIN        MIN(col1)               Return the min value for a given column and window
MAX        MAX(col1)               Return the max value for a given column and window
=========  ======================  =======================================================
