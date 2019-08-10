.. _query-with-structured-data:

Query With Structured Data
##########################

KSQL enables querying structured, or nested, data, by using the STRUCT data type.
You use familiar syntax to declare and access structured data, like
``mystruct STRUCT<fieldName1 type1, fieldName2 type2>`` and
``mystruct->fieldName1``. 

The following example shows how to create a KSQL stream from an |ak-tm| topic that
has structured data. Also, it shows how to run queries to access the structured
data.

#. Set up the KSQL environment.
#. Use the |kcat| utility to create and populate a new topic, named ``raw-topic``. 
#. Create a stream on the topic that models the topic's data.
#. Inspect the stream to ensure that the data model matches the topic.
#. Query the stream to access the structured data.

Set up the KSQL Environment
***************************

To set up KSQL, follow the first three steps in :ref:`ksql_quickstart-docker`,
or if you have git and Docker installed already, run the following commands: 

.. codewithvars:: bash

    # Step 1
    git clone https://github.com/confluentinc/ksql.git
    cd ksql

    # Step 2
    git checkout |release|-post

    # Step 3
    cd docs/tutorials/
    docker-compose up -d

After all of the Docker images are pulled, confirm that the KSQL and Kafka
containers are running:

.. code:: bash

    docker-compose ps

Your output should resemble:

::

              Name                        Command            State                 Ports
    ----------------------------------------------------------------------------------------------------
    tutorials_kafka_1             /etc/confluent/docker/run   Up      0.0.0.0:39092->39092/tcp, 9092/tcp
    tutorials_ksql-server_1       /etc/confluent/docker/run   Up      8088/tcp
    tutorials_schema-registry_1   /etc/confluent/docker/run   Up      8081/tcp
    tutorials_zookeeper_1         /etc/confluent/docker/run   Up      2181/tcp, 2888/tcp, 3888/tcp

The KSQL environment is ready for you to develop real-time streaming
applications.

Create and Populate a New Topic With Structured Data
****************************************************

Use the |kcat| utility to create and populate a new topic, named
``raw-topic``, with some records that have nested data. The records are 
formatted as JSON arrays. For more information, see :ref:`kafkacat-usage`.

.. code:: bash

    docker run --interactive --rm --network tutorials_default \
      confluentinc/cp-kafkacat \
      kafkacat -b kafka:39092 \
              -t raw-topic \
              -K: \
              -P <<EOF
    1:{"type":"key1","data":{"timestamp":"2018-12-21 23:58:42.1","field-a":1,"field-b":"first-value-for-key1"}}
    2:{"type":"key2","data":{"timestamp":"2018-12-21 23:58:42.2","field-a":1,"field-c":11,"field-d":"first-value-for-key2"}}
    3:{"type":"key1","data":{"timestamp":"2018-12-21 23:58:42.3","field-a":2,"field-b":"updated-value-for-key1"}}
    4:{"type":"key2","data":{"timestamp":"2018-12-21 23:58:42.4","field-a":3,"field-c":22,"field-d":"updated-value-for-key2"}}
    EOF 

The nested structure is named ``data`` and has five fields:

* ``timestamp``, a string
* ``field-a``, an integer
* ``field-b``, a string
* ``field-c``, an integer
* ``field-d``, a string 

In the following KSQL queries, the ``data`` structure is modeled by using the
STRUCT type:

::

    DATA STRUCT<timestamp VARCHAR, "field-a" INT, "field-b" VARCHAR, "field-c" INT, "field-d" VARCHAR>

Double-quotes are necessary for the fieldnames that contain the ``-`` character.

.. note::
    
    ``Properties`` is not a valid field name.

Create a Stream With Structured Data
************************************

Start the KSQL CLI:

.. codewithvars:: bash

  docker run --network tutorials_default --rm --interactive --tty \
      confluentinc/cp-ksql-cli:|release| \
      http://ksql-server:8088

In the KSQL CLI, ensure that ``raw-topic`` is available: 

::

    SHOW TOPICS;

Your output should resemble:

::

     Kafka Topic        | Partitions | Partition Replicas
    ------------------------------------------------------
     _confluent-metrics | 12         | 1
     _schemas           | 1          | 1
     raw-topic          | 1          | 1
    ------------------------------------------------------

Inspect ``raw-topic`` to ensure that |kcat| populated it: 

::

    PRINT 'raw-topic' FROM BEGINNING;

Your output should resemble:

::

    Format:JSON
    {"ROWTIME":1544042630406,"ROWKEY":"1","type":"key1","data":{"timestamp":"2018-12-21 23:58:42.1","field-a":1,"field-b":"first-value-for-key1"}}
    {"ROWTIME":1544042630406,"ROWKEY":"2","type":"key2","data":{"timestamp":"2018-12-21 23:58:42.2","field-a":1,"field-c":11,"field-d":"first-value-for-key2"}}
    {"ROWTIME":1544042630406,"ROWKEY":"3","type":"key1","data":{"timestamp":"2018-12-21 23:58:42.3","field-a":2,"field-b":"updated-value-for-key1"}}
    {"ROWTIME":1544042630406,"ROWKEY":"4","type":"key2","data":{"timestamp":"2018-12-21 23:58:42.4","field-a":3,"field-c":22,"field-d":"updated-value-for-key2"}}
    ^CTopic printing ceased

Press Ctrl+C to stop printing the topic.

Run the following CREATE STREAM statement to register the topic with KSQL:

.. code:: sql

    CREATE STREAM T (TYPE VARCHAR,
                    DATA STRUCT<
                          timestamp VARCHAR,
                          "field-a" INT,
                          "field-b" VARCHAR,
                          "field-c" INT,
                          "field-d" VARCHAR>)
            WITH (KAFKA_TOPIC='raw-topic',
                  VALUE_FORMAT='JSON');

Your output should resemble:

::

     Message
    ----------------
     Stream created
    ----------------

Run KSQL Queries to Access the Structured Data
**********************************************

Run the following command to tell KSQL to read from the beginning of the topic:

::

    SET 'auto.offset.reset' = 'earliest';

Run a SELECT query to inspect the ``T`` stream:

.. code:: sql

    SELECT * FROM T;

Your output should resemble:

::

    1544042630406 | 1 | key1 | {TIMESTAMP=2018-12-21 23:58:42.1, field-a=1, field-b=first-value-for-key1, field-c=null, field-d=null}
    1544042630406 | 2 | key2 | {TIMESTAMP=2018-12-21 23:58:42.2, field-a=1, field-b=null, field-c=11, field-d=first-value-for-key2}
    1544042630406 | 3 | key1 | {TIMESTAMP=2018-12-21 23:58:42.3, field-a=2, field-b=updated-value-for-key1, field-c=null, field-d=null}
    1544042630406 | 4 | key2 | {TIMESTAMP=2018-12-21 23:58:42.4, field-a=3, field-b=null, field-c=22, field-d=updated-value-for-key2}
    ^CQuery terminated

Press Ctrl+C to cancel the SELECT query.

.. note::

    KSQL assigns ``null`` to the fields that were omitted when |kcat|
    populated ``raw-topic``, like ``field-c`` and ``field-d`` in record
    ``key1``.

Query ``field-a`` and ``field-b`` by using the ``->`` operator to access the
nested elements:

.. code:: sql

    SELECT DATA->"field-a", DATA->"field-b" FROM T WHERE TYPE='key1' LIMIT 2;

Your output should resemble:

::

    1 | first-value-for-key1
    2 | updated-value-for-key1
    Limit Reached
    Query terminated

Query the other nested elements:

.. code:: sql

    SELECT DATA->"field-a", DATA->"field-c", DATA->"field-d" FROM T WHERE TYPE='key2' LIMIT 2;

Your output should resemble:

::

    1 | 11 | first-value-for-key2
    3 | 22 | updated-value-for-key2
    Limit Reached
    Query terminated

Create persistent queries based on the previous SELECT statements. In this 
example, two different queries are used to separate the input data into two
new streams. 

.. code:: sql

    CREATE STREAM TYPE_1 AS SELECT DATA->"field-a", DATA->"field-b" FROM T WHERE TYPE='key1';

.. code:: sql

    CREATE STREAM TYPE_2 AS SELECT DATA->"field-a", DATA->"field-c",DATA->"field-d" FROM T2 WHERE TYPE='key2';

For both statements, your output should resemble:

::

     Message
    ----------------------------
     Stream created and running
    ----------------------------

Inspect the schema of the ``TYPE_1`` stream:

::

    DESCRIBE TYPE_1;

Your output should resemble:

::

    Name                 : TYPE_1
     Field         | Type
    -------------------------------------------
     ROWTIME       | BIGINT           (system)
     ROWKEY        | VARCHAR(STRING)  (system)
     DATA__field-a | INTEGER
     DATA__field-b | VARCHAR(STRING)
    -------------------------------------------
    For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;

Inspect the schema of the ``TYPE_2`` stream:

::

    DESCRIBE TYPE_2;

Your output should resemble:

::

    Name                 : TYPE_2
     Field         | Type
    -------------------------------------------
     ROWTIME       | BIGINT           (system)
     ROWKEY        | VARCHAR(STRING)  (system)
     DATA__field-a | INTEGER
     DATA__field-c | INTEGER
     DATA__field-d | VARCHAR(STRING)
    -------------------------------------------
    For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;

Next Steps
**********

* :ref:`struct_overview`
* :ref:`ksql_quickstart-docker`
* :ref:`query-with-arrays-and-maps`

