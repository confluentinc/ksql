.. _ksql_generate-custom-test-data:

Generate Custom Test Data by Using the ksql-datagen tool
========================================================

You can use the ``ksql-datagen`` command-line tool to generate test data that
complies with a custom schema that you define.

To generate test data, create an Apache Avro schema and pass it to 
``ksql-datagen``. This generates random data according to the schema you
provide.

**Prerequisites:** 

- :ref:`Confluent Platform <installation>` is installed and running.
  This installation includes a Kafka broker, KSQL, |c3-short|, |zk|,
  Schema Registry, REST Proxy, and Kafka Connect.
- If you installed |cp| via TAR or ZIP, navigate to the installation
  directory. The paths and commands used throughout this tutorial assume
  that you're in this installation directory.
- Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on your
  local machine.

Start a Kafka cluster, including |zk|, a Kafka broker, and |sr|. KSQL queries
messages from this Kafka cluster. KSQL is installed in the |cp| by default.

Define a custom schema
----------------------

In this example, you register a custom Avro schema to the |sr|. The schema is
named `impressions.avro 
<https://github.com/apurvam/streams-prototyping/blob/master/src/main/resources/impressions.avro>`_, 
and it represents ads delivered to users.

#. Download ``impressions.avro`` and copy it to a convenient directory. It's used
   by ``ksql-datagen`` when you start generating test data.

   .. code:: bash

      curl https://raw.githubusercontent.com/apurvam/streams-prototyping/master/src/main/resources/impressions.avro > impressions.avro

#. Use the ``curl`` command to post the ``impressions-key`` schema to the |sr|.

   .. code:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data '{"schema": "{\"namespace\":\"streams\",\"name\":\"impressions\",\"type\":\"string\"}"}' \
      http://localhost:8081/subjects/impressions-key/versions

   The return value is the identifier for the schema in the |sr|.
   Your output should resemble:

   .. code:: bash

      {"id":1}

#. Use the ``curl`` command to post the ``impressions-value`` schema to the |sr|.

   .. code:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data '{"schema": "{\"namespace\":\"streams\",\"name\":\"impressions\",\"type\":\"record\",\"fields\":[{\"name\":\"impresssiontime\",\"type\":{\"type\":\"long\",\"format_as_time\":\"unix_long\",\"arg.properties\":{\"iteration\":{\"start\":1,\"step\":10}}}},{\"name\":\"impressionid\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"regex\":\"impression_[1-9][0-9][0-9]\"}}},{\"name\":\"userid\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"regex\":\"user_[1-9][0-9]?\"}}},{\"name\":\"adid\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"regex\":\"ad_[1-9][0-9]?\"}}}]}"}' \
      http://localhost:8081/subjects/impressions-value/versions

   Your output should resemble:

   .. code:: bash

      {"id":1}

#. Query the |sr| for the list of registered schemas:

   .. code:: bash

      curl -X GET http://localhost:8081/subjects

   Your output should resemble:

   .. code:: bash

      ["impressions-value","impressions-key"]

For more |sr| commands, see :ref:`_schemaregistry_intro`.

Generate Test Data
------------------

When you have a custom schema registered, you can generate test data that's
made up of random values that satisfy the schema requirements. In the
``impressions`` schema, ad identifiers are two-digit random numbers between
10 and 99, as specified by the regular expression ``ad_[1-9][0-9]``.

Open a new command shell, and in the ``<path-to-confluent>/bin`` directory, start
generating test values by using the ``ksql-datagen`` command. In this example,
the schema file, ``impressions.avro``, is in the same directory as ``ksql-datagen``. 

.. code:: bash

    ./ksql-datagen schema=impressions.avro format=delimited topic=impressions key=impressionid propertiesFile=../etc/ksql/datagen.properties

After a few startup messages, your output should resemble:

.. code:: bash

    impression_796 --> ([ 1528756317023 | 'impression_796' | 'user_41' | 'ad_29' ])
    impression_341 --> ([ 1528756317446 | 'impression_341' | 'user_34' | 'ad_32' ])
    impression_419 --> ([ 1528756317869 | 'impression_419' | 'user_58' | 'ad_74' ])
    impression_399 --> ([ 1528756318146 | 'impression_399' | 'user_32' | 'ad_78' ])

Consume the Test Data Stream
----------------------------

In the KSQL query editor, create the ``impressions`` stream:

.. code:: bash

    CREATE STREAM impressions (viewtime BIGINT, key VARCHAR, userid VARCHAR, adid VARCHAR) WITH (KAFKA_TOPIC='impressions', VALUE_FORMAT='DELIMITED');

.. code:: bash

In the KSQL query editor, create the persistent ``impressions2`` stream:

    CREATE STREAM impressions2 as select * from impressions;


