.. _ksql-rest-api:

KSQL REST API Reference
=======================

REST Endpoint
---------------------

The default REST API endpoint is ``http://localhost:8088/``. 

Change the server configuration that controls the REST API endpoint by setting
the ``listeners`` parameter in the KSQL server config file. For more info, see
:ref:`ksql-listeners`.

Content Types
-------------

The KSQL REST API uses content types for requests and responses to indicate the
serialization format of the data. Currently, the only serialization format
supported is JSON, specified as ``application/json``. Your request should
specify this content type in the ``Accept`` header::

    Accept: application/json

Here's an example request that returns the results from the ``LIST STREAMS``
command:

.. code:: bash

   curl -X "POST" "http://localhost:8088/ksql" \
        -H "Content-Type: application/json; charset=utf-8" \
        -d $'{
     "ksql": "LIST STREAMS;",
     "streamsProperties": {}
   }'

Here's an example request that retrieves streaming data from ``TEST_STREAM``:

.. code:: bash

   curl -X "POST" "http://localhost:8088/query" \
        -H "Content-Type: application/json; charset=utf-8" \
        -d $'{
     "ksql": "SELECT * FROM TEST_STREAM;",
     "streamsProperties": {}
   }'


Errors
------

All API endpoints use a standard error message format for any requests that return an HTTP status indicating an error (any 4xx or 5xx statuses):

.. code:: http

   HTTP/1.1 <Error Status>
   Content-Type: application/json

   {
       "message": <Error Message>
   }

Get the Status of a KSQL Server
-------------------------------

The ``/info`` resource gives you information about the status of a KSQL
server, which can be useful for health checks and troubleshooting. You can
use the ``curl`` command to query the ``/info`` endpoint:

.. code:: bash

   curl -sX GET "http://localhost:8088/info" | jq '.'

Your output should resemble:

.. code:: json

   {
     "KsqlServerInfo": {
       "version": "5.0.0",
       "kafkaClusterId": "j3tOi6E_RtO_TMH3gBmK7A",
       "ksqlServiceId": "default_"
     }
   }

Run a KSQL Statement
--------------------

The KSQL resource runs a sequence of KSQL statements. All statements, except those starting with ``SELECT``, can be run on this endpoint. To run ``SELECT`` statements use the ``/query`` endpoint.

.. http:post:: /ksql

   Run a sequence of KSQL statements.

   :json string ksql: A semicolon-delimited sequence of KSQL statements to run.
   :json map streamsProperties: Property overrides to run the statements with. Refer to the :ref:`Config Reference <ksql-param-reference>` for details on properties that can be set.
   :json string streamsProperties[``property-name``]: The value of the property named by ``property-name``. Both the value and ``property-name`` should be strings.

   The response JSON is an array of result objects. The result object contents depend on the statement that it is returning results for. The following sections detail the contents of the result objects by statement.

   **CREATE, DROP, TERMINATE**

   :>json string currentStatus.statementText: The KSQL statement whose result is being returned.
   :>json string currentStatus.commandId: A string that identifies the requested operation. You can use this ID to poll the result of the operation using the status endpoint.
   :>json string currentStatus.commandStatus.status: One of QUEUED, PARSING, EXECUTING, TERMINATED, SUCCESS, or ERROR.
   :>json string currrentStatus.commandStatus.message: Detailed message regarding the status of the execution statement.

   **LIST STREAMS, SHOW STREAMS**

   :>json string streams.statementText: The KSQL statement whose result is being returned.
   :>json array  streams.streams: List of streams.
   :>json string streams.streams[i].name: The name of the stream.
   :>json string streams.streams[i].topic: The topic backing the stream.
   :>json string streams.streams[i].format: The serialization format of the data in the stream. One of JSON, AVRO, or DELIMITED.

   **LIST TABLES, SHOW TABLES**

   :>json string tables.statementText: The KSQL statement whose result is being returned.
   :>json array  tables.tables: List of tables.
   :>json string tables.tables[i].name: The name of the table.
   :>json string tables.tables[i].topic: The topic backing the table.
   :>json string tables.tables[i].format: The serialization format of the data in the table. One of JSON, AVRO, or DELIMITED.

   **LIST QUERIES, SHOW QUERIES**

   :>json string queries.statementText: The KSQL statement whose result is being returned.
   :>json array  queries.queries: List of queries.
   :>json string queries.queries[i].queryString: The text of the statement that started the query.
   :>json string queries.queries[i].kafkaTopic: The name of the topic that the query is writing into.
   :>json string queries.queries[i].id.id: The query ID.

   **LIST PROPERTIES, SHOW PROPERTIES**

   :>json string properties.statementText: The KSQL statement whose result is being returned.
   :>json map    properties.properties: The KSQL server query properties.
   :>json string properties.roperties[``property-name``]: The value of the property named by ``property-name``.

   **DESCRIBE**

   :>json string description.statementText: The KSQL statement whose result is being returned.
   :>json string description.name: The name of the stream or table.
   :>json array  description.readQueries: The id and statement text of the queries reading from the stream or table
   :>json array  description.writeQueries: The id and statement text of the queries writing into the stream or table
   :>json array  description.schema: The schema of the stream or table as a list of column names and types.
   :>json string description.schema[i].name: The name of the column.
   :>json string description.schema[i].type: The data type of the column.
   :>json string description.type: STREAM or TABLE
   :>json string description.key: The name of the key column.
   :>json string description.timestamp: The name of the timestamp column.
   :>json string description.serdes: The serialization format of the data in the stream or table. One of JSON, AVRO, or DELIMITED.
   :>json string description.kafkaTopic: The topic backing the stream or table.
   :>json boolean description.extended: A boolean that indicates whether this is an extended description.
   :>json string description.statistics: A string containing statistics about production/consumption to/from the backing topic (extended only).
   :>json string description.errorStats: A string containing statistics about errors producing/consuming to/from the backing topic (extended only).
   :>json int description.replication: The replication factor of the backing topic (extended only).
   :>json int description.partitions: The number of partitions in the backing topic (extended only).

   **EXPLAIN**

   :>json string description.statementText: The KSQL statement for which the query being explained is running.
   :>json string description.name: The KSQL statement for which the query being explained is running.
   :>json string description.type: QUERY
   :>json string description.serdes: The serialization format of the data in the query's output topic. One of JSON, AVRO, or DELIMITED.
   :>json string description.kafkaTopic: The topic the query is writing into.
   :>json string description.statistics: A string containing statistics about production/consumption to/from the topic the query is writing to.
   :>json string description.errorStats: A string containing statistics about errors producing/consuming to/from the topic the query is writing to.
   :>json int description.replication: The replication factor of the topic the query is writing to.
   :>json int description.partitions: The number of partitions in the topis the query is writing to.

   **Errors**

   If KSQL fails to execute a statement, it returns a response with a successful status code (200) and writes the error in a result object with the following contents:

   :>json string error.statementText: The statement for which the error is being reported.
   :>json string error.errorMessage.message: Details about the error that was encountered.

   **Example request**

   .. code:: http

      POST /ksql HTTP/1.1
      Accept: application/json
      Content-Type: application/json

      {
        "ksql": "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home'; CREATE STREAM pageviews_alice AS SELECT * FROM pageviews_original WHERE userid='alice'",
        "streamsProperties": {
          "ksql.streams.auto.offset.reset": "earliest"
        }
      }

   **Example response**

   .. code:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      [
        {
          "currentStatus": {
            "statementText":"CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home';",
            "commandId":"stream/PAGEVIEWS_HOME/create",
            "commandStatus": {
              "status":"SUCCESS",
              "message":"Stream created and running"
            }
          }
        },
        {
          "currentStatus": {
            "statementText":"CREATE STREAM pageviews_alice AS SELECT * FROM pageviews_original WHERE userid='alice';",
            "commandId":"stream/PAGEVIEWS_ALICE/create",
            "commandStatus": {
              "status":"SUCCESS",
              "message":"Stream created and running"
            }
          }
        }
      ]

Run A Query And Stream Back The Output
--------------------------------------

The query resource lets you stream the output records of a ``SELECT`` statement via a chunked transfer encoding. The response is streamed back until the ``LIMIT`` specified in the statement is reached, or the client closes the connection. If no ``LIMIT`` is specified in the statement, then the response is streamed until the client closes the connection.

.. http:post:: /query

   Run a ``SELECT`` statement and stream back the results.

   :json string ksql: The SELECT statement to run.
   :json map streamsProperties: Property overrides to run the statements with. Refer to the :ref:`Config Reference <ksql-param-reference>` for details on properties that can be set.
   :json string streamsProperties[``property-name``]: The value of the property named by ``property-name``. Both the value and ``property-name`` should be strings.

   Each response chunk is a JSON object with the following format:

   :>json object row: A single row being returned. This will be null if an error is being returned.
   :>json array  row.columns: The values contained in the row.
   :>json ?      row.columns[i]: The value contained in a single column for the row. The value type depends on the type of the column.
   :>json string errorMessage: If this field is non-null, an error has been encountered while running the statement. No additional rows are returned and the server will end the response. Note that when the limit is reached for a query that specified a limit in the LIMIT clause, the server returns a row with error message "LIMIT reached for the partition."

   **Example request**

   .. code:: http

      POST /query HTTP/1.1
      Accept: application/json
      Content-Type: application/json

      {
        "ksql": "SELECT * FROM pageviews;"
        "streamsProperties": {
          "ksql.streams.auto.offset.reset": "earliest"
        }
      }

   **Example response**

   .. code:: http

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked

      ...
      {"row":{"columns":[1524760769983,"1",1524760769747,"alice","home"]},"errorMessage":null}
      ...

Get the Status of a CREATE, DROP, or TERMINATE
----------------------------------------------

CREATE, DROP, and TERMINATE statements return an object that indicates the current state of statement execution. A statement can be in one of the following states:

- QUEUED, PARSING, EXECUTING: The statement was accepted by the server and is being processed.
- SUCCESS: The statement was successfully processed.
- ERROR: There was an error processing the statement. The statement was not executed.
- TERMINATED: The query started by the statement was terminated. Only returned for ``CREATE STREAM|TABLE AS SELECT``.

If a CREATE, DROP, or TERMINATE statement returns a command status with state QUEUED, PARSING, or EXECUTING from the ``/ksql`` endpoint, you can use the ``/status`` endpoint to poll the status of the command.

.. http:get:: /status/(string:commandId)

   Get the current command status for a CREATE, DROP, or TERMINATE statement.

   :param string commandId: The command ID of the statement. This ID is returned by the /ksql endpoint.

   :>json string status: One of QUEUED, PARSING, EXECUTING, TERMINATED, SUCCESS, or ERROR.
   :>json string message: Detailed message regarding the status of the execution statement.

   **Example request**

   .. code:: http

      GET /status/stream/PAGEVIEWS/create HTTP/1.1
      Accept: application/json
      Content-Type: application/json

   **Example response**

   .. code:: http

      HTTP/1.1 200 OK
      Content-Type application/json

      {
        "status": "SUCCESS",
        "message":"Stream created and running"
      }
