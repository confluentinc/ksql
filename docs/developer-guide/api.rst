.. _ksql-rest-api:

KSQL REST API Reference
=======================

REST Endpoint
---------------------

The default REST API endpoint is ``http://0.0.0.0:8088/``.

Change the server configuration that controls the REST API endpoint by setting
the ``listeners`` parameter in the KSQL server config file. For more info, see
:ref:`ksql-listeners`. To configure the endpoint to use HTTPS, see
:ref:`config-ksql-for-https`.

Content Types
-------------

The KSQL REST API uses content types for requests and responses to indicate the serialization format of the data and the API version. Currently, the only serialization format supported is JSON. The only version supported is v1. Your request should specify this serialization format and version in the ``Accept`` header as::

    Accept: application/vnd.ksql.v1+json

The less specific ``application/json`` content type is also permitted. However, this is only for compatibility and ease of use, and you should use the versioned value if possible.

The server also supports content negotiation, so you may include multiple, weighted preferences::

    Accept: application/vnd.ksql.v1+json; q=0.9, application/json; q=0.5

For example, content negotiation is useful when a new version of the API is preferred, but you are not sure if it is available yet.

Here's an example request that returns the results from the ``LIST STREAMS``
command:

.. code:: bash

   curl -X "POST" "http://localhost:8088/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
        -d $'{
     "ksql": "LIST STREAMS;",
     "streamsProperties": {}
   }'

Here's an example request that retrieves streaming data from ``TEST_STREAM``:

.. code:: bash

   curl -X "POST" "http://localhost:8088/query" \
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
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
       "error_code": <Error code>
       "message": <Error Message>
   }

Some endpoints may include additional fields that provide more context for handling the error.

Get the Status of a KSQL Server
-------------------------------

The ``/info`` resource gives you information about the status of a KSQL
server, which can be useful for health checks and troubleshooting. You can
use the ``curl`` command to query the ``/info`` endpoint:

.. code:: bash

   curl -sX GET "http://localhost:8088/info" | jq '.'

Your output should resemble:

.. codewithvars:: bash

   {
     "KsqlServerInfo": {
       "version": "|release|",
       "kafkaClusterId": "j3tOi6E_RtO_TMH3gBmK7A",
       "ksqlServiceId": "default_"
     }
   }

Run a KSQL Statement
--------------------

The ``/ksql`` resource runs a sequence of KSQL statements. All statements, except
those starting with SELECT, can be run on this endpoint. To run SELECT
statements use the ``/query`` endpoint.

.. note::

   If you use the SET or UNSET statements to assign query properties by using
   the REST API, the assignment is scoped only to the current request. In
   contrast, SET and UNSET assignments in the KSQL CLI persist throughout the
   CLI session.

.. http:post:: /ksql

   Run a sequence of KSQL statements.

   :json string ksql: A semicolon-delimited sequence of KSQL statements to run.
   :json map streamsProperties: Property overrides to run the statements with. Refer to the :ref:`Config Reference <ksql-param-reference>` for details on properties that can be set.
   :json string streamsProperties[``property-name``]: The value of the property named by ``property-name``. Both the value and ``property-name`` should be strings.
   :json long commandSequenceNumber: Optional. If specified, the statements will not be run until all existing commands up to and including the specified sequence number have completed. If unspecified, the statements are run immediately. (Note: When a command is processed, the result object contains its sequence number.)

   The response JSON is an array of result objects. The result object contents depend on the statement that it is returning results for. The following sections detail the contents of the result objects by statement.

   **Common Fields**
   The following fields are common to all responses.

   :>json string statementText: The KSQL statement whose result is being returned.
   :>json array  warnings: A list of warnings about conditions that may be unexpected by the user, b
ut don't result in failure to execute the statement.
   :>json string warnings[i].message: A message detailing the condition being warned on.

   **CREATE, DROP, TERMINATE**

   :>json string commandId: A string that identifies the requested operation. You can use this ID to poll the result of the operation using the status endpoint.
   :>json string commandStatus.status: One of QUEUED, PARSING, EXECUTING, TERMINATED, SUCCESS, or ERROR.
   :>json string commandStatus.message: Detailed message regarding the status of the execution statement.
   :>json long commandSequenceNumber: The sequence number of the requested operation in the command queue, or -1 if the operation was unsuccessful.

   **LIST STREAMS, SHOW STREAMS**

   :>json array  streams: List of streams.
   :>json string streams[i].name: The name of the stream.
   :>json string streams[i].topic: The topic backing the stream.
   :>json string streams[i].format: The serialization format of the data in the stream. One of JSON, AVRO, or DELIMITED.

   **LIST TABLES, SHOW TABLES**

   :>json array  tables: List of tables.
   :>json string tables[i].name: The name of the table.
   :>json string tables[i].topic: The topic backing the table.
   :>json string tables[i].format: The serialization format of the data in the table. One of JSON, AVRO, or DELIMITED.

   **LIST QUERIES, SHOW QUERIES**

   :>json array  queries: List of queries.
   :>json string queries[i].queryString: The text of the statement that started the query.
   :>json string queries[i].sinks: The streams and tables being written to by the query.
   :>json string queries[i].id: The query ID.

   **LIST PROPERTIES, SHOW PROPERTIES**

   :>json map    properties: The KSQL server query properties.
   :>json string properties[``property-name``]: The value of the property named by ``property-name``.

   **DESCRIBE**

   :>json string  sourceDescription.name: The name of the stream or table.
   :>json array   sourceDescription.readQueries: The queries reading from the stream or table.
   :>json array   sourceDescription.writeQueries: The queries writing into the stream or table
   :>json array   sourceDescription.fields: A list of field objects that describes each field in the stream/table.
   :>json string  sourceDescription.fields[i].name: The name of the field.
   :>json object  sourceDescription.fields[i].schema: A schema object that describes the schema of the field.
   :>json string  sourceDescription.fields[i].schema.type: The type the schema represents. One of INTEGER, BIGINT, BOOLEAN, DOUBLE, STRING, MAP, ARRAY, or STRUCT.
   :>json object  sourceDescription.fields[i].schema.memberSchema: A schema object. For MAP and ARRAY types, contains the schema of the map values and array elements, respectively. For other types this field is not used and its value is undefined.
   :>json array   sourceDescription.fields[i].schema.fields: For STRUCT types, contains a list of field objects that descrbies each field within the struct. For other types this field is not used and its value is undefined.
   :>json string  sourceDescription.type: STREAM or TABLE
   :>json string  sourceDescription.key: The name of the key column.
   :>json string  sourceDescription.timestamp: The name of the timestamp column.
   :>json string  sourceDescription.format: The serialization format of the data in the stream or table. One of JSON, AVRO, or DELIMITED.
   :>json string  sourceDescription.topic: The topic backing the stream or table.
   :>json boolean sourceDescription.extended: A boolean that indicates whether this is an extended description.
   :>json string  sourceDescription.statistics: A string that contains statistics about production and consumption to and from the backing topic (extended only).
   :>json string  sourceDescription.errorStats: A string that contains statistics about errors producing and consuming to and from the backing topic (extended only).
   :>json int     sourceDescription.replication: The replication factor of the backing topic (extended only).
   :>json int     sourceDescription.partitions: The number of partitions in the backing topic (extended only).

   **EXPLAIN**

   :>json string queryDescription.statementText: The KSQL statement for which the query being explained is running.
   :>json array  queryDescription.fields: A list of field objects that describes each field in the query output.
   :>json string queryDescription.fields[i].name: The name of the field.
   :>json object queryDescription.fields[i].schema: A schema object that describes the schema of the field.
   :>json string queryDescription.fields[i].schema.type: The type the schema represents. One of INTEGER, BIGINT, BOOLEAN, DOUBLE, STRING, MAP, ARRAY, or STRUCT.
   :>json object queryDescription.fields[i].schema.memberSchema: A schema object. For MAP and ARRAY types, contains the schema of the map values and array elements, respectively. For other types this field is not used and its value is undefined.
   :>json array  queryDescription.fields[i].schema.fields: For STRUCT types, contains a list of field objects that descrbies each field within the struct. For other types this field is not used and its value is undefined.
   :>json array  queryDescription.sources: The streams and tables being read by the query.
   :>json string queryDescription.sources[i]: The name of a stream or table being read from by the query.
   :>json array  queryDescription.sinks: The streams and tables being written to by the query.
   :>json string queryDescription.sinks[i]: The name of a stream or table being written to by the query.
   :>json string queryDescription.executionPlan: They query execution plan.
   :>json string queryDescription.topology: The Kafka Streams topology that the query is running.
   :>json map    overriddenProperties: The property overrides that the query is running with.

   **Errors**

   If KSQL fails to execute a statement, it returns a response with an error status code (4xx/5xx). Even if an error is returned, the server may have been able to successfully execute some statements in the request. In this case, the response includes the ``error_code`` and ``message`` fields, a ``statementText`` field with the text of the failed statement, and an ``entities`` field that contains an array of result objects:

   :>json string statementText: The text of the KSQL statement where the error occurred.
   :>json array  entities: Result objects for statements that were successfully executed by the server.

   The ``/ksql`` endpoint may return the following error codes in the ``error_code`` field:

   - 40001 (BAD_STATEMENT): The request contained an invalid KSQL statement.
   - 40002 (QUERY_ENDPOINT): The request contained a statement that should be issued to the ``/query`` endpoint.

   **Example request**

   .. code:: http

      POST /ksql HTTP/1.1
      Accept: application/vnd.ksql.v1+json
      Content-Type: application/vnd.ksql.v1+json

      {
        "ksql": "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home'; CREATE STREAM pageviews_alice AS SELECT * FROM pageviews_original WHERE userid='alice';",
        "streamsProperties": {
          "ksql.streams.auto.offset.reset": "earliest"
        }
      }

   **Example response**

   .. code:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.ksql.v1+json

      [
        {
          "statementText":"CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home';",
          "commandId":"stream/PAGEVIEWS_HOME/create",
          "commandStatus": {
            "status":"SUCCESS",
            "message":"Stream created and running"
          },
          "commandSequenceNumber":10
        },
        {
          "statementText":"CREATE STREAM pageviews_alice AS SELECT * FROM pageviews_original WHERE userid='alice';",
          "commandId":"stream/PAGEVIEWS_ALICE/create",
          "commandStatus": {
            "status":"SUCCESS",
            "message":"Stream created and running"
          },
          "commandSequenceNumber":11
        }
      ]

.. _coordinate_multiple_requests:

Coordinate Multiple Requests
----------------------------

To submit multiple, interdependent requests, there are two options. The first is to submit them as a single request,
similar to the example request above:

.. code:: http

   POST /ksql HTTP/1.1
   Accept: application/vnd.ksql.v1+json
   Content-Type: application/vnd.ksql.v1+json

   {
     "ksql": "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home'; CREATE TABLE pageviews_home_count AS SELECT userid, COUNT(*) FROM pageviews_home GROUP BY userid;"
   }

The second method is to submit the statements as separate requests and incorporate the interdependency by using ``commandSequenceNumber``.
Send the first request:

.. code:: http

   POST /ksql HTTP/1.1
   Accept: application/vnd.ksql.v1+json
   Content-Type: application/vnd.ksql.v1+json

   {
     "ksql": "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home';"
   }

Make note of the ``commandSequenceNumber`` returned in the response:

.. code:: http

   HTTP/1.1 200 OK
   Content-Type: application/vnd.ksql.v1+json

   [
     {
       "statementText":"CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home';",
       "commandId":"stream/PAGEVIEWS_HOME/create",
       "commandStatus": {
         "status":"SUCCESS",
         "message":"Stream created and running"
       },
       "commandSequenceNumber":10
     }
   ]

Provide this ``commandSequenceNumber`` as part of the second request, indicating that this request should not
execute until after command number 10 has finished executing:

.. code:: http

   POST /ksql HTTP/1.1
   Accept: application/vnd.ksql.v1+json
   Content-Type: application/vnd.ksql.v1+json

   {
     "ksql": "CREATE TABLE pageviews_home_count AS SELECT userid, COUNT(*) FROM pageviews_home GROUP BY userid;",
     "commandSequenceNumber":10
   }

Run A Query And Stream Back The Output
--------------------------------------

The ``/query`` resource lets you stream the output records of a ``SELECT`` statement via a chunked transfer encoding. The response is streamed back until the ``LIMIT`` specified in the statement is reached, or the client closes the connection. If no ``LIMIT`` is specified in the statement, then the response is streamed until the client closes the connection.

.. http:post:: /query

   Run a ``SELECT`` statement and stream back the results.

   :json string ksql: The SELECT statement to run.
   :json map streamsProperties: Property overrides to run the statements with. Refer to the :ref:`Config Reference <ksql-param-reference>` for details on properties that can be set.
   :json string streamsProperties[``property-name``]: The value of the property named by ``property-name``. Both the value and ``property-name`` should be strings.

   Each response chunk is a JSON object with the following format:

   :>json object row: A single row being returned. This will be null if an error is being returned.
   :>json array  row.columns: The values contained in the row.
   :>json ?      row.columns[i]: The value contained in a single column for the row. The value type depends on the type of the column.
   :>json string finalMessage: If this field is non-null, it contains a final message from the server. No additional rows will be returned and the server will end the response.
   :>json string errorMessage: If this field is non-null, an error has been encountered while running the statement. No additional rows are returned and the server will end the response.


   **Example request**

   .. code:: http

      POST /query HTTP/1.1
      Accept: application/vnd.ksql.v1+json
      Content-Type: application/vnd.ksql.v1+json

      {
        "ksql": "SELECT * FROM pageviews;",
        "streamsProperties": {
          "ksql.streams.auto.offset.reset": "earliest"
        }
      }

   **Example response**

   .. code:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.ksql.v1+json
      Transfer-Encoding: chunked

      ...
      {"row":{"columns":[1524760769983,"1",1524760769747,"alice","home"]},"errorMessage":null}
      ...

Get the Status of a CREATE, DROP, or TERMINATE
----------------------------------------------

CREATE, DROP, and TERMINATE statements returns an object that indicates the current state of statement execution. A statement can be in one of the following states:

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
      Accept: application/vnd.ksql.v1+json
      Content-Type: application/vnd.ksql.v1+json

   **Example response**

   .. code:: http

      HTTP/1.1 200 OK
      Content-Type application/vnd.ksql.v1+json

      {
        "status": "SUCCESS",
        "message":"Stream created and running"
      }


.. _ksql_cluster_terminate:

Terminate a Cluster
-------------------

If you don't need your KSQL cluster anymore, you can terminate the cluster and clean up the resources using this endpoint. To terminate a KSQL cluster, first shutdown all the servers except one.
Then, send the ``TERMINATE CLUSTER`` request to the ``/ksql/terminate`` endpoint in the last remaining server.
When the server receives a ``TERMINATE CLUSTER`` request at ``/ksql/terminate`` endpoint, it writes a ``TERMINATE CLUSTER`` command into the command topic.
Note that ``TERMINATE CLUSTER`` request can only be sent via the ``/ksql/terminate`` endpoint and you cannot send it via the CLI.
When the server reads the ``TERMINATE CLUSTER`` command, it takes the following steps:

-Sets the KSQL engine mode to ``NOT ACCEPTING NEW STATEMENTS`` so no new statements will be passed to the engine for execution.
-Terminates all persistent and transient queries in the engine and performs the required clean up for each query.
-Deletes the command topic for the cluster.

**Example request**

   .. code:: http

      POST /ksql/terminate HTTP/1.1
      Accept: application/vnd.ksql.v1+json
      Content-Type: application/vnd.ksql.v1+json

      {}

You can customize the clean up process if you want to delete some or all of the Kafka topics too:

**Provide a List of Kafka Topics to Delete**
You can provide a list of kafka topic names or regular expressions for kafka topic names along with your ``TERMINATE CLUSTER`` request.
The KSQL server will delete all topics with names that are in the list or that match any of the regular expressions in the list.
Only the topics that were generated by KSQL queries will be considered for deletion. Topics that were not generated by KSQL queries
will not be deleted even if they match the provided list.
The following example will delete topic ``FOO`` along with all topics with prefix ``bar``.

   .. code:: http

      POST /ksql/terminate HTTP/1.1
      Accept: application/vnd.ksql.v1+json
      Content-Type: application/vnd.ksql.v1+json

      {
        "deleteTopicList": ["FOO", "bar.*"]
      }
