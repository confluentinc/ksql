.. _cloud-ksql-migration-guide:

Migrate |ccloud| KSQL applications
##################################

To deliver the benefits of new features and improvements as soon as they become
available, new versions of |ccloud| KSQL are deployed frequently. Most of these
deployments are invisible to you as a user, requiring no action on your part.

The production-available (PA) version of |ccloud| KSQL is not backward-compatible 
with earlier versions, but after the release of the production-available
version, we're committed to preserving backward compatibility.

Backward incompatibility comes in various forms, including changes to
serialization formats, data models, and the SQL syntax. To continue running your
application from earlier versions of KSQL, you may need to migrate your
workload to an application that's backed by the production-available version.
This document provides a simple process to guide you through this migration.

.. note::

    We will never automatically upgrade your KSQL application to use a
    backward-incompatible release.

Port the application’s schema
*****************************

You can run the following commands by using the |ccloud| web interface or the
KSQL CLI. If you choose to use the KSQL CLI, you may find the SPOOL command
to be helpful, because it captures all the output from your session to a file
on your local filesystem. For more information, see
`Confluent Cloud ksqlDB Quick Start <https://docs.confluent.io/cloud/current/get-started/ksql.html>`__.

Now let’s begin the migration process:

#. Capture all stream definitions:

   .. code:: sql

      LIST STREAMS EXTENDED;

#. Copy the SQL statements that created each stream and save them to a file, ignoring KSQL_PROCESSING_LOG. You will run these statements in a later step.

#. Capture all table definitions:

   .. code:: sql

      LIST TABLES EXTENDED;

#. Copy the SQL statements that created each table and save them to your schema file.

#. Capture custom types:

   .. code:: sql

      LIST TYPES;

#. Convert the ``LIST TYPES`` output into ``CREATE TYPE <name> AS <schema>``
   syntax by obtaining the name from the first column and the schema from the
   second column of the output. Save these statements to your schema file.

#. Order all captured SQL statements by dependency: you'll now have the list
   of all SQL statements required to rebuild the schema, but they are not yet
   ordered in terms of dependencies. You must reorder the statements to ensure
   each statement comes after any other statements it depends on.

#. Update the captured SQL statements to take into account any changes in
   syntax or functionality between the old and new KSQL versions.

#. Destroy your old application using the web interface or |ccloud| CLI. If you
   don't do this, both the old and new applications will be publishing to sink
   topics, resulting in undefined behavior.

#. Provision a new |ccloud| KSQL application, backed by your existing |ak|
   cluster, by using the web interface or the |ccloud| CLI.

#. If you want your new application to consume all of your |ak| data from the
   earliest offset, add the following line to the beginning of your schema file:
   ``SET 'auto.offset.reset'='earliest'``;

#. Build the schema in the new application by executing each SQL statement in
   your schema file from the previous steps. This is best achieved by using
   the RUN SCRIPT command from a KSQL CLI session, which takes a file as its input.

#. Rebuild application state.

Porting the database schema to the new application causes KSQL to start
processing data. If you used ``SET 'auto.offset.reset'='earliest'``, the new
application begins processing all data from the beginning of each input topic.
The application will be processing data that the old application has already
processed.

.. note::

    Source data that the old application processed may no longer be available
    in |ak| for the new application to process, for example, topics that have
    limited retention. It's possible that the new application has different
    results from the previous application.

Persistent queries that output to an existing |ak| topic may produce duplicate
rows when replaying input data. For example, the following persistent query
writes its output to a topic named ``sink_topic``:

.. code:: sql

    CREATE STREAM output_stream WITH (kafka_topic=’sink_topic’, value_format=’json’) AS
      SELECT * FROM input_stream EMIT CHANGES;

If the same query is recreated in a new KSQL application using the existing |ak|
cluster, that output is duplicated if the query consumes from earliest.


