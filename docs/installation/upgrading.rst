.. _upgrading-ksql:

Upgrading ksqlDB
================

Upgrade one server at a time in a "rolling restart". The remaining servers
should have sufficient spare capacity to take over temporarily for unavailable,
restarting servers.

For general guidance on upgrading, see
[Upgrade ksqlDB](https://docs.ksqldb.io/en/latest/operate-and-deploy/installation/upgrading/).

Upgrading to ksqlDB 5.5 from KSQL 5.4
-------------------------------------

.. important::

   The upgrade from KSQL 5.4 to ksqlDB 5.5 is not a rolling restart. You must
   shut down all KSQL instances and then start up all ksqlDB instances, so there
   will be downtime.

#. Prepare a list of SQL statements for migration. One way is to dump the command
   topic.

   ```bash
   ./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic _confluent-ksql-default__command_topic --from-beginning | jq ".statement" 
   ```

#. Use the DESCRIBE statement to list your application's streams, tables, and
   types. Don't use the EXTENDED option. Go through each list and re-create the
   corresponding CREATE statements based on the displayed schemas.
   
#. Make other compatibility-related changes, as shown in the
   `upgrade guide <https://docs.ksqldb.io/en/latest/operate-and-deploy/installation/upgrading/#how-to-upgrade>`__.

#. Save your statements as an SQL file. 

#. Stop the ``ksql`` service.

#. Uninstall the ksql 5.4 DEB or RPM package. For RPM, you can use the
   ``yum swap`` command.

#. Install the ksqldb 5.5 DEB or RPM package. 

#. Copy the version 5.4 configuration file to the ksqldb 5.5 configuration
   folder, which is located at ``${CONFLUENT_HOME}/etc/ksqldb``:

   ```bash
   cp /etc/ksql/ksql-server.properties /etc/ksqldb/ksql-server.properties
   ```

#. Change the ``ksql.service.id`` in the property file, for example, from
   ``default_`` to ``ksqldb_``.

#. Set up security for your ksqlDB app. ksqlDB supports using
   :ref:`role-based access control (RBAC) <ksql-rbac>`,
   :ref:`ACLs <config-security-ksql-acl>`, and no authorization.

   Create three new role bindings or assign ACLs for the ``ksql`` service
   principal:

   - Topic: ``__consumer_offsets``
   - Topic: ``__transaction_state``
   - TransactionalId: ``ksqldb_``. Use the value that you set in the
     configuration file.

#. Start the ``ksqldb`` service.

#. Run the SQL file that you prepared previously.

.. note::

    In addition to copying the properties file from ``ksql`` to ``ksqldb``, you may
    need to copy other configs, like log4j and systemd overrides, depending on how
    you have customized your deployment.

Upgrading to KSQL 5.4
---------------------

Notable changes in 5.4:

* KSQL Server

  * Query Id generation

    * This version of KSQL includes a change to how query ids are generated for Persistent Queries
      (INSERT INTO/CREATE STREAM AS SELECT/CREATE TABLE AS SELECT). Previously, query ids would be incremented
      on every successful Persistent Query created. New query ids use the Kafka record offset of the query
      creating command in the KSQL command topic.


      In order to prevent inconsistent query ids, don't create new Persistent Queries while
      upgrading your KSQL servers (5.3 or lower). Old running queries will retain their original id on restart,
      while new queries will utilize the new id convention.

      See `Github PR #3354 <https://github.com/confluentinc/ksql/pull/3354>`_ for more info.


Upgrading from KSQL 5.2 to KSQL 5.3
-----------------------------------

Notable changes in 5.3:

* KSQL Server

  * Avro schema compatibility

    * This version of KSQL fixes a bug where the schemas returned by UDF and UDAFs might
      not be marked as nullable. This can cause serialization issues in the presence of ``null``
      values, as might be encountered if the UDF fails.

      With the bug fix all fields are now optional.

      This is a forward compatible change in Avro, i.e. after upgrading, KSQL will be able to
      read old values using the new schema. However, it is important to ensure downstream
      consumers of the data are using the updated schema before upgrading KSQL, as otherwise
      deserialization may fail. The updated schema is best obtained from running the query in
      another KSQL cluster, running version 5.3.

      See `Github issue #2769 <https://github.com/confluentinc/ksql/pull/2769>`_ for more info.

* Configuration:

  * ``ksql.sink.partitions`` and ``ksql.sink.replicas`` are deprecated. All
    new queries will use the source topic partition count and replica count
    for the sink topic instead unless partitions and replicas are set in the
    WITH clause.

  * A new config variable, ``ksql.internal.topic.replicas``, was introduced to set the replica count for
    the internal topics created by KSQL Server. The internal topics include command topic or config topic.


Upgrading from KSQL 5.1 to KSQL 5.2
-----------------------------------

 Notable changes in 5.2:

* KSQL Server

  * Interactive mode:

    * The use of the ``RUN SCRIPT`` statement via the REST API is now deprecated and will be
      removed in the next major release.
      (`Github issue 2179 <https://github.com/confluentinc/ksql/issues/2179>`_).
      The feature circumnavigates certain correctness checks and is unnecessary,
      given the script content can be supplied in the main body of the request.
      If you are using the ``RUN SCRIPT`` functionality from the KSQL CLI you will not be
      affected, as this will continue to be supported.
      If you are using the ``RUN SCRIPT`` functionality directly against the REST API your
      requests will work with the 5.2 server, but will be rejected after the next major version
      release.
      Instead, include the contents of the script in the main body of your request.

* Configuration:

  * When upgrading your headless (non-interactive) mode application from version 5.0.0 and below, you must include the configs specified in the :ref:`5.1 upgrade instructions <5-1-upgrade>`.
  * When upgrading your headless (non-interactive) mode application, you must include the following properties in your properties file:

::

    ksql.windowed.session.key.legacy=true
    ksql.named.internal.topics=off
    ksql.streams.topology.optimization=none

.. _5-1-upgrade:

Upgrading from KSQL 5.0.0 and below to KSQL 5.1
-----------------------------------------------

* KSQL server:

  * The KSQL engine metrics are now prefixed with the ``ksql.service.id``. If you have been using any metric monitoring
    tool you need to update your metric names.
    For instance, assuming ``ksql.service.id`` is set to ``default_``, ``messages-produced-per-sec`` will be changed to ``_confluent-ksql-default_messages-consumed-per-sec``.

* Configuration:

  * When upgrading your headless (non-interactive) mode application, you must either update your queries to use the new SUBSTRING indexing semantics, or set ``ksql.functions.substring.legacy.args`` to ``true``. If possible, we recommend that you update your queries accordingly, instead of enabling this configuration setting. Refer to the SUBSTRING documentation in the :ref:`function <functions>` guide for details on how to do so. Note that this is NOT required for interactive mode KSQL.

Upgrading from KSQL 0.x (Developer Preview) to KSQL 4.1
-------------------------------------------------------

KSQL 4.1 is not backward-compatible with the previous KSQL 0.x developer preview releases.
In particular, you must manually migrate queries running in the older preview releases of KSQL to the 4.1 version by
issuing statements like ``CREATE STREAM`` and ``CREATE TABLE`` again.

Notable changes in 4.1:

* KSQL CLI:

  * The ``ksql-cli`` command was renamed to ``ksql``.
  * The CLI no longer supports what was formerly called "standalone" or "local" mode, where ``ksql-cli`` would run
    both the CLI and also a KSQL server process inside the same JVM.  In 4.1, ``ksql`` will only run the CLI.  For
    local development and testing, you can now run ``confluent start`` (which will also launch a KSQL server),
    followed by ``ksql`` to start the CLI. This setup is used for the
    :ref:`Confluent Platform quickstart <quickstart>`.  Alternatively, you can start the KSQL server directly as
    described in :ref:`start_ksql-server`, followed by ``ksql`` to start the CLI.

* KSQL server:

  * The default ``listeners`` address was changed to ``http://localhost:8088`` (KSQL 0.x used
    ``http://localhost:8080``).
  * Assigning KSQL servers to a specific KSQL cluster has been simplified and is now done with the
    ``ksql.service.id`` setting.  See :ref:`ksql-server-config` for details.

* Executing ``.sql`` files: To run pre-defined KSQL queries stored in a ``.sql`` file, see
  :ref:`restrict-ksql-interactive`.

* Configuration: Advanced KSQL users can configure the Kafka Streams and Kafka producer/consumer client settings used
  by KSQL.  This is achieved by using prefixes for the respective configuration settings.
  See :ref:`ksql-param-reference` as well as :ref:`ksql-server-config` and :ref:`install_cli-config` for details.
