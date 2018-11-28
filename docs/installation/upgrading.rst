.. _upgrading-ksql:

Upgrading KSQL
==============

Upgrade one KSQL server at a time (i.e. rolling restart). The remaining KSQL servers should have sufficient spare
capacity to take over temporarily for unavailable, restarting servers.


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

Upgrading from KSQL 5.0.0 and below to KSQL 5.1
-----------------------------------------------

* KSQL server:

    * The KSQL engine metrics are now prefixed with the ``ksql.service.id``. If you have been using any metric monitoring
      tool you need to update your metric names.
      For instance, assuming ``ksql.service.id`` is set to ``default``, ``messages-produced-per-sec`` will be changed to ``_confluent-ksql-default_messages-consumed-per-sec``.

* Configuration:

    * When upgrading your headless (non-interactive) mode application, you must either update your queries to use the new SUBSTRING indexing semantics, or set ``ksql.functions.substring.legacy.args`` to ``true``. If possible, we recommend that you update your queries accordingly, instead of enabling this configuration setting. Refer to the SUBSTRING documentation in the :ref:`function <functions>` guide for details on how to do so. Note that this is NOT required for interactive mode KSQL.
