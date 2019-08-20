.. _ksql_changelog:

KSQL Changelog
==============

Version 5.4.0
-------------

KSQL 5.4.0 includes new features, including:

* UDAFs support STRUCTs as parameters and return values.

* KSQL now supports working with source data where the value is an anonymous Avro or JSON serialized
  ``ARRAY``, ``MAP`` or primitive type, for example ``STRING`` or ``BIGINT``. Previously KSQL required all
  Avro values to be Avro records, and all JSON values to be JSON objects.
  For more information, refer to :ref:`ksql_single_field_wrapping`.

* KSQL now allows users to control how results containing only a single value field are serialized
  to Kafka. Users can now choose to serialize the single value as a named field within an outer
  Avro record or JSON object, depending on the format in use, or as an anonymous value.
  For more information, refer to :ref:`ksql_single_field_wrapping`.

* A new config ``ksql.metrics.tags.custom`` for adding custom tags to emitted JMX metrics.
  See :ref:`ksql-metrics-tags-custom` for usage.

* New ``UNIX_TIMESTAMP()`` and ``UNIX_DATE()`` functions.

* A new ``KAFKA`` format that supports ``INT``, ``BIGINT``, ``DOUBLE`` and ``STRING`` fields that
  have been serialized using the standard Kafka serializers,
  e.g. ``org.apache.kafka.common.serialization.LongSerializer``.

  The format only supports single values, i.e. only single field, being primarily intended for use
  as a key format.

KSQL 5.4.0 includes the following misc. changes:

* Require either the value for a ``@UdfParameter`` or for the UDF JAR to be compiled with
  the Java8 ``-parameters`` compilation option. The UDF archetype now includes this flag.

Version 5.3.0
-------------

KSQL 5.3.0 includes new features, including:

* Drop the requirement that ``CREATE TABLE`` statements must have a ``KEY`` set in their ``WITH`` clause.
  This is now an optional optimization to avoid unnecessary repartition steps.
  See `Github issue #2745 <https://github.com/confluentinc/ksql/pull/2745>`_ for more info.

* Improved handling of ``KEY`` fields. The ``KEY`` field is an optional copy of the Kafka record's key held
  within the record's value. Users can supply the name of the field that holds the copy of the key within
  the ``WITH`` clause.
  The improved handling may eliminate unnecessary repartition steps in certain queries.
  Please note that preexisting persistent queries, e.g. those created via ``CREATE TABLE AS SELECT ...`` or
  ``CREATE STREAM AS SELECT ...`` or ``INSERT INTO ...``, will continue to have the unnecessary repartition step.
  This is required to avoid the potential for data loss should this step be dropped.
  See `Github issue #2636 <https://github.com/confluentinc/ksql/pull/2636>`_ for more info.

* ``INSERT INTO ... VALUES`` is now supported, with standard SQL syntax to insert rows to existing
  KSQL streams/tables. To disable this functionality, set ``ksql.insert.into.values.enabled`` to
  ``false`` in the server properties.

* ``CREATE STREAM`` and ``CREATE TABLE`` will now allow you to create the topic if it is missing.
  To do this, specify the ``PARTITIONS`` and optionally ``REPLICAS`` in the ``WITH`` clause.

* ``CREATE STREAM AS SELECT ...`` and ``CREATE TABLE AS SELECT ...`` statements now use the source
  topic partitions and replica counts for the sink topic if the config properties are not set in the ``WITH`` clause.
  In case of JOIN, the left hand side topic is used.
  This deprecates ``ksql.sink.partitions`` and ``ksql.sink.replicas`` config properties.

* A new config variable, ``ksql.internal.topic.replicas``, was introduced to set the replica count for
  the internal topics created by KSQL Server. The internal topics include the command topic and the config topic.

* A new KSQL testing tool was added. The tool is a command line utility that enables testing KSQL statements
  without requiring any infrastructure, like Apache Kafka and KSQL clusters.

KSQL 5.3.0 includes bug fixes, including:

* The ``ROWTIME`` of the row generated when a ``JOIN`` encounters late data was previous the ``ROWTIME`` of the late event,
  where as now it is the max of ``ROWTIME`` of the rows involved in the join.  This provides more deterministic join semantics.

* Return values of UDF and UDAFs are now correctly marked as optional, where previously there was
  potential for non-optional fields, which would result in serialization issues in the presence
  of ``null`` values.

  This is a forward compatible change in Avro, i.e. after upgrading, KSQL will be able to
  read old values using the new schema. However, it is important to ensure downstream
  consumers of the data are using the updated schema before upgrading KSQL, as otherwise
  deserialization may fail. The updated schema is best obtained from running the query in
  another KSQL cluster, running version 5.3.

  See `Github issue #2769 <https://github.com/confluentinc/ksql/pull/2769>`_ for more info.

* Fixed issues with using ``AS`` keyword when aliasing sources.
  See `#2732 <https://github.com/confluentinc/ksql/issues/2732>`_ for more info.


Version 5.2.0
-------------

KSQL 5.2 includes new features, including:

* Support for :ref:`HTTPS <config-ksql-for-https>`.
* Support for CASE expression: KSQL now supports CASE conditional expression in Searched form where KSQL evaluates each condition from left to right.
  It returns the result for the first condition that evaluates to true. If no condition evaluates to true, the result for the ELSE clause will be returned.
  If there is no ELSE clause, null is returned.

* A new family of UDFs for improved handling of URIs (e.g. extracting information/decoding information), see :ref:`UDF table <functions>` for all URL functions
* ``LIMIT`` keyword support for ``PRINT`` (`#1316 <https://github.com/confluentinc/ksql/issues/1316>`_)
* Support for read-after-write consistency: new commands don't execute until previous commands have finished executing.
  This feature is enabled by default in the CLI (`#2280 <https://github.com/confluentinc/ksql/pull/2280>`_)
  and can be implemented by the user for the REST API (:ref:`coordinate_multiple_requests`).
* A log of record processing events to help users debug their KSQL queries. The log can be configured
  to log to Kafka to be consumed as a KSQL stream. See :ref:`KSQL processing log <ksql_processing_log>`
  for more details.
* Aggregation functionality has been extended. KSQL now supports:

  * ``GROUP BY`` more than just simple columns, including fields within structs,
    arithmetic results, functions, string concatenations and literals.
  * literals in the projection, (a.k.a the ``SELECT`` clause).
  * Multiple ``HAVING`` clauses, including the use of aggregate functions and literals.
  
* Automatic compatibility management for queries in headless mode across versions. Starting with 5.2, KSQL will automatically take care
  of ensuring query compatiblity when upgrading. This means you won't need to worry about setting properties correctly during upgrade, as
  has been required for previous upgrades. Refer to the :ref:`architecture documentation <ksql-architecture-config-topic>` for details.
  Note that it is still up to the user to set properties correctly before upgrading to 5.2. The :ref:`upgrade doc <upgrading-ksql>` has
  details about the properties required to safely upgrade to 5.2.

KSQL 5.2 includes bug fixes, including:

* Improved support for multi-line requests in interactive mode deployments.
  Prior to version 5.2 KSQL parsed the full request before attempting to execute any statements.
  Requests that contained later statements that were dependent the execution of prior statements
  may have failed. In version 5.2 and later, this is no longer an issue.

* Improved support for non-interactive, "headless" mode deployments.
  Prior to version 5.2 KSQL parsed the full script before attempting to execute any statements.
  The full parse would often fail when later statements relied on the execution of earlier
  statements. In version 5.2 and later, this is no longer an issue.

KSQL 5.2 deprecates some features, including:

* The use of the ``RUN SCRIPT`` statement via the REST API is now deprecated and will be removed
  in the next major release.
  (`Github issue 2179 <https://github.com/confluentinc/ksql/issues/2179>`_).
  The feature circumnavigates certain correctness checks and is unnecessary,
  given the script content can be supplied in the main body of the request.
  If you are using the ``RUN SCRIPT`` functionality from the KSQL CLI you will not be affected,
  as this will continue to be supported.
  If you are using the ``RUN SCRIPT`` functionality directly against the REST API your requests
  will work with the 5.2 server, but will be rejected after the next major version release.
  Instead, include the contents of the script in the main body of your request.

Version 5.1.0
-------------

KSQL 5.1 includes new features, including:

* ``WindowStart()`` and ``WindowEnd()`` UDFs
* ``StringToDate()`` and ``DateToString()`` UDFs

Detailed Changlog
+++++++++++++++++

* `PR-2265 <https://github.com/confluentinc/ksql/pull/2265>`_ - MINOR: Fix bug encountered when restoring RUN SCRIPT
* `PR-2240 <https://github.com/confluentinc/ksql/pull/2240>`_ - Bring version checker improvements to 5.1.x
* `PR-2242 <https://github.com/confluentinc/ksql/pull/2242>`_ - KSQL-1795: First draft of STRUCT topic
* `PR-2235 <https://github.com/confluentinc/ksql/pull/2235>`_ - KSQL-1794: First draft of query with arrays and maps topic
* `PR-2239 <https://github.com/confluentinc/ksql/pull/2239>`_ - KSQL-1975: Fix munged Docker commands for kafkacat examples
* `PR-2232 <https://github.com/confluentinc/ksql/pull/2232>`_ - KSQL-1912: Fix munged scalar functions table
* `PR-2229 <https://github.com/confluentinc/ksql/pull/2229>`_ - KSQL-1912: Remove extraneous newline
* `PR-2227 <https://github.com/confluentinc/ksql/pull/2227>`_ - KSQL-1912: Add IFNULL to Scalar Functions table
* `PR-2219 <https://github.com/confluentinc/ksql/pull/2219>`_ - KSQL-1912: Add IFNULL function to functions table
* `PR-2223 <https://github.com/confluentinc/ksql/pull/2223>`_ - KSQL-1958: Fix munged CSAS properties table YET AGAIN
* `PR-2222 <https://github.com/confluentinc/ksql/pull/2222>`_ - KSQL-1957: Add links to new topics; also restore missing CSAS and CTAS text
* `PR-2221 <https://github.com/confluentinc/ksql/pull/2221>`_ - DOCS-960: Add link to partitioning topic in key requirements section
* `PR-2220 <https://github.com/confluentinc/ksql/pull/2220>`_ - DOCS-960: Add note about the KEY property
* `PR-2134 <https://github.com/confluentinc/ksql/pull/2134>`_ - KSQL-1787: First draft of Time and Windows topic
* `PR-2201 <https://github.com/confluentinc/ksql/pull/2201>`_ - KSQL-1930: Fix a typo in the new Transform a Stream topic
* `PR-2180 <https://github.com/confluentinc/ksql/pull/2180>`_ - KSQL-1797: First draft of Transform a Stream topic
* `PR-2181 <https://github.com/confluentinc/ksql/pull/2181>`_ - KSQL-1796: First draft of aggregation topic
* `PR-2136 <https://github.com/confluentinc/ksql/pull/2136>`_ - Add reference about compatibility breaking configs in upgrade docs
* `PR-2193 <https://github.com/confluentinc/ksql/pull/2193>`_ - Fix flaky json format test
* `PR-2195 <https://github.com/confluentinc/ksql/pull/2195>`_ - 5.0.x fix flaky
* `PR-2174 <https://github.com/confluentinc/ksql/pull/2174>`_ - DOCS-1006: Fix munged :: block
* `PR-2170 <https://github.com/confluentinc/ksql/pull/2170>`_ - DOCS-911: Fix typos and grammatical errors
* `PR-2169 <https://github.com/confluentinc/ksql/pull/2169>`_ - DOCS-911: Fix typos and grammatical errors
* `PR-2142 <https://github.com/confluentinc/ksql/pull/2142>`_ - KSQL-1786: First draft of KSQL and KStreams topic
* `PR-2165 <https://github.com/confluentinc/ksql/pull/2165>`_ - KSQL-1854: Merge partition sections
* `PR-2143 <https://github.com/confluentinc/ksql/pull/2143>`_ - Fix some bugs in recovery logic
* `PR-2156 <https://github.com/confluentinc/ksql/pull/2156>`_ - KSQL-1864: Remove ksql> prompt from example commands
* `PR-2155 <https://github.com/confluentinc/ksql/pull/2155>`_ - KSQL-1864: Remove ksql> prompt from example commands
* `PR-2152 <https://github.com/confluentinc/ksql/pull/2152>`_ - KSQL-1864: Remove $ chars prompts for example commands
* `PR-2150 <https://github.com/confluentinc/ksql/pull/2150>`_ - Currently we don't support AS for aliasing stream/table.
* `PR-2149 <https://github.com/confluentinc/ksql/pull/2149>`_ - Using ksql topic name instead of Kafka topic name in topic map in metastore.
* `PR-2137 <https://github.com/confluentinc/ksql/pull/2137>`_ - Clarify the description of SUBSTRING and its legacy mode setting.
* `PR-2120 <https://github.com/confluentinc/ksql/pull/2120>`_ - KSQL-1789: First draft of Create a KSQL Table topic
* `PR-2132 <https://github.com/confluentinc/ksql/pull/2132>`_ - KSQL-1853: Fix heading levels in join and partition topics
* `PR-2130 <https://github.com/confluentinc/ksql/pull/2130>`_ - DOCS-950: Reworked partitions topic per feedback
* `PR-2122 <https://github.com/confluentinc/ksql/pull/2122>`_ - Bringing back the commit that was lost because of bad merge.
* `PR-2109 <https://github.com/confluentinc/ksql/pull/2109>`_ - KSQL-1799: New topic: Troubleshoot KSQL
* `PR-2092 <https://github.com/confluentinc/ksql/pull/2092>`_ - Window's UDF doc changes.
* `PR-2090 <https://github.com/confluentinc/ksql/pull/2090>`_ - Add WindowStart and WindowEnd UDFs (#1993)
* `PR-2075 <https://github.com/confluentinc/ksql/pull/2075>`_ - Disable optimizations for 5.1.x
* `PR-2051 <https://github.com/confluentinc/ksql/pull/2051>`_ - Preserve originals when merging configs
* `PR-2080 <https://github.com/confluentinc/ksql/pull/2080>`_ - Fixed the test.
* `PR-2079 <https://github.com/confluentinc/ksql/pull/2079>`_ - Fix deprecation issues.
* `PR-2031 <https://github.com/confluentinc/ksql/pull/2031>`_ - Fix deprecated issues in the build
* `PR-2066 <https://github.com/confluentinc/ksql/pull/2066>`_ - Minor: Fix bug involving filters with NOT keyword.
* `PR-2056 <https://github.com/confluentinc/ksql/pull/2056>`_ - Added stringtodate and datetostring UDFs for 5.1.x
* `PR-2048 <https://github.com/confluentinc/ksql/pull/2048>`_ - Minor: Fix bug involving LIKE patterns without wildcards.
* `PR-2045 <https://github.com/confluentinc/ksql/pull/2045>`_ - List UDAFs for 5.1.x
* `PR-2043 <https://github.com/confluentinc/ksql/pull/2043>`_ - Bump airline version to 2.6.0
* `PR-2023 <https://github.com/confluentinc/ksql/pull/2023>`_ - MINOR: Cause 'ksql help' and 'ksql -help' to behave the same as 'ksql -h' and 'ksql --help'
* `PR-1979 <https://github.com/confluentinc/ksql/pull/1979>`_ - Metrics refactor + fix a couple issues
* `PR-2018 <https://github.com/confluentinc/ksql/pull/2018>`_ - Display stats timestamps in unambiguous format.
* `PR-2017 <https://github.com/confluentinc/ksql/pull/2017>`_ - KSQL-1725: Fix tables and build warnings
* `PR-1997 <https://github.com/confluentinc/ksql/pull/1997>`_ - MINOR: Remove duplicate junit dependency in ksql-examples
* `PR-2014 <https://github.com/confluentinc/ksql/pull/2014>`_ - KSQL-1722: Fix broken inline literal
* `PR-2007 <https://github.com/confluentinc/ksql/pull/2007>`_ - KSQL-1722: Fix build error in changelog.rst
* `PR-1991 <https://github.com/confluentinc/ksql/pull/1991>`_ - Minor: Switch tests to use mock Kafka clients.
* `PR-1992 <https://github.com/confluentinc/ksql/pull/1992>`_ - Minor: Improve test output for QueryTranslationTest
* `PR-1999 <https://github.com/confluentinc/ksql/pull/1999>`_ - KSQL-1717: Fix build warning in faq.rst
* `PR-1981 <https://github.com/confluentinc/ksql/pull/1981>`_ - ST-1153: Switch to use cp-base-new and bash-config to hide passwords by default
* `PR-1977 <https://github.com/confluentinc/ksql/pull/1977>`_ - Use version 5.0.0 for KSQL server image
* `PR-1955 <https://github.com/confluentinc/ksql/pull/1955>`_ - Hide ssl configs and refactor KsqlResourceTest
