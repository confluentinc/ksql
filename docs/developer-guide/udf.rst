.. _ksql-udfs:

KSQL Custom Function Reference (UDF and UDAF)
=============================================

KSQL has many built-in functions that help with processing records in
streaming data, like ABS and SUM. Functions are used within a KSQL query
to filter, transform, or aggregate data.

With the KSQL API, you can implement custom functions that go beyond the
built-in functions. For example, you can create a custom function that applies
a pre-trained machine learning model to a stream.

KSQL supports these kinds of functions: 

Stateless scalar function (UDF)
    A scalar function that takes one input row and returns one output value.
    No state is retained between function calls. When you implement a custom
    scalar function, it's called a *User-Defined Function (UDF)*.

Stateful aggregate function (UDAF)
    An aggregate function that takes *N* input rows and returns one output value.
    During the function call, state is retained for all input records, which
    enables aggregating results. When you implement a custom aggregate function,
    it's called a *User-Defined Aggregate Function (UDAF)*.

.. note:: Tabular functions, which take one input row and return *N* output
          values, aren't supported.

Implement a Custom Function
*************************** 

Folow these steps to create your custom functions:

#. Write your UDF or UDAF class in Java.

   * If your Java class is a UDF, mark it with the ``@UdfDescription`` and
     ``@Udf`` annotations.
   * If your class is a UDAF, mark it with the ``@UdafDescription`` and
     ``@UdafFactory`` annotations.

   For more information, see :ref:`example-udf-class` and :ref:`example-udaf-class`. 

#. Deploy the JAR file to the KSQL extensions directory. For more information,
   see :ref:`deploying-udf`.
#. Use your function like any other KSQL function in your queries.

.. tip:: The SHOW FUNCTIONS statement lists the available functions in your
         KSQL server, including your custom UDF and UDAF functions. Use the
         DESCRIBE FUNCTION statement to display details about your custom functions.

For a detailed walkthrough on creating a UDF, see :ref:`implement-a-udf`.

======================
Creating UDF and UDAFs
======================

KSQL supports creating User Defined Scalar Functions (UDFs) and User Defined Aggregate Functions (UDAF) via custom jars that are
uploaded to the ``ext/`` directory of the KSQL installation.
At start up time KSQL scans the jars in the directory looking for any classes that annotated
with ``@UdfDescription`` (UDF) or ``@UdafDescription`` (UDAF).
Classes annotated with ``@UdfDescription`` are scanned for any public methods that are annotated
with ``@Udf``. Classes annotated with ``@UdafDescription`` are scanned for any public static methods
that are annotated with ``@UdafFactory``. Each UD(A)F that is found is parsed and, if successful, loaded into KSQL.

Each UD(A)F instance has its own child-first ``ClassLoader`` that is isolated from other UD(A)Fs. If you
need to use any third-party libraries with your UDFs then they should also be part of your jar, i.e.,
you should create an "uber-jar". The classes in your uber-jar will be loaded in preference to any
classes on the KSQL classpath excluding anything vital to the running of KSQL, i.e., classes that are
part of ``org.apache.kafka`` and ``io.confluent``. Further, the ``ClassLoader`` can restrict access
to other classes via a blacklist. The blacklist file is ``resource-blacklist.txt``. You can add
any classes or packages that you want blacklisted from UDF use, for example you may not
want a UDF to be able to fork processes. Further details on how to blacklist are available below.

UDFs
----

To create a UDF you need to create a class that is annotated with ``@UdfDescription``.
Each method in the class that represents a UDF must be public and annotated with ``@Udf``. The class
you create represents a collection of UDFs all with the same name but may have different
arguments and return types.


Null Handling
~~~~~~~~~~~~~

If a UDF uses primitive types in its signature it is indicating that the parameter should never be null.
Conversely, using boxed types indicates the function can accept null values for the parameter.
It is up to the implementor of the UDF to chose which is the most appropriate.
A common pattern is to return ``null`` if the input is ``null``, though generally this is only for
parameters that are expected to be supplied from the source row being processed. For example,
a ``substring(String value, int beginIndex)`` UDF might return null if ``value`` is null, but a
null ``beginIndex`` parameter would be treated as an error, and hence should be a primitive.

The return type of a UDF can also be a primitive or boxed type. A primitive return type indicates
the function will never return ``null``, where as a boxed type indicates it may return ``null``.

The KSQL server will check the value being passed to each parameter and report an error to the server
log for any null values being passed to a primitive type. The associated column in the output row
will be ``null``.

.. _example-udf-class:

Example UDF class
~~~~~~~~~~~~~~~~~

The class below creates a UDF named ``multiply``. The name of the UDF is provided in the ``name``
parameter of the ``UdfDescription`` annotation. This name is case-insensitive and is what can be
used to call the UDF. As can be seen this UDF can be invoked in different ways:

- with two int parameters returning a long (BIGINT) result.
- with two long (BIGINT) parameters returning a long (BIGINT) result.
- with two nullable Long (BIGINT) parameters returning a nullable Long (BIGINT) result.
- with two double parameters returning a double result.

.. code:: java

    import io.confluent.ksql.function.udf.Udf;
    import io.confluent.ksql.function.udf.UdfDescription;

    @UdfDescription(name = "multiply", description = "multiplies 2 numbers")
    public class Multiply {

      @Udf(description = "multiply two non-nullable INTs.")
      public long multiply(final int v1, final int v2) {
        return v1 * v2;
      }

      @Udf(description = "multiply two non-nullable BIGINTs.")
      public long multiply(final long v1, final long v2) {
        return v1 * v2;
      }

      @Udf(description = "multiply two nullable BIGINTs. If either param is null, null is returned.")
      public Long multiply(final Long v1, final Long v2) {
        return v1 == null || v2 == null ? null : v1 * v2;
      }

      @Udf(description = "multiply two non-nullable DOUBLEs.")
      public double multiply(final double v1, double v2) {
        return v1 * v2;
      }
    }

If you're using Gradle to build your UDF or UDAF, specify the ``ksql-udf``
dependency: 

.. codewithvars:: bash

    compile 'io.confluent.ksql:ksql-udf:|release|'

To compile with the latest version of ``ksql-udf``:

.. codewithvars:: bash

    compile 'io.confluent.ksql:ksql-udf:+'

If you're using Maven to build your UDF or UDAF, specify the ``ksql-udf``
dependency in your POM file:

.. codewithvars:: xml

    <!-- Specify the repository for Confluent dependencies -->
        <repositories>
            <repository>
                <id>confluent</id>
                <url>http://packages.confluent.io/maven/</url>
            </repository>
        </repositories>

    <!-- Specify the ksql-udf dependency -->
    <dependencies>
        <dependency>
            <groupId>io.confluent.ksql</groupId>
            <artifactId>ksql-udf</artifactId>
            <version>|release|</version>
        </dependency>
    </dependencies>


UdfDescription Annotation
~~~~~~~~~~~~~~~~~~~~~~~~~
The ``@UdfDescription`` annotation is applied at the class level and has four fields, two of which are required.
The information provided here is used by the ``SHOW FUNCTIONS`` and ``DESCRIBE FUNCTION <function>`` commands.

+------------+------------------------------+---------+
| Field      | Description                  | Required|
+============+==============================+=========+
| name       | The case-insensitive name of | Yes     |
|            | the UDF(s)                   |         |
|            | represented by this class.   |         |
+------------+------------------------------+---------+
| description| A string describing generally| Yes     |
|            | what the function(s) in this |         |
|            | class do.                    |         |
+------------+------------------------------+---------+
| author     | The author of the UDF.       | No      |
+------------+------------------------------+---------+
| version    | The version of the UDF.      | No      |
+------------+------------------------------+---------+


Udf Annotation
~~~~~~~~~~~~~~

The ``@Udf`` annotation is applied to public methods of a class annotated with ``@UdfDescription``.
Each annotated method will become an invocable function in KSQL. The annotation only has a single
field ``description`` that is required. You can use this to better describe what a particular version
of the UDF does, for example:

.. code:: java

    @Udf(description = "Returns a string that is a substring of this string. The"
        + " substring begins with the character at the specified startIndex and"
        + " extends to the end of this string.")
    public String substring(final String value, final int startIndex)

    @Udf(description = "Returns a string that is a substring of this string. The"
        + " substring begins with the character at the specified startIndex and"
        + " extends to the character at endIndex -1.")
    public String substring(final String value, final int startIndex, final int endIndex)

.. _ksql-udafs:

UDAFs
-----
To create a UDAF you need to create a class that is annotated with ``@UdafDescription``.
Each method in the class that is used as a factory for creating an aggregation must be ``public static``,
be annotated with ``@UdafFactory``, and must return either ``Udaf`` or ``TableUdaf``. The class
you create represents a collection of UDAFs all with the same name but may have different
arguments and return types.

.. _example-udaf-class:

Example UDAF class
~~~~~~~~~~~~~~~~~~

The class below creates a UDAF named ``my_sum``. The name of the UDAF is provided in the ``name``
parameter of the ``UdafDescription`` annotation. This name is case-insensitive and is what can be
used to call the UDAF. The UDAF can be invoked in four ways:

- With a Long (BIGINT) column, returning the aggregated value as Long (BIGINT). Can also be used to support table aggregations
  as the return type is ``TableUdaf`` and therefore supports the ``undo`` operation.
- with an Integer column returning the aggregated value as Long (BIGINT).
- with a Double column, returning the aggregated value as Double.
- with a String (VARCHAR) and an initializer that is a String (VARCHAR), returning the aggregated String (VARCHAR) length
  as a Long (BIGINT).

.. code:: java

    @UdafDescription(name = "my_sum", description = "sums")
    public class SumUdaf {

      @UdafFactory(description = "sums longs")
      // Can be used with table aggregations
      public static TableUdaf<Long, Long> createSumLong() {
        return new TableUdaf<Long, Long>() {
          @Override
          public Long undo(final Long valueToUndo, final Long aggregateValue) {
            return aggregateValue - valueToUndo;
          }

          @Override
          public Long initialize() {
            return 0L;
          }

          @Override
          public Long aggregate(final Long aggregate, final Long aLong) {
            return aggregate + aLong;
          }

          @Override
          public Long merge(final Long aggOne, final Long aggTwo) {
            return aggOne + aggTwo;
          }
        };
      }

      @UdafFactory(description = "sums int")
      public static TableUdaf<Integer, Long> createSumInt() {
        return new TableUdaf<Integer, Long>() {
          @Override
          public Long undo(final Integer valueToUndo, final Long aggregateValue) {
            return aggregateValue - valueToUndo;
          }

          @Override
          public Long initialize() {
            return 0L;
          }

          @Override
          public Long aggregate(final Integer current, final Long aggregate) {
            return current + aggregate;
          }

          @Override
          public Long merge(final Long aggOne, final Long aggTwo) {
            return aggOne + aggTwo;
          }
        };
      }

      @UdafFactory(description = "sums double")
      public static Udaf<Double, Double> createSumDouble() {
        return new Udaf<Double, Double>() {
          @Override
          public Double initialize() {
            return 0.0;
          }

          @Override
          public Double aggregate(final Double aggregate, final Double val) {
            return aggregate + val;
          }

          @Override
          public Double merge(final Double aggOne, final Double aggTwo) {
            return aggOne + aggTwo;
          }
        };
      }

      // This method shows providing an initial value to an aggregated, i.e., it would be called
      // with my_sum(col1, 'some_initial_value')
      @UdafFactory(description = "sums the length of strings")
      public static Udaf<String, Long> createSumLengthString(final String initialString) {
        return new Udaf<String, Long>() {
          @Override
          public Long initialize() {
            return (long) initialString.length();
          }

          @Override
          public Long aggregate(final String s, final Long aggregate) {
            return aggregate + s.length();
          }

          @Override
          public Long merge(final Long aggOne, final Long aggTwo) {
            return aggOne + aggTwo;
          }
        };
      }

    }

UdafDescription Annotation
~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``@UdafDescription`` annotation is applied at the class level and has four fields, two of which are required.
The information provided here is used by the ``SHOW FUNCTIONS`` and ``DESCRIBE FUNCTION <function>`` commands.

+------------+------------------------------+---------+
| Field      | Description                  | Required|
+============+==============================+=========+
| name       | The case-insensitive name of | Yes     |
|            | the UDAF(s)                  |         |
|            | represented by this class.   |         |
+------------+------------------------------+---------+
| description| A string describing generally| Yes     |
|            | what the function(s) in this |         |
|            | class do.                    |         |
+------------+------------------------------+---------+
| author     | The author of the UDF.       | No      |
+------------+------------------------------+---------+
| version    | The version of the UDF.      | No      |
+------------+------------------------------+---------+


UdafFactory Annotation
~~~~~~~~~~~~~~~~~~~~~~

The ``@UdafFactory`` annotation is applied to public static methods of a class annotated with ``@UdafDescription``.
The method must return either ``Udaf``, or, if it supports table aggregations, ``TableUdaf``.
Each annotated method is a factory for an invocable aggregate function in KSQL. The annotation only has a single
field ``description`` that is required. You can use this to better describe what a particular version
of the UDF does, for example:

.. code:: java

    @UdafFactory(description = "Sums BIGINT columns.")
    public static TableUdaf<Long, Long> createSumLong(){...}

    @UdafFactory(description = "Sums the length of VARCHAR columns".)
    public static Udaf<String, Long> createSumLengthString(final String initialString)


===============
Supported Types
===============

The types supported by UDFs are currently limited to:

+--------------+------------------+
|  Java Type   | KSQL Type        |
+==============+==================+
| int          | INTEGER          |
+--------------+------------------+
| Integer      | INTEGER          |
+--------------+------------------+
| boolean      | BOOLEAN          |
+--------------+------------------+
| Boolean      | BOOLEAN          |
+--------------+------------------+
| long         | BIGINT           |
+--------------+------------------+
| Long         | BIGINT           |
+--------------+------------------+
| double       | DOUBLE           |
+--------------+------------------+
| Double       | DOUBLE           |
+--------------+------------------+
| String       | VARCHAR          |
+--------------+------------------+
| List         | ARRAY            |
+--------------+------------------+
| Map          | MAP              |
+--------------+------------------+

Note: Complex types other than List and Map are not currently supported

.. _deploying-udf:

=========
Deploying
=========

To deploy your UD(A)Fs you need to create a jar containing all of the classes required by the UD(A)Fs.
If you depend on third-party libraries then this should be an uber-jar containing those libraries.
Once the jar is created you need to deploy it to each KSQL server instance. The jar should be copied
to the ``ext/`` directory that is part of the KSQL distribution. The ``ext/`` directory can be configured
via the ``ksql.extension.dir``.

The jars in the ``ext/`` directory are only scanned at start-up, so you will need to restart your
KSQL server instances to pick up new UD(A)Fs.

It is important to ensure that you deploy the custom jars to each server instance. Failure to do so
will result in errors when processing any statements that try to use these functions. The errors
may go unnoticed in the KSQL CLI if the KSQL server instance it is connected to has the jar installed,
but one or more other KSQL servers don't have it installed. In these cases the errors will appear
in the KSQL server log (ksql.log) . The error would look something like:

::

    [2018-07-04 12:37:28,602] ERROR Failed to handle: Command{statement='create stream pageviews_ts as select tostring(viewtime) from pageviews;', overwriteProperties={}} (io.confluent.ksql.rest.server.computation.StatementExecutor:210)
    io.confluent.ksql.util.KsqlException: Can't find any functions with the name 'TOSTRING'


The servers that don't have the jars will not process any queries using
the custom UD(A)Fs. Processing will continue, but it will be restricted to only the servers with the
correct jars installed.


=====
Usage
=====

Once your UD(A)Fs are deployed you can call them in the same way you would invoke any of the KSQL
built-in functions. The function names are case-insensitive. For example, using the ``multiply`` example above:

.. code:: sql

    CREATE STREAM number_stream (int1 INT, int2 INT, long1 BIGINT, long2 BIGINT)
      WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC = 'numbers');

    SELECT multiply(int1, int2), MULTIPLY(long1, long2) FROM number_stream;



==================================
KSQL Custom Functions and Security
==================================

Blacklisting
------------

In some deployment environments it may be necessary to restrict the classes that UD(A)Fs have access
to as they may represent a security risk. To reduce the attack surface of KSQL UD(A)Fs you can optionally
blacklist classes and packages such that they can't be used from a UD(A)F. There is an example
blacklist that is found in the file ``resource-blacklist.txt`` that is in the ``ext/`` directory.
All the entries in it are commented out, but it demonstrates how you can use the blacklist.

This file contains an entry per line, where each line is a class or package that should be blacklisted.
The matching of the names is based on a regular expression, so if you have an entry, ``java.lang.Process``

::

    java.lang.Process

This would match any paths that begin with java.lang.Process, i.e., java.lang.Process, java.lang.ProcessBuilder etc.

If you want to blacklist a single class, i.e., ``java.lang.Compiler``, then you would add:

::

    java.lang.Compiler$

Any blank lines or lines beginning with ``#`` are ignored. If the file is not present, or is empty, then
no classes are blacklisted.

Security Manager
----------------

By default KSQL installs a simple java security manager for UD(A)F execution. The security manager
blocks attempts by any UD(A)Fs to fork processes from the KSQL server. It also prevents them from
calling ``System.exit(..)``.

The security manager can be disabled by setting ``ksql.udf.enable.security.manager`` to false.

Disabling KSQL Custom Functions
-------------------------------

You can disable the loading of all UDFs in the ``ext/`` directory by setting ``ksql.udfs.enabled`` to
``false``. By default they are enabled.


=================
Metric Collection
=================

Metric collection can be enabled by setting the config ``ksql.udf.collect.metrics`` to ``true``.
This defaults to ``false`` and is generally not recommended for production usage as metrics
will be collected on each invocation and will introduce some overhead to processing time.


