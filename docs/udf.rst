.. _ksql-udfs:

KSQL User Defined Function Reference
====================================

KSQL supports creating User Defined Functions (UDFs) via custom jars that are
uploaded to the ext/ directory of the KSQL installation. At start up time KSQL scans the
jars in the directory looking for any classes that annotated with ``@UdfDescription``. It then
searches the class for any public methods that are annotated with ``@Udf``. Each UDF that is found
is parsed and, if successful, loaded into KSQL.

Each UDF instance has its own child-first ``ClassLoader`` that is isolated from other UDFs. If you
need to use any third-party libraries with your UDFs then they should also be part of your jar, i.e.,
you should create an "uber-jar". The classes in your uber-jar will be loaded in preference to any
classes on the KSQL classpath excluding anything vital to the running of KSQL, i.e., classes that are
part of ``org.apache.kafka`` and ``io.confluent``. Further, the ``ClassLoader`` can restrict access
to other classes via a blacklist. The blacklist file is ``resource-blacklist.txt``. You can add
any classes or packages that you want blacklisted from UDF use, for example you may not
want a UDF to be able to fork processes. Further details on how to blacklist are available below.

=============
Creating UDFs
=============

To create a UDF you need to create a class that is annotated with ``@UdfDescription``. Each method in
the class that represents a UDF must be public and annotated with ``@Udf``. The class
you create represents a collection of UDFs all with the same name but may have different
arguments and return types.


Example UDF class
-----------------

The class below creates a UDF named ``multiply``. As can be seen this UDF can be invoked in three
different ways:

- with two int parameters returning a long result.
- with two long parameters returning a long result.
- with two double parameters returning a double result.

.. code:: java

    @UdfDescription(name = "multiply", description = "multiplies 2 numbers")
    public class Multiply {

      @Udf(description = "multiply two ints")
      public long multiply(final int v1, final int v2) {
        return v1 * v2;
      }

      @Udf(description = "multiply two longs")
      public long multiply(final long v1, final long v2) {
        return v1 * v2;
      }

      @Udf(description = "multiply two doubles")
      public double multiply(final double v1, double v2) {
        return v1 * v2;
      }

    }


UdfDescription Annotation
-------------------------
The ``@UdfDescription`` annotation is applied at the class level and has four fields, two of which are required:

+------------+------------------------------+---------+
| Field      | Description                  | Required|
+============+==============================+=========+
| name       | The name of the function(s)  | Y       |
|            | represented by this class.   |         |
+------------+------------------------------+---------+
| description| A string describing generally| Y       |
|            | what the function(s) in this |         |
|            | class do.                    |         |
+------------+------------------------------+---------+
| author     | The author of the UDF.       | N       |
+------------+------------------------------+---------+
| version    | The version of the UDF.      | N       |
+------------+------------------------------+---------+


Udf Annotation
--------------

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


Supported Types
---------------

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
| String       | VARCHAR          |
+--------------+------------------+
| List         | ARRAY            |
+--------------+------------------+
| Map          | MAP              |
+--------------+------------------+

Note: Complex types other than List and Map are not currently supported


=========
Deploying
=========

To deploy your UDFs you need to create a jar containing all of the classes required by the UDFs.
If you depend on third-party libraries then this should be an uber-jar containing those libraries.
Once the jar is created you need to deploy it to each KSQL server instance. The jar should be copied
to the ``ext/`` directory that is part of the KSQL distribution. The ``ext/`` directory can be configured
via the ``ksql.extension.dir``.

The jars in the ``ext`` directory are only scanned at start-up, so you will need to restart your
KSQL server instances to pick up new UDFs.


=====
Usage
=====

Once your UDFs are deployed you can call them in the same way you would invoke any of the KSQL
built-in functions, for example, using the ``multiply`` example above:

.. code:: sql

    CREATE STREAM number_stream (int1 INT, int2 INT, long1 BIGINT, long2 BIGINT)
      WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC = 'numbers');

    SELECT multiply(int1, int2), MULTIPLY(long1, long2) FROM number_stream;



============
Blacklisting
============

You can blacklist classes and packages such that they can't be used from a UDF. There is small
blacklist that is found in the file ``resource-blacklist.txt`` that is in the ``ext/`` directory.
This file contains an entry per line, where each line is a class or package that should be blacklisted.
The matching of the names is based on a regular expression, so if you have an entry, ``java.lang.Process``

.. code:: txt

    java.lang.Process

This would match any paths that begin with java.lang.Process, i.e., java.lang.Process, java.lang.ProcessBuilder etc.

Any blank lines or lines beginning with ``#`` are ignored. If the file is not present then all classes
are blacklisted.


Metric Collection
=================

Metric collection can be enabled by setting the config ``ksql.udf.collect.metrics`` to ``true``.
This defaults to ``false`` and is generally not recommended for production usage as metrics
will be collected on each invocation and will introduce some overhead to processing time.


=========
Disabling
=========

You can disable the loading of all UDFs in the ``ext`` directory by setting ``ksql.udfs.enabled`` to
``false``. By default they are enabled.
