.. _implement-a-udf:

Implement a User-defined Function (UDF and UDAF)
################################################

Prerequisites
     - `Apache Maven <https://maven.apache.org/download.cgi>`__
     - |cp| :ref:`installed <installation>` locally
     - Internet connectivity for downloading Confluent POM files

Create a user-defined function (UDF) or a user-defined aggregation function
(UDAF) by following these steps:

#. :ref:`Create the KSQL extensions directory <create-ksql-ext-dir>` that
   contains your UDF and UDAF packages.
#. :ref:`Create Java source and project files <create-source-and-project-files>`
   for your implementation.
#. :ref:`Build the package <build-udf-package>` for your UDF or UDAF.
#. :ref:`Use your custom function <use-udf-in-ksql-query>` in a KSQL query or
   statement.

For more information on custom functions, see :ref:`ksql-udfs`.

.. _create-ksql-ext-dir:

Create the KSQL Extensions Directory
************************************

When you create a custom user-defined function (UDF), you implement it in Java
and deploy it as a JAR to the KSQL extensions directory. By default, this 
directory doesn't exist, so you need to create it and assign it in the KSQL
Server configuration properties.

Create the KSQL extensions directory, ``<path-to-confluent>/etc/ksql/ext``:

.. codewithvars:: bash

    mkdir confluent-|release|/etc/ksql/ext

Edit the ``ksql-server.properties`` configuration file in
``<path-to-confluent>/etc/ksql`` to add the fully qualified path to the
``ext`` directory:

.. codewithvars:: text

    ksql.extension.dir=/home/my-home-dir/confluent-|release|/etc/ksql/ext

.. note::

    Use the fully qualified path or the relative path from
    ``<path-to-confluent>/bin``, which is ``../etc/ksql/ext``.
    KSQL Server won't load extensions if the path begins with ``~``.

.. _create-source-and-project-files:

Create the Source and Project Files
***********************************

The following steps shows how to implement your UDF in a Java class and build
it by defining a Maven POM file.

#. :ref:`Create a root directory <create-root-source-dir>` for your UDF's
   source code and project files.
#. :ref:`Create the source code directory <create-source-code-dir>`, which has
   a path that corresponds with the package name.
#. :ref:`Create the Java source code file <create-java-source-code-file>` in
   the source code directory. 
#. :ref:`Create a Project Object Model (POM) file <create-pom-file>` that defines how Maven builds the
   source code.

.. _create-root-source-dir:

Create a Project Root Directory
===============================

Create the directory that holds your UDF or UDAF project:

.. code:: bash

    mkdir ksql-udf-demo && cd ksql-udf-demo

.. _create-source-code-dir:

Create the Source Code Directory
================================

From the root directory for your UDF, create the source code directory. In this
example, the package name is ``my.company.ksql.udfdemo``.

.. code:: bash

    mkdir -p src/main/java/my/company/ksql/udfdemo

.. _create-java-source-code-file:

Create the Java Source Code File
================================

The following Java code defines four overloads for a ``multiply`` function.
The ``UdfDescription`` and ``Udf`` annotations tell KSQL Server to load the
``Multiply`` class and look for methods to add to its list of available
functions. For more information, see :ref:`ksql-udfs`.

Copy the following code into a new file, named ``Multiply.java``:

.. code:: java

    package my.company.ksql.udfdemo;

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

Save the file to the source code directory that you created in the previous
step, ``src/main/java/my/company/ksql/udfdemo``.

.. _create-pom-file:

Create the POM File
===================

In the root directory for your custom UDF implementation, create the Project
Object Model (POM) file for the Maven build, and name it ``pom.xml``:

.. codewithvars:: xml

    <?xml version="1.0" encoding="UTF-8"?>

    <project xmlns="http://maven.apache.org/POM/4.0.0"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <!-- Specify the package details for the custom UDF -->
        <groupId>my.company.ksql.udfdemo</groupId>
        <artifactId>ksql-udf-demo</artifactId>
        <version>1.0</version>

        <!-- Specify the repository for Confluent dependencies -->
        <repositories>
            <repository>
                <id>confluent</id>
                <url>http://packages.confluent.io/maven/</url>
            </repository>
        </repositories>

        <!-- Specify build properties -->
        <properties>
            <exec.mainClass>my.company.ksql.udfdemo.thisisignored</exec.mainClass>
            <java.version>1.8</java.version>
            <kafka.version>2.0.0</kafka.version>
            <kafka.scala.version>2.11</kafka.scala.version>
            <scala.version>${kafka.scala.version}.8</scala.version>
            <confluent.version>5.1.0</confluent.version>
            <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        </properties>

        <!-- Specify the ksql-udf dependency -->
        <dependencies>
            <!-- KSQL dependency is needed to write your own UDF -->
            <dependency>
                <groupId>io.confluent.ksql</groupId>
                <artifactId>ksql-udf</artifactId>
                <version>|release|</version>
            </dependency>
        </dependencies>

        <!-- Build boilerplate -->
        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.6.1</version>
                    <configuration>
                        <source>1.8</source>
                        <target>1.8</target>
                    </configuration>
                </plugin>

                <!-- Package all dependencies as one jar -->
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <version>2.5.2</version>
                    <configuration>
                        <descriptorRefs>
                            <descriptorRef>jar-with-dependencies</descriptorRef>
                        </descriptorRefs>
                        <archive>
                            <manifest>
                                <addClasspath>true</addClasspath>
                                <mainClass>${exec.mainClass}</mainClass>
                            </manifest>
                        </archive>
                    </configuration>
                    <executions>
                        <execution>
                            <id>assemble-all</id>
                            <phase>package</phase>
                            <goals>
                                <goal>single</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    </project>

.. important::

    For production environments, we strongly recommend that you write
    comprehensive tests to cover your custom functions.

.. _build-udf-package:

Build the UDF Package
*********************

Use Maven to build the package and create a JAR. Copy the JAR to the KSQL 
extensions directory.

In the root folder for your UDF, run Maven to build the package:

.. code:: bash

    mvn clean package

After a great deal of build info, your output should resemble:

::

    ...
    [INFO] --- maven-assembly-plugin:2.5.2:single (assemble-all) @ ksql-udf-demo ---
    [INFO] Building jar: /home/my-home-dir/ksql-udf-demo/target/ksql-udf-demo-1.0-jar-with-dependencies.jar
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 17.511 s
    [INFO] Finished at: 2018-12-17T22:07:08Z
    [INFO] Final Memory: 26M/280M
    [INFO] ------------------------------------------------------------------------

The Maven build creates a directory named ``target`` and saves the build output
there. Copy the JAR file, ``ksql-udf-demo-1.0-jar-with-dependencies.jar``, from
the ``target`` directory to the ``ext`` directory of your KSQL installation. 
For example, if your |cp| installation is at :litwithvars:`/home/my-home-dir/confluent-|release|`,
copy the JAR to :litwithvars:`/home/my-home-dir/confluent-|release|/etc/ksql/ext`.

.. code:: bash

    cp target/ksql-udf-demo-1.0-jar-with-dependencies.jar <path-to-confluent>/etc/ksql/ext

The custom UDF is deployed and ready to run.

.. _use-udf-in-ksql-query:

Use Your Custom UDF in a KSQL Query
***********************************

When your custom UDF is deployed in the KSQL extensions directory, it's loaded
automatically when you start KSQL Server, and you can use it like you use the
other KSQL functions.

.. note::

    KSQL loads UDFs and UDAFs only on startup, so when you make changes to your
    UDF code and re-deploy the JAR, you must restart KSQL Server to get the
    latest version of your UDF. 

Start |cp| and KSQL Server:

.. code:: bash

    <path-to-confluent>/bin/confluent start ksql-server

Start the KSQL CLI:

.. code:: bash

    LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql

In the KSQL CLI, list the available functions to ensure that KSQL Server
loaded the MULTIPLY user-defined function:

::

    LIST FUNCTIONS;

Your output should resemble:

::

     Function Name     | Type
    -------------------------------
     ABS               | SCALAR
     ARRAYCONTAINS     | SCALAR
     ...               |
     MULTIPLY          | SCALAR
     ...               |
     SUBSTRING         | SCALAR    
     SUM               | AGGREGATE 
     ...               |
    -------------------------------

Inspect the details of the MULTIPLY function:

::

    DESCRIBE FUNCTION MULTIPLY;

Your output should resemble:

.. codewithvars:: text

    Name        : MULTIPLY
    Overview    : multiplies 2 numbers
    Type        : scalar
    Jar         : /home/my-home-dir/confluent-|release||/etc/ksql/ext/ksql-udf-demo-1.0-jar-with-dependencies.jar
    Variations  : 

    	Variation   : MULTIPLY(BIGINT, BIGINT)
    	Returns     : BIGINT
    	Description : multiply two nullable BIGINTs. If either param is null, null is 
                    returned.

    	Variation   : MULTIPLY(DOUBLE, DOUBLE)
    	Returns     : DOUBLE
    	Description : multiply two non-nullable DOUBLEs.

    	Variation   : MULTIPLY(INT, INT)
    	Returns     : BIGINT
    	Description : multiply two non-nullable INTs.

Use the MULTIPLY function in a query. If you follow the steps in
:ref:`ksql_quickstart-local`, you can multiply the two BIGINT fields in the
``pageviews_original`` stream:

::

    SELECT MULTIPLY(rowtime, viewtime) FROM pageviews_original;

Your output should resemble:

::

    2027398056717155428
    2028560009956135428
    2029465468198408945
    2030608879630876785
    2031171314443704673
    2032147849613387385
    2032926605508340785
    ^CQuery terminated

Press Ctrl+C to terminate the query.

Custom Aggregation Function (UDAF)
**********************************

Implementing a user-defined aggregation function (UDAF) is similar to the way
that you implement a UDF. You use the ``UdafDescription`` and ``UdafFactory``
annotations in your Java code, and you deploy a JAR to the KSQL extensions
directory. For more information, see :ref:`ksql-udafs`.

Next Steps
**********

* `How to Build a UDF and/or UDAF in KSQL 5.0 <https://www.confluent.io/blog/build-udf-udaf-ksql-5-0>`__
* :ref:`aggregate-streaming-data-with-ksql`
* :ref:`join-streams-and-tables`