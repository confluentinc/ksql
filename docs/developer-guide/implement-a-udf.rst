.. _implement-a-udf:

Implement a User-defined Function (UDF)
#######################################

Prerequisites
     - `Apache Maven <https://maven.apache.org/download.cgi>`__
     - `Git <https://git-scm.com/downloads>`__
     - Internet connectivity
     - `wget` to get the connector configuration file


Create the ext folder.

Copy ``ksql-udf-deep-learning-mqtt-iot-1.0-jar-with-dependencies.jar`` from the
``target`` folder to the ``ext`` folder of your KSQL installation 

If your |cp| installation is at /home/my.home.dir/confluent-|release|, copy the
JAR to /home/my.home.dir/confluent-|release|/etc/ksql/ext

Assign the ksql.extension.dir setting in etc/ksql/ksql-server.properties:

::

    ksql.extension.dir=/home/my.home.dir/confluent-|release|/etc/ksql/ext

