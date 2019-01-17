.. _check-ksql-server-health:

Check the Health of a KSQL Server
#################################

Use the ``ps`` command to check whether the KSQL server process is running:

.. code:: bash

    ps -aux | grep ksql

Your output should resemble:

.. code:: bash

    jim       2540  5.2  2.3 8923244 387388 tty2   Sl   07:48   0:33 /usr/lib/jvm/java-8-oracle/bin/java -cp /home/jim/confluent-5.0.0/share/java/monitoring-interceptors/* ...

If the process status of the JVM isn't ``Sl`` or ``Ssl``, the KSQL server may be down.

Check runtime stats for the KSQL server that you're connected to.
  - Run ``ksql-print-metrics`` on a server host. The tool connects to a KSQL server
    that's running on ``localhost`` and collects JMX metrics from the server process.
    Metrics include the number of messages, the total throughput, the throughput
    distribution, and the error rate. 
  - Run SHOW STREAMS or SHOW TABLES, then run DESCRIBE EXTENDED <stream|table>.
  - Run SHOW QUERIES, then run EXPLAIN <query>.

The KSQL REST API supports a "server info" request (for example, ``http://<ksql-server-url>/info``), 
which returns info such as the KSQL version. For more info, see :ref:`ksql-rest-api`.