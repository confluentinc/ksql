.. _check-ksql-server-health:

Check the Health of a KSQL Server
#################################

Check a KSQL Server Running in a Native Deployment
**************************************************

If you installed KSQL server by using a package manager, like a DEB or RPM,
or from an archive, like a TAR or ZIP file, you can check the health of your
KSQL Server instances by using shell commands. 

Use the ``ps`` command to check whether the KSQL server process is running:

.. code:: bash

    ps -aux | grep ksql

Your output should resemble:

.. codewithvars:: bash

    jim       2540  5.2  2.3 8923244 387388 tty2   Sl   07:48   0:33 /usr/lib/jvm/java-8-oracle/bin/java -cp /home/jim/confluent-|release|/share/java/monitoring-interceptors/* ...

If the process status of the JVM isn't ``Sl`` or ``Ssl``, the KSQL server may be down.

Check runtime stats for the KSQL server that you're connected to.
  - Run ``ksql-print-metrics`` on a server host. The tool connects to a KSQL server
    that's running on ``localhost`` and collects JMX metrics from the server process.
    Metrics include the number of messages, the total throughput, the throughput
    distribution, and the error rate. For more information, see
    :ref:`ksql-monitoring-and-metrics`
  - In the KSQL CLI or in |c3|, run SHOW STREAMS or SHOW TABLES, then run
    DESCRIBE EXTENDED <stream|table>.
  - In the KSQL CLI or in |c3|, run SHOW QUERIES, then run EXPLAIN <query>.

Check a KSQL Server by using the REST API
*****************************************

The KSQL REST API supports a "server info" request, which you access with a URL
like ``http://<ksql-server-url>/info``. The ``/info`` endpoint returns the
KSQL Server version, the |ak-tm| cluster ID, and the service ID of the KSQL Server.
For more information, see :ref:`ksql-rest-api`.

.. code:: bash

   curl -sX GET "http://localhost:8088/info"

Your output should resemble:

.. codewithvars:: json

  {
      "KsqlServerInfo":{
          "version":"|release|",
          "kafkaClusterId":"X5ZV2fjQR1u4zQDLlw62PQ",
          "ksqlServiceId":"default_"
      }
  }

.. note::

   This approach doesn't work for non-interactive, or *headless*, deployments
   of KSQL Server, because a headless deployment doesn't have a REST API server.


KSQL Server Running in a Docker Container
*****************************************


If you're running KSQL server in a Docker container, run the ``docker ps`` or 
``docker-compose ps`` command, and check that the status of the ``ksql-server``
container is ``Up``. Check the health of the process in the container by running
``docker logs <ksql-server-container-id>``.
