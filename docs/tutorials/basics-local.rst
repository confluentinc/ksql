.. _ksql_quickstart-local:

Writing Streaming Queries Against |ak-tm| Using KSQL (Local)
============================================================

This tutorial demonstrates a simple workflow using KSQL to write streaming queries against messages in Kafka.

To get started, you must start a Kafka cluster, including |zk| and a Kafka broker. KSQL will then query messages from
this Kafka cluster. KSQL is installed in the |cp| by default.

Watch the `screencast of Reading Kafka Data from KSQL <https://www.youtube.com/embed/EzVZOUt9JsU>`_ on YouTube.

**Prerequisites:**

- :ref:`Confluent Platform <installation>` is installed and running. This
  installation includes a Kafka broker, KSQL, |c3-short|, |zk|, |sr|, |crest|,
  and |kconnect|.
- If you installed |cp| via TAR or ZIP, navigate into the installation
  directory. The paths and commands used throughout this tutorial assume
  that you are in this installation directory.
- Consider :ref:`installing <cli-install>` the |confluent-cli| to start a local
  installation of |cp|.
- Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on your local machine

.. include:: ../includes/ksql-includes.rst
      :start-after: basics_tutorial_01_start
      :end-before: basics_tutorial_01_end

.. include:: ../includes/ksql-includes.rst
      :start-after: log_limitations_start
      :end-before: log_limitations_end

.. include:: ../includes/ksql-includes.rst
      :start-after: basics_tutorial_02_start
      :end-before: basics_tutorial_02_end

.. include:: ../includes/ksql-includes.rst
    :start-after: inspect_topics_start
    :end-before: inspect_topics_end

.. include:: ../includes/ksql-includes.rst
      :start-after: basics_tutorial_03_start
      :end-before: basics_tutorial_03_end

.. _struct-support-local: 

.. include:: ../includes/ksql-includes.rst
    :start-after: struct_support_01_start
    :end-before: struct_support_01_end

.. code:: bash

       $ <path-to-confluent>/bin/ksql-datagen  \
            quickstart=orders \
            format=avro \
            topic=orders 

.. include:: ../includes/ksql-includes.rst
    :start-after: struct_support_02_start
    :end-before: struct_support_02_end

.. _ss-joins-local: 

.. include:: ../includes/ksql-includes.rst
    :start-after: ss-join_01_start
    :end-before: ss-join_01_end

.. code:: bash

    $ <path-to-confluent>/bin/kafka-console-producer \
	  --broker-list localhost:9092 \
	  --topic new_orders \
	  --property "parse.key=true" \
	  --property "key.separator=:"<<EOF
    1:{"order_id":1,"total_amount":10.50,"customer_name":"Bob Smith"}
    2:{"order_id":2,"total_amount":3.32,"customer_name":"Sarah Black"}
    3:{"order_id":3,"total_amount":21.00,"customer_name":"Emma Turner"}
    EOF

    $ <path-to-confluent>/bin/kafka-console-producer \
	  --broker-list localhost:9092 \
	  --topic shipments \
	  --property "parse.key=true" \
	  --property "key.separator=:"<<EOF
    1:{"order_id":1,"shipment_id":42,"warehouse":"Nashville"}
    3:{"order_id":3,"shipment_id":43,"warehouse":"Palo Alto"}
    EOF

.. tip:: Note that you may see the following warning message when running the above statements—it can be safely ignored: 

      .. code:: bash

            Error while fetching metadata with correlation id 1 : {new_orders=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
            Error while fetching metadata with correlation id 1 : {shipments=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)

.. include:: ../includes/ksql-includes.rst
    :start-after: ss-join_02_start
    :end-before: ss-join_02_end


.. _tt-joins-local: 

.. include:: ../includes/ksql-includes.rst
    :start-after: tt-join_01_start
    :end-before: tt-join_01_end

.. code:: bash

    $ <path-to-confluent>/bin/kafka-console-producer \
	  --broker-list localhost:9092 \
	  --topic warehouse_location \
	  --property "parse.key=true" \
	  --property "key.separator=:"<<EOF

Your output should resemble:

::

    1:{"warehouse_id":1,"city":"Leeds","country":"UK"}
    2:{"warehouse_id":2,"city":"Sheffield","country":"UK"}
    3:{"warehouse_id":3,"city":"Berlin","country":"Germany"}
    EOF

.. code:: bash

    $ <path-to-confluent>/bin/kafka-console-producer \
	  --broker-list localhost:9092 \
	  --topic warehouse_size \
	  --property "parse.key=true" \
	  --property "key.separator=:"<<EOF

Your output should resemble:

::

    1:{"warehouse_id":1,"square_footage":16000}
    2:{"warehouse_id":2,"square_footage":42000}
    3:{"warehouse_id":3,"square_footage":94000}
    EOF

.. include:: ../includes/ksql-includes.rst
    :start-after: tt-join_02_start
    :end-before: tt-join_02_end

.. _insert-into-local: 

.. include:: ../includes/ksql-includes.rst
    :start-after: insert-into-01-start
    :end-before: insert-into-01-end

.. tip:: Each of these commands should be run in a separate window. When the exercise is finished, exit them by pressing Ctrl-C.

.. code:: bash

       $ <path-to-confluent>/bin/ksql-datagen \ 
            quickstart=orders \
            format=avro \
            topic=orders_local 

       $ <path-to-confluent>/bin/ksql-datagen \ 
            quickstart=orders \
            format=avro \
            topic=orders_3rdparty 

.. include:: ../includes/ksql-includes.rst
    :start-after: insert-into_02_start
    :end-before: insert-into_02_end

.. _terminate-local: 

.. include:: ../includes/ksql-includes.rst
      :start-after: terminate_and_exit__start
      :end-before: terminate_and_exit__end

Confluent CLI
--------------

If you are running |cp| using the CLI, you can stop it with this
command.

.. code:: bash

    $ <path-to-confluent>/bin/confluent local stop

