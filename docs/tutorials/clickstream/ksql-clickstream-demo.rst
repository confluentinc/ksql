.. _ksql_clickstream:

Clickstream Analysis Tutorial
=============================

Clickstream analysis is the process of collecting, analyzing, and reporting aggregate data about the pages that are visited and
the in what order they are visited. The path the visitor takes though a website is called the clickstream.

This demo focuses on building real-time analytics of users to determine:

* General website analytics, such as hit count and visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

The demo uses standard streaming functions (i.e., min, max, etc), as well as enrichment using child tables, stream-table join and different
types of windowing functionality.

These steps will guide you through how to setup your environment and run the clickstream analysis demo.

**Prerequisites**

- :ref:`Confluent Platform <installation>` is installed and running. This installation includes a Kafka broker, KSQL, |c3-short|,
  ZooKeeper, Schema Registry, REST Proxy, and Kafka Connect.
-  `ElasticSearch <https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html>`__
-  `Grafana <http://docs.grafana.org/installation/>`__
-  `Git <https://git-scm.com/downloads>`__
-  `Maven <https://maven.apache.org/install.html>`__
-  Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on
   your local machine

-----------------
Download the Demo
-----------------

Clone the KSQL GitHub repository. The demo is located in the ``ksql-clickstream-demo/`` folder.

.. code:: bash

    $ git clone git@github.com:confluentinc/ksql.git

---------------------------------------
Configure and Start Elastic and Grafana
---------------------------------------

#.  Copy the Kafka Connect Elasticsearch configuration file (``ksql/ksql-clickstream-demo/demo/connect-config/null-filter-4.0.0-SNAPSHOT.jar``)
    to your |cp| installation ``share`` directory (``/share/java/kafka-connect-elasticsearch/``).

    .. code:: bash

        cp ksql-clickstream-demo/demo/connect-config/null-filter-4.0.0-SNAPSHOT.jar \
        <path-to-confluent>/share/java/kafka-connect-elasticsearch/

#.  From your terminal, start the Elastic and Grafana servers. ElasticSearch should be running on the default port 9200. Grafana
    should be running on the default port 3000.

    -  `Start Elastic <https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html>`__
    -  `Start Grafana <http://docs.grafana.org/installation/>`__

.. include:: ../../includes/ksql-includes.rst
    :start-line: 40
    :end-line: 319

Troubleshooting
    -  Check that Elasticsearch is running: http://localhost:9200/.
    -  Check the Data Sources page in Grafana.

       -  If your data source is shown, select it and scroll to the bottom and click the **Save & Test** button. This will
          indicate whether your data source is valid.
       -  If your data source is not shown, go to ``<path-to-ksql>/demo/`` and run ``./ksql-tables-to-grafana.sh``.


