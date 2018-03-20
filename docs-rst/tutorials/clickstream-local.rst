.. _ksql_clickstream-local:

Clickstream Data Analysis Pipeline Using KSQL (Local)
=====================================================

Clickstream analysis is the process of collecting, analyzing, and reporting aggregate data about the pages that are visited and
the in what order they are visited. The path the visitor takes though a website is called the clickstream.

This tutorial focuses on building real-time analytics of users to determine:

* General website analytics, such as hit count and visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

The tutorial uses standard streaming functions (i.e., min, max, etc), as well as enrichment using child tables, stream-table join and different
types of windowing functionality.

These steps will guide you through how to setup your environment and run the clickstream analysis tutorial.

**Prerequisites**

- :ref:`Confluent Platform <installation>` is installed and running. This installation includes a Kafka broker, KSQL, |c3-short|,
  |zk|, Schema Registry, REST Proxy, and Kafka Connect.
-  `ElasticSearch <https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html>`__
-  `Grafana <http://docs.grafana.org/installation/>`__
-  `Git <https://git-scm.com/downloads>`__
-  `Maven <https://maven.apache.org/install.html>`__
-  Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on
   your local machine

---------------------
Download the Tutorial
---------------------

Clone the KSQL GitHub repository. The tutorial is located in the ``ksql-clickstream-demo/`` folder.

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

#.  Start the Elastic and Grafana servers. ElasticSearch should be running on the default port 9200. Grafana
    should be running on the default port 3000.

    -  `Start Elastic <https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html>`__
    -  `Start Grafana <http://docs.grafana.org/installation/>`__


---------------------------
Create the Clickstream Data
---------------------------

#.  Create the clickStream data using the ``ksql-datagen`` utility. This stream will run continuously until you
    terminate.

    **Tip:** Because of shell redirection, this command does not print a new line and so it might look like it’s still
    in the foreground. The process is running as a daemon, so just press return again to see the shell prompt.

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-datagen -daemon quickstart=clickstream format=json \
        topic=clickstream maxInterval=100 iterations=500000

    Your output should resemble:

    .. code:: bash

        Writing console output to /tmp/ksql-logs/ksql.out

#.  Create the status codes using the ``ksql-datagen`` utility. This stream runs once to populate the table.

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-datagen  quickstart=clickstream_codes format=json \
        topic=clickstream_codes maxInterval=20 iterations=100

    Your output should resemble:

    .. code:: bash

        200 --> ([ 200 | 'Successful' ])
        302 --> ([ 302 | 'Redirect' ])
        200 --> ([ 200 | 'Successful' ])
        406 --> ([ 406 | 'Not acceptable' ])
        ...

#.  Create a set of users using ``ksql-datagen`` utility. This stream runs once to populate the table.

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-datagen quickstart=clickstream_users format=json topic=clickstream_users \
        maxInterval=10 iterations=1000

    Your output should resemble:

    .. code:: bash

        1 --> ([ 1 | 'GlenAlan_23344' | 1424796387808 | 'Curran' | 'Lalonde' | 'Palo Alto' | 'Gold' ])
        2 --> ([ 2 | 'ArlyneW8ter' | 1433932319457 | 'Oriana' | 'Vanyard' | 'London' | 'Platinum' ])
        3 --> ([ 3 | 'akatz1022' | 1478233258664 | 'Ferd' | 'Trice' | 'Palo Alto' | 'Platinum' ])
        ...

-------------------------------
Load the Streaming Data to KSQL
-------------------------------

#.  Launch the KSQL CLI in Client Server mode.

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties\
          > /tmp/ksql-logs/ksql-server.log 2>&1 &

    You should see the KSQL CLI welcome screen.

    .. include:: ../includes/ksql-includes.rst
        :start-line: 17
        :end-line: 38

#.  From the the KSQL CLI, load the ``clickstream.sql`` schema file that will run the tutorial app.

    .. code:: bash

        ksql> RUN SCRIPT 'ksql-clickstream-demo/demo/clickstream-schema.sql';

    The output should resemble:

    .. code:: bash

         Message
        ------------------------------------
         Executing statement

Verify the data
---------------

.. note::
        The following steps are optional and can be used to verify that the data was loaded properly. Otherwise, you can skip to :ref:`Load and View the Clickstream Data in Grafana <view-grafana>`.

#.  Verify that data is being streamed through various tables and streams.

    **Verify that the tables are created**

    .. code:: bash

        ksql> LIST TABLES;

    Your output should resemble:

    .. code:: bash

         Table Name                 | Kafka Topic                | Format | Windowed
        -----------------------------------------------------------------------------
         WEB_USERS                  | clickstream_users          | JSON   | false
         ERRORS_PER_MIN_ALERT       | ERRORS_PER_MIN_ALERT       | JSON   | true
         CLICKSTREAM_CODES_TS       | CLICKSTREAM_CODES_TS       | JSON   | false
         USER_IP_ACTIVITY           | USER_IP_ACTIVITY           | JSON   | true
         CLICKSTREAM_CODES          | clickstream_codes          | JSON   | false
         PAGES_PER_MIN              | PAGES_PER_MIN              | JSON   | true
         CLICK_USER_SESSIONS        | CLICK_USER_SESSIONS        | JSON   | true
         ENRICHED_ERROR_CODES_COUNT | ENRICHED_ERROR_CODES_COUNT | JSON   | true
         EVENTS_PER_MIN_MAX_AVG     | EVENTS_PER_MIN_MAX_AVG     | JSON   | true
         ERRORS_PER_MIN             | ERRORS_PER_MIN             | JSON   | true
         EVENTS_PER_MIN             | EVENTS_PER_MIN             | JSON   | true


    **Verify that the streams are created**

    .. code:: bash

        ksql> LIST STREAMS;

    Your output should resemble:

    .. code:: bash

         Stream Name               | Kafka Topic               | Format
        ----------------------------------------------------------------
         USER_CLICKSTREAM          | USER_CLICKSTREAM          | JSON
         EVENTS_PER_MIN_MAX_AVG_TS | EVENTS_PER_MIN_MAX_AVG_TS | JSON
         ERRORS_PER_MIN_TS         | ERRORS_PER_MIN_TS         | JSON
         EVENTS_PER_MIN_TS         | EVENTS_PER_MIN_TS         | JSON
         ENRICHED_ERROR_CODES      | ENRICHED_ERROR_CODES      | JSON
         ERRORS_PER_MIN_ALERT_TS   | ERRORS_PER_MIN_ALERT_TS   | JSON
         CLICK_USER_SESSIONS_TS    | CLICK_USER_SESSIONS_TS    | JSON
         PAGES_PER_MIN_TS          | PAGES_PER_MIN_TS          | JSON
         ENRICHED_ERROR_CODES_TS   | ENRICHED_ERROR_CODES_TS   | JSON
         USER_IP_ACTIVITY_TS       | USER_IP_ACTIVITY_TS       | JSON
         CUSTOMER_CLICKSTREAM      | CUSTOMER_CLICKSTREAM      | JSON
         CLICKSTREAM               | clickstream               | JSON


    **View clickstream data**

    .. code:: bash

        ksql> SELECT * FROM CLICKSTREAM LIMIT 5;

    Your output should resemble:

    .. code:: bash

        1503585407989 | 222.245.174.248 | 1503585407989 | 24/Aug/2017:07:36:47 -0700 | 233.90.225.227 | GET /site/login.html HTTP/1.1 | 407 | 19 | 4096 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
        1503585407999 | 233.168.257.122 | 1503585407999 | 24/Aug/2017:07:36:47 -0700 | 233.173.215.103 | GET /site/user_status.html HTTP/1.1 | 200 | 15 | 14096 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
        1503585408009 | 222.168.57.122 | 1503585408009 | 24/Aug/2017:07:36:48 -0700 | 111.249.79.93 | GET /images/track.png HTTP/1.1 | 406 | 22 | 4096 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
        1503585408019 | 122.145.8.244 | 1503585408019 | 24/Aug/2017:07:36:48 -0700 | 122.249.79.233 | GET /site/user_status.html HTTP/1.1 | 404 | 6 | 4006 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
        1503585408029 | 222.152.45.45 | 1503585408029 | 24/Aug/2017:07:36:48 -0700 | 222.249.79.93 | GET /images/track.png HTTP/1.1 | 200 | 29 | 14096 | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
        LIMIT reached for the partition.
        Query terminated

    **View the events per minute**

    .. code:: bash

        ksql> SELECT * FROM EVENTS_PER_MIN_TS LIMIT 5;

    Your output should resemble:

    .. code:: bash

        1503585450000 | 29 : | 1503585450000 | 29 | 19
        1503585450000 | 37 : | 1503585450000 | 37 | 25
        1503585450000 | 8 : | 1503585450000 | 8 | 35
        1503585450000 | 36 : | 1503585450000 | 36 | 14
        1503585450000 | 24 : | 1503585450000 | 24 | 22
        LIMIT reached for the partition.
        Query terminated

    **View pages per minute**

    .. code:: bash

        ksql> SELECT * FROM PAGES_PER_MIN LIMIT 5;

    Your output should resemble:

    .. code:: bash

        1503585475000 | 4 : Window{start=1503585475000 end=-} | 4 | 14
        1503585480000 | 25 : Window{start=1503585480000 end=-} | 25 | 9
        1503585480000 | 16 : Window{start=1503585480000 end=-} | 16 | 6
        1503585475000 | 25 : Window{start=1503585475000 end=-} | 25 | 20
        1503585480000 | 37 : Window{start=1503585480000 end=-} | 37 | 6
        LIMIT reached for the partition.
        Query terminated

.. _view-grafana:

---------------------------------------------
Load and View the Clickstream Data in Grafana
---------------------------------------------

In this step, you send the KSQL tables to Elasticsearch and Grafana and then view the Grafana output in your browser.

#. Navigate to the tutorial directory:

   .. code:: bash

       cd ksql-clickstream-demo/demo/

#. Run this command to send the KSQL tables to Elasticsearch and Grafana:

   .. code:: bash

       $ ./ksql-tables-to-grafana.sh

   Your output should resemble:

   .. code:: bash

       Loading Clickstream-Demo TABLES to Confluent-Connect => Elastic => Grafana datasource
       Logging to: /tmp/ksql-connect.log
       Charting  CLICK_USER_SESSIONS_TS
       Charting  USER_IP_ACTIVITY_TS
       Charting  CLICKSTREAM_STATUS_CODES_TS
       Charting  ENRICHED_ERROR_CODES_TS
       Charting  ERRORS_PER_MIN_ALERT_TS
       Charting  ERRORS_PER_MIN_TS
       Charting  EVENTS_PER_MIN_MAX_AVG_TS
       Charting  EVENTS_PER_MIN_TS
       Charting  PAGES_PER_MIN_TS
       Navigate to http://localhost:3000/dashboard/db/click-stream-analysis

   **Important:** The ``http://localhost:3000/`` URL is only
   available inside the container. We will access the dashboard with
   a slightly different URL, after running the next command.

#. Load the dashboard into Grafana.

   .. code:: bash

       $ ./clickstream-analysis-dashboard.sh

   Your output should resemble:

   .. code:: bash

       Loading Grafana ClickStream Dashboard
       {"slug":"click-stream-analysis","status":"success","version":1}

#.  Go to your browser and view the Grafana output at `http://localhost:3000/dashboard/db/click-stream-analysis <http://localhost:3000/dashboard/db/click-stream-analysis>`_. You can login with user ID ``admin`` and password ``admin``.

    **Important:** If you already have Grafana UI open, you may need to enter the specific clickstream URL as
    `http://localhost:3000/dashboard/db/click-stream-analysis <http://localhost:3000/dashboard/db/click-stream-analysis>`_.

    .. image:: ../img/grafana-success.png

This dashboard demonstrates a series of streaming functionality where the title of each panel describes the type of stream
processing required to generate the data. For example, the large chart in the middle is showing web-resource requests on a per-username basis
using a Session window - where a sessions expire after 300 seconds of inactivity. Editing the panel allows you to view the datasource - which
is named after the streams and tables captured in the ``clickstream-schema.sql`` file.

Things to try
    * Understand how the ``clickstream-schema.sql`` file is structured. We use a **DataGen.KafkaTopic.clickstream -> Stream -> Table** (for window &
      analytics with group-by) -> Table (to Add EVENT_TS for time-index) ->
      ElasticSearch/Connect topic
    * Run the KSQL CLI ``LIST TOPICS;`` command to see where data is persisted
    * Run the KSQL CLI ``history`` command

Troubleshooting
    -  Check that Elasticsearch is running: http://localhost:9200/.
    -  Check the Data Sources page in Grafana.

       -  If your data source is shown, select it and scroll to the bottom and click the **Save & Test** button. This will
          indicate whether your data source is valid.
       -  If your data source is not shown, go to ``<path-to-ksql>/demo/`` and run ``./ksql-tables-to-grafana.sh``.


