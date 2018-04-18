.. _ksql_clickstream-docker:

Clickstream Data Analysis Pipeline Using KSQL (Docker)
======================================================

These steps will guide you through how to setup your environment and run the clickstream analysis tutorial from a Docker container.

**Prerequisites**

-  Docker is installed and configured with at least 4 GB of memory.

   -  `macOS <https://docs.docker.com/docker-for-mac/install/>`__
   -  `All platforms <https://docs.docker.com/engine/installation/>`__

-  `Git <https://git-scm.com/downloads>`__

---------------------
Download the Tutorial
---------------------

Download and start the KSQL clickstream container. This container
image is large and contains |cos|, Grafana, and Elasticsearch.
Depending on your network speed, this may take up to 10-15 minutes.
The ``-p`` flag will forward the Grafana dashboard to port 33000 on
your local host.

.. code:: bash

    $ docker run -p 33000:3000 -it confluentinc/ksql-clickstream-demo:4.1.0 bash

Your output should resemble:

.. code:: bash

    Unable to find image 'confluentinc/ksql-clickstream-demo:4.1.0' locally
    latest: Pulling from confluentinc/ksql-clickstream-demo
    ad74af05f5a2: Already exists
    d02e292e7b5e: Already exists
    8de7f5c81ab0: Already exists
    ed0b76dc2730: Already exists
    cfc44fa8a002: Already exists
    d9ece951ea0c: Pull complete
    f26010779356: Pull complete
    c9dad5440731: Pull complete
    935591799d9d: Pull complete
    696df0f65482: Pull complete
    14fd98e52325: Pull complete
    fcbeb94bace2: Pull complete
    32cca4f1567d: Pull complete
    5df0d25e7260: Pull complete
    e16097edc4fc: Pull complete
    72b33b348958: Pull complete
    015da01a41b0: Pull complete
    80e29f47abe0: Pull complete
    Digest: sha256:f3b2b19668b851d1300f77aa8c2236a126b628b911578cc688c7e0de442c1cd3
    Status: Downloaded newer image for confluentinc/ksql-clickstream-demo:latest
    $

You should now be in the Docker container and the remaining steps are run from within the container.

----------------------------------------------
Configure and Start Elastic, Grafana, and |cp|
----------------------------------------------

#.  Start  Elasticsearch.

    .. code:: bash

       $ /etc/init.d/elasticsearch start

    Your output should resemble:

    .. code:: bash

        [....] Starting Elasticsearch Server:sysctl: setting key "vm.max_map_count": Read-only file system
        . ok

#.  Start Grafana.

    .. code:: bash

        $ /etc/init.d/grafana-server start

    Your output should resemble:

    .. code:: bash

        [ ok ] Starting Grafana Server:.

#.  Start |cp|.

    .. code:: bash

        $ confluent start

    Your output should resemble:

    .. code:: bash

        Starting zookeeper
        zookeeper is [UP]
        Starting kafka
        kafka is [UP]
        Starting schema-registry
        schema-registry is [UP]
        Starting kafka-rest
        kafka-rest is [UP]
        Starting connect
        connect is [UP]
        Starting ksql-server
        ksql-server is [UP]

---------------------------
Create the Clickstream Data
---------------------------

#.  Create the clickStream data using the ksql-datagen utility. This stream will run continuously until you terminate.

    **Tip:** This command does not print a new line and so it might look like it’s still in the foreground. Because the
    process is running as a daemon, you can press return again to see the shell prompt.

    .. code:: bash

        $ ksql-datagen -daemon quickstart=clickstream format=json topic=clickstream maxInterval=100 iterations=500000

    Your output should resemble:

    .. code:: bash

        Writing console output to /tmp/ksql-logs/ksql.out

#.  Create the status codes using the ksql-datagen utility. This stream runs once to populate the table.

    .. code:: bash

        $ ksql-datagen quickstart=clickstream_codes format=json topic=clickstream_codes maxInterval=20 iterations=100

    Your output should resemble:

    .. code:: bash

        200 --> ([ 200 | 'Successful' ])
        302 --> ([ 302 | 'Redirect' ])
        200 --> ([ 200 | 'Successful' ])
        406 --> ([ 406 | 'Not acceptable' ])
        ...

#.  Create a set of users using ksql-datagen utility. This stream runs once to populate the table.

    .. code:: bash

        $ ksql-datagen quickstart=clickstream_users format=json topic=clickstream_users maxInterval=10 iterations=1000

    Your output should resemble:

    .. code:: bash

        1 --> ([ 1 | 'GlenAlan_23344' | 1424796387808 | 'Curran' | 'Lalonde' | 'Palo Alto' | 'Gold' ])
        2 --> ([ 2 | 'ArlyneW8ter' | 1433932319457 | 'Oriana' | 'Vanyard' | 'London' | 'Platinum' ])
        3 --> ([ 3 | 'akatz1022' | 1478233258664 | 'Ferd' | 'Trice' | 'Palo Alto' | 'Platinum' ])
        ...

-------------------------------
Load the Streaming Data to KSQL
-------------------------------

#.  Launch the KSQL CLI

       .. code:: bash

           $ ksql

       You should now be in the KSQL CLI.

       .. include:: ../includes/ksql-includes.rst
            :start-line: 19
            :end-line: 40

#.  Load the ``clickstream.sql`` schema file that runs the tutorial app.

    **Important:** Before running this step, you must have already run
    ksql-datagen utility to create the clickstream data, status codes,
    and set of users.

    .. code:: bash

        ksql> run script '/usr/share/doc/ksql-clickstream-demo/clickstream-schema.sql';

    The output should resemble:

    .. code:: bash

         Message
        ---------

        ---------

Verify the data
---------------

.. note::
    The following steps are optional and can be used to verify that the data was loaded properly. Otherwise, you can skip to :ref:`Load and View the Clickstream Data in Grafana <view-grafana-docker>`.

#.  Verify that the tables are created.

    .. code:: bash

        ksql> list TABLES;

    Your output should resemble:

    .. code:: bash

         Table Name                 | Kafka Topic                | Format | Windowed
        -----------------------------------------------------------------------------
         WEB_USERS                  | clickstream_users          | JSON   | false
         ERRORS_PER_MIN_ALERT       | ERRORS_PER_MIN_ALERT       | JSON   | true
         USER_IP_ACTIVITY           | USER_IP_ACTIVITY           | JSON   | true
         CLICKSTREAM_CODES          | clickstream_codes          | JSON   | false
         PAGES_PER_MIN              | PAGES_PER_MIN              | JSON   | true
         CLICK_USER_SESSIONS        | CLICK_USER_SESSIONS        | JSON   | true
         ENRICHED_ERROR_CODES_COUNT | ENRICHED_ERROR_CODES_COUNT | JSON   | true
         EVENTS_PER_MIN_MAX_AVG     | EVENTS_PER_MIN_MAX_AVG     | JSON   | true
         ERRORS_PER_MIN             | ERRORS_PER_MIN             | JSON   | true
         EVENTS_PER_MIN             | EVENTS_PER_MIN             | JSON   | true

#.  Verify that the streams are created.

    .. code:: bash

        ksql> list STREAMS;

    Your output should resemble:

    .. code:: bash

         Stream Name               | Kafka Topic               | Format
        ----------------------------------------------------------------
         USER_CLICKSTREAM          | USER_CLICKSTREAM          | JSON
         ENRICHED_ERROR_CODES      | ENRICHED_ERROR_CODES      | JSON
         CUSTOMER_CLICKSTREAM      | CUSTOMER_CLICKSTREAM      | JSON
         CLICKSTREAM               | clickstream               | JSON

#.  Verify that data is being streamed through
    various tables and streams.

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

        ksql> SELECT * FROM EVENTS_PER_MIN LIMIT 5;

    Your output should resemble:

    .. code:: bash

        1521108180000 | 6 : Window{start=1521108180000 end=-} | 6 | 24
        1521108180000 | 4 : Window{start=1521108180000 end=-} | 4 | 23
        1521108180000 | 35 : Window{start=1521108180000 end=-} | 35 | 20
        1521108180000 | 5 : Window{start=1521108180000 end=-} | 5 | 24
        1521108180000 | 9 : Window{start=1521108180000 end=-} | 9 | 19
        1521108180000 | 34 : Window{start=1521108180000 end=-} | 34 | 18
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

.. _view-grafana-docker:

---------------------------------------------
Load and View the Clickstream Data in Grafana
---------------------------------------------
Send the KSQL tables to Elasticsearch and Grafana.

1. Exit the KSQL CLI with ``CTRL+D``.

   .. code:: bash

        ksql>
        Exiting KSQL.

2. Navigate to the tutorial directory in the Docker container:

   .. code:: bash

       $ cd /usr/share/doc/ksql-clickstream-demo/

3. Run this command to send the KSQL tables to Elasticsearch and
   Grafana:

   .. code:: bash

       $ ./ksql-tables-to-grafana.sh

   Your output should resemble:

   .. code:: bash

       Loading Clickstream-Demo TABLES to Confluent-Connect => Elastic => Grafana datasource
       Logging to: /tmp/ksql-connect.log
       Charting  CLICK_USER_SESSIONS
       Charting  USER_IP_ACTIVITY
       Charting  CLICKSTREAM_STATUS_CODES
       Charting  ENRICHED_ERROR_CODES_COUNT
       Charting  ERRORS_PER_MIN_ALERT
       Charting  ERRORS_PER_MIN
       Charting  EVENTS_PER_MIN_MAX_AVG
       Charting  EVENTS_PER_MIN
       Charting  PAGES_PER_MIN
       Done

4. Load the dashboard into Grafana.

   .. code:: bash

       $ ./clickstream-analysis-dashboard.sh

   Your output should resemble:

   .. code:: bash

       Loading Grafana ClickStream Dashboard
       {"id":1,"slug":"click-stream-analysis","status":"success","uid":"VhmK8Mkik","url":"/d/VhmK8Mkik/click-stream-analysis","version":1}

       Navigate to:
          http://localhost:3000/d/VhmK8Mkik/click-stream-analysis (non-docker)
       or
          http://localhost:33000/d/VhmK8Mkik/click-stream-analysis (docker)

#.  Open your your browser using the second url output from the previous step's command.
    You can login with user ID ``admin`` and password ``admin``.

    **Important:** If you already have Grafana UI open, you may need to
    enter the specific clickstream URL output by the previous step

    .. image:: ../img/grafana-success.png
       :alt: Grafana UI success

This dashboard demonstrates a series of streaming functionality where the title of each panel describes the type of stream
processing required to generate the data. For example, the large chart in the middle is showing web-resource requests on a per-username basis
using a Session window - where a sessions expire after 300 seconds of inactivity. Editing the panel allows you to view the datasource - which
is named after the streams and tables captured in the ``clickstream-schema.sql`` file.


Things to try
    * Understand how the ``clickstream-schema.sql`` file is structured. We use a **DataGen.KafkaTopic.clickstream -> Stream -> Table** (for window &
      analytics with group-by) -> ElasticSearch/Connect topic
    * Run the KSQL CLI ``LIST TOPICS;`` command to see where data is persisted
    * Run the KSQL CLI ``history`` command

Troubleshooting
---------------

-  Check the Data Sources page in Grafana.

   -  If your data source is shown, select it and scroll to the bottom
      and click the **Save & Test** button. This will indicate whether
      your data source is valid.
