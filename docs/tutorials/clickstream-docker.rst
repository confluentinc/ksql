.. _ksql_clickstream-docker:

Clickstream Data Analysis Pipeline Using KSQL (Docker)
======================================================

These steps will guide you through how to setup your environment and run the clickstream analysis tutorial from a Docker container.

**Prerequisites**

-  Docker must be installed and configured with at least 8 GB of memory.

   -  `macOS <https://docs.docker.com/docker-for-mac/install/>`__
   -  `All platforms <https://docs.docker.com/engine/installation/>`__

-  `Git <https://git-scm.com/downloads>`__


-----------------------------
Download and run the tutorial
-----------------------------

The tutorial is built using Docker Compose. It brings together several 
Docker images with the required networking and dependencies. The images
are quite large and depending on your network connection may take 
10-15 minutes to download.

#. Clone the Confluent KSQL repository.

   .. code:: bash

       $ git clone https://github.com/confluentinc/ksql.git
       $ cd ksql

#. Switch to the correct |cp| release branch:

   .. codewithvars:: bash
   
       $ git checkout |release_post_branch|

#. Navigate to the KSQL repository ``docs/tutorials/`` directory and launch the tutorial in
   Docker. Depending on your network speed, this may take up to 5-10 minutes.

   .. code:: bash

       $ cd ksql-clickstream-demo
       $ docker-compose up -d


#. After a minute or so, run the ``docker-compose ps`` status command to ensure that everything has started correctly: 

   .. code:: bash

        $ docker-compose ps
                        Name                                Command               State                Ports
        -------------------------------------------------------------------------------------------------------------------
        ksql-clickstream-demo_control-center_1    /etc/confluent/docker/run        Up      0.0.0.0:9021->9021/tcp
        ksql-clickstream-demo_datagen-ratings_1   bash -c echo Waiting for K ...   Up
        ksql-clickstream-demo_elasticsearch_1     /usr/local/bin/docker-entr ...   Up      0.0.0.0:9200->9200/tcp, 9300/tcp
        ksql-clickstream-demo_grafana_1           /run.sh                          Up      0.0.0.0:3000->3000/tcp
        ksql-clickstream-demo_kafka-connect_1     /etc/confluent/docker/run        Up      8083/tcp, 9092/tcp
        ksql-clickstream-demo_kafka_1             /etc/confluent/docker/run        Up      9092/tcp
        ksql-clickstream-demo_ksql-server_1       /etc/confluent/docker/run        Up      8088/tcp
        ksql-clickstream-demo_schema-registry_1   /etc/confluent/docker/run        Up      8081/tcp
        ksql-clickstream-demo_zookeeper_1         /etc/confluent/docker/run        Up      2181/tcp, 2888/tcp, 3888/tcp

---------------------------
Create the Clickstream Data
---------------------------

A data generator is already running, simulating the stream of clicks. You can sample this stream by 
using a console consumer such as ``kafkacat``: 

    .. code:: bash

        docker run --network ksql-clickstream-demo_default --tty --interactive --rm \
                confluentinc/cp-kafkacat \
                kafkacat -b kafka:29092 -C -c 10 -K: \
                -f '\nKey  : %k\t\nValue: %s\n' \
                -t clickstream

This will stop after ten messages, and your output should look like this: 

    .. code:: bash

        Key  : 111.90.225.227
        Value: {"ip":"111.90.225.227","userid":36,"remote_user":"-","time":"11/Sep/2018:09:53:04 +0000","_time":1536659584702,"request":"GET /images/track.png HTTP/1.1","status":"302","bytes":"2048","referrer":"-","agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"}

        Key  : 233.173.215.103
        Value: {"ip":"233.173.215.103","userid":15,"remote_user":"-","time":"11/Sep/2018:09:53:05 +0000","_time":1536659585434,"request":"GET /index.html HTTP/1.1","status":"406","bytes":"278","referrer":"-","agent":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
        [...]

If you remove the ``-c 10`` argument from the previous command you can run it and see a complete 
stream of all messages on the topic. If you do run this, press Ctrl-C to cancel it and 
return to the command prompt.

You now need to run two more to create fixed sets of data that will be 
used to enrich the click data.

#.  Create the status codes using the ksql-datagen utility. This stream runs once to populate the table.

    .. code:: bash

        $ docker run --network ksql-clickstream-demo_default --rm --name datagen-clickstream_codes \
            confluentinc/ksql-examples:5.0.0 \
            ksql-datagen \
                bootstrap-server=kafka:29092 \
                quickstart=clickstream_codes \
                format=json \
                topic=clickstream_codes \
                maxInterval=20 \
                iterations=100

    Your output should resemble:

    .. code:: bash

        200 --> ([ 200 | 'Successful' ])
        302 --> ([ 302 | 'Redirect' ])
        200 --> ([ 200 | 'Successful' ])
        406 --> ([ 406 | 'Not acceptable' ])
        ...

#.  Create a set of users using ksql-datagen utility. This stream runs once to populate the table.

    .. code:: bash

        $ docker run --network ksql-clickstream-demo_default \
                     --rm --name datagen-clickstream_codes \
            confluentinc/ksql-examples:5.0.0 \
            ksql-datagen \
                bootstrap-server=kafka:29092 \
                quickstart=clickstream_users \
                format=json \
                topic=clickstream_users \
                maxInterval=10 \
                iterations=1000

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

            $ docker run --network ksql-clickstream-demo_default \
                        --interactive --tty --rm \
                        --volume $PWD/demo:/usr/share/doc/ksql-clickstream-demo \
                confluentinc/cp-ksql-cli:latest \
                http://ksql-server:8088

       You should now be in the KSQL CLI.

        .. include:: ../includes/ksql-includes.rst
            :start-after: CLI_welcome_start
            :end-before: CLI_welcome_end

#.  Set up the environment for the demo to run: 

    .. code:: sql

        ksql> set 'commit.interval.ms'='2000';
        ksql> set 'cache.max.bytes.buffering'='10000000';
        ksql> set 'auto.offset.reset'='earliest';

#.  Load the ``clickstream.sql`` schema file that runs the tutorial app.

    **Important:** Before running this step, you must have already run
    ksql-datagen utility to create the clickstream data, status codes,
    and set of users.

    .. code:: bash

        ksql> RUN SCRIPT '/usr/share/doc/ksql-clickstream-demo/clickstream-schema.sql';

    The output will show either a blank message, or ``Executing statement``, similar to this: 

    .. code:: bash

         Message
        ---------
         Executing statement
        ---------

Verify the data
---------------

.. note::
    The following steps are optional and can be used to verify that the data was loaded properly. Otherwise, you can skip to :ref:`Load and View the Clickstream Data in Grafana <view-grafana-docker>`.

#.  Verify that the tables are created.

    .. code:: bash

        ksql> LIST TABLES;

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

        ksql> LIST STREAMS;

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

        1536662784214 | 111.168.57.122 | 1536662783614 | 11/Sep/2018:10:46:23 +0000 | 111.168.57.122 | GET /images/logo-small.png HTTP/1.1 | 200 | 35 | 1289 | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
        1536662784261 | 222.245.174.248 | 1536662784260 | 11/Sep/2018:10:46:24 +0000 | 222.245.174.248 | GET /index.html HTTP/1.1 | 404 | 7 | 14096 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
        1536662784335 | 111.90.225.227 | 1536662784335 | 11/Sep/2018:10:46:24 +0000 | 111.90.225.227 | GET /site/login.html HTTP/1.1 | 302 | 36 | 4096 | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
        1536662784351 | 233.245.174.248 | 1536662784351 | 11/Sep/2018:10:46:24 +0000 | 233.245.174.248 | GET /site/user_status.html HTTP/1.1 | 405 | 15 | 2048 | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
        1536662784421 | 222.168.57.122 | 1536662784421 | 11/Sep/2018:10:46:24 +0000 | 222.168.57.122 | GET /images/logo-small.png HTTP/1.1 | 302 | 28 | 14096 | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
        Limit Reached
        Query terminated
        ksql>

    **View the events per minute**

    .. code:: bash

        ksql> SELECT * FROM EVENTS_PER_MIN LIMIT 5;

    Your output should resemble:

    .. code:: bash

        1536662819576 | 24 : Window{start=1536662760000 end=-} | 24 | 12
        1536662819685 | 4 : Window{start=1536662760000 end=-} | 4 | 19
        1536662847582 | 4 : Window{start=1536662820000 end=-} | 4 | 75
        1536662847586 | 24 : Window{start=1536662820000 end=-} | 24 | 101
        1536662879959 | 29 : Window{start=1536662820000 end=-} | 29 | 2
        Limit Reached
        Query terminated

    **View pages per minute**

    .. code:: bash

        ksql> SELECT * FROM PAGES_PER_MIN LIMIT 5;

    Your output should resemble:

    .. code:: bash

        1536662784977 | 21 : Window{start=1536662725000 end=-} | 21 | 2
        1536662789353 | 21 : Window{start=1536662730000 end=-} | 21 | 7
        1536662793715 | 21 : Window{start=1536662735000 end=-} | 21 | 20
        1536662799627 | 21 : Window{start=1536662740000 end=-} | 21 | 35
        1536662804534 | 21 : Window{start=1536662745000 end=-} | 21 | 40
        Limit Reached
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

2. Set up the required Elasticsearch document mapping template

   .. code:: bash

       $ docker-compose exec elasticsearch bash -c '/scripts/elastic-dynamic-template.sh'

3. Run this command to send the KSQL tables to Elasticsearch and
   Grafana:

   .. code:: bash

       $ docker-compose exec kafka-connect bash -c '/scripts/ksql-tables-to-grafana.sh'

   Your output should resemble:

   .. code:: bash

        Loading Clickstream-Demo TABLES to Confluent-Connect => Elastic => Grafana datasource
        ==================================================================
        Charting  CLICK_USER_SESSIONS
        Charting  CLICK_USER_SESSIONS

        Remove any existing Elastic search config
        {"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index","resource.type":"index_or_alias","resource.id":"click_user_sessions","index_uuid":"_na_","index":"click_user_sessions"}],"type":"index_not_found_exception","reason":"no such index","resource.type":"index_or_alias","resource.id":"click_user_sessions","index_uuid":"_na_","index":"click_user_sessions"},"status":404}
        Remove any existing Connect config
        {"error_code":404,"message":"Connector es_sink_CLICK_USER_SESSIONS not found"}
        Remove any existing Grafana config
        {"message":"Failed to delete datasource"}
        Connecting KSQL->Elastic->Grafana  click_user_sessions
        Connecting: click_user_sessions
        Adding Kafka Connect Elastic Source es_sink_CLICK_USER_SESSIONS:


        {"name":"es_sink_CLICK_USER_SESSIONS","config":{"schema.ignore":"true","topics":"CLICK_USER_SESSIONS","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter.schemas.enable":"false","connector.class":"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector","key.ignore":"true","value.converter":"org.apache.kafka.connect.json.JsonConverter","type.name":"type.name=kafkaconnect","topic.index.map":"CLICK_USER_SESSIONS:click_user_sessions","connection.url":"http://elasticsearch:9200","transforms":"FilterNulls,ExtractTimestamp","transforms.FilterNulls.type":"io.confluent.transforms.NullFilter","transforms.ExtractTimestamp.type":"org.apache.kafka.connect.transforms.InsertField$Value","transforms.ExtractTimestamp.timestamp.field":"EVENT_TS","name":"es_sink_CLICK_USER_SESSIONS"},"tasks":[],"type":null}

        Adding Grafana Source
        {"datasource":{"id":1,"orgId":1,"name":"click_user_sessions","type":"elasticsearch","typeLogoUrl":"","access":"proxy","url":"http://elasticsearch:9200","password":"","user":"","database":"click_user_sessions","basicAuth":false,"basicAuthUser":"","basicAuthPassword":"","withCredentials":false,"isDefault":false,"jsonData":{"timeField":"EVENT_TS"},"secureJsonFields":{},"version":1,"readOnly":false},"id":1,"message":"Datasource added","name":"click_user_sessions"}==================================================================
        [...]

4. Load the dashboard into Grafana.

   .. code:: bash

       $ docker-compose exec grafana bash -c '/scripts/clickstream-analysis-dashboard.sh'

   Your output should resemble:

   .. code:: bash

        Loading Grafana ClickStream Dashboard
        {"id":1,"slug":"click-stream-analysis","status":"success","uid":"lUHTGDTmz","url":"/d/lUHTGDTmz/click-stream-analysis","version":4}


        Navigate to:
                http://localhost:3000/d/lUHTGDTmz/click-stream-analysis
        (Default user: admin / password: admin)

#.  Open your your browser using the URL output from the previous step's command.
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
