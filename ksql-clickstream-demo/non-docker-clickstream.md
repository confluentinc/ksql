# Clickstream Analysis
| [Overview](/docs#ksql-documentation) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  |
|---|----|-----|----|----|----|----|

These steps will guide you through how to setup your environment and run the clickstream analysis demo. For instructions using Docker, see [this documentation](/ksql-clickstream-demo/docker-clickstream.md).

#### Prerequisites
- [Confluent 3.3.0](http://docs.confluent.io/current/installation.html) locally
    - Make sure you only have one broker running on the host
- [ElasticSearch](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html)
- [Grafana](http://docs.grafana.org/installation/)
- [Git](https://git-scm.com/downloads)
- [Maven](https://maven.apache.org/install.html)
- Java: Minimum version 1.8. Install Oracle Java JRE or JDK \>= 1.8 on your local machine

1.  Clone the Confluent KSQL repository.

    ```bash
    $ git clone https://github.com/confluentinc/ksql.git
    ```

1.  Change directory to the `ksql` directory and compile the KSQL code.

    ```bash
    $ cd ksql
    $ git checkout v0.5 -b 0.5
    $ mvn clean compile install -DskipTests
    ```

1.  Copy the Kafka Connect Elasticsearch configuration file (`ksql/ksql-clickstream-demo/demo/connect-config/null-filter-4.0.0-SNAPSHOT.jar`) to your Confluent installation `share` directory (`confluent-3.3.0/share/java/kafka-connect-elasticsearch/`).

    ```bash
    cp ksql-clickstream-demo/demo/connect-config/null-filter-4.0.0-SNAPSHOT.jar <path-to-confluent-3.3.0>/share/java/kafka-connect-elasticsearch/
    ```

1.  From your terminal, start the Confluent Platform. It should be running on default port 8083.

    ```bash
    $ <path-to-confluent-3.3.0>/bin/confluent start
    ```

    The output should resemble:

    ```bash
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
    ```

1.  From your terminal, start the Elastic and Grafana servers. ElasticSearch should be running on the default port 9200. Grafana should be running on the default port 3000. <!-- http://docs.grafana.org/installation/ -->

    - [Start Elastic](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html)
    - [Start Grafana](http://docs.grafana.org/installation/)

1.  From your terminal, create the clickStream data using the ksql-datagen utility. This stream will run continuously until you terminate.

    **Tip:** Because of shell redirection, this command does not print a newline and so it might look like it's still in the foreground. The process is running as a daemon, so just press return again to see the shell prompt.

    ```bash
    $ <path-to-ksql>/bin/ksql-datagen  -daemon quickstart=clickstream format=json topic=clickstream maxInterval=100 iterations=500000
    ```

    Your output should resemble:

    ```bash
    Writing console output to /tmp/ksql-logs/ksql.out
    ```

1.  From your terminal, create the status codes using the ksql-datagen utility. This stream runs once to populate the table.

    ```bash
    $ <path-to-ksql>/bin/ksql-datagen  quickstart=clickstream_codes format=json topic=clickstream_codes maxInterval=20 iterations=100
    ```

    Your output should resemble:

    ```bash
    200 --> ([ 200 | 'Successful' ])
    302 --> ([ 302 | 'Redirect' ])
    200 --> ([ 200 | 'Successful' ])
    406 --> ([ 406 | 'Not acceptable' ])
    ...
    ```

1.  From your terminal, create a set of users using ksql-datagen utility. This stream runs once to populate the table.

    ```bash
    $ <path-to-ksql>/bin/ksql-datagen  quickstart=clickstream_users format=json topic=clickstream_users maxInterval=10 iterations=1000
    ```

    Your output should resemble:

    ```bash
    1 --> ([ 1 | 'GlenAlan_23344' | 1424796387808 | 'Curran' | 'Lalonde' | 'Palo Alto' | 'Gold' ])
    2 --> ([ 2 | 'ArlyneW8ter' | 1433932319457 | 'Oriana' | 'Vanyard' | 'London' | 'Platinum' ])
    3 --> ([ 3 | 'akatz1022' | 1478233258664 | 'Ferd' | 'Trice' | 'Palo Alto' | 'Platinum' ])
    ...
    ```

1.  Launch the KSQL CLI in local mode.

    ```bash
    $ <path-to-ksql>/bin/ksql-cli local
    ```

    You should see the KSQL CLI welcome screen.

    ```bash
                       ======================================
                       =      _  __ _____  ____  _          =
                       =     | |/ // ____|/ __ \| |         =
                       =     | ' /| (___ | |  | | |         =
                       =     |  <  \___ \| |  | | |         =
                       =     | . \ ____) | |__| | |____     =
                       =     |_|\_\_____/ \___\_\______|    =
                       =                                    =
                       =   Streaming SQL Engine for Kafka   =
    Copyright 2017 Confluent Inc.                         

    CLI v0.1, Server v0.1 located at http://localhost:9098

    Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

    ksql>
    ``` 

1.  From the the KSQL CLI, load the `clickstream.sql` schema file that will run the demo app.

    **Important:** Before running this step, you must have already run ksql-datagen utility to create the clickstream data, status codes, and set of users.


    ```
    ksql> run script 'ksql-clickstream-demo/demo/clickstream-schema.sql';
    ```

    The output should resemble:

    ```bash
     Message                            
    ------------------------------------
     Executing statement
    ksql>
    ```

1.  From the the KSQL CLI, verify that the tables are created.

    ```bash
    ksql> list TABLES;
    ```

    Your output should resemble:

    ```bash
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
    ```

1.  From the the KSQL CLI, verify that the streams are created.

    ```bash
    ksql> list STREAMS;
    ```

    Your output should resemble:

    ```bash
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
    ```

1.  From the the KSQL CLI, verify that data is being streamed through various tables and streams. 

    **View clickstream data**

    ```bash
    ksql> SELECT * FROM CLICKSTREAM LIMIT 5;
    ```

    Your output should resemble:

    ```bash
    1503585407989 | 222.245.174.248 | 1503585407989 | 24/Aug/2017:07:36:47 -0700 | 233.90.225.227 | GET /site/login.html HTTP/1.1 | 407 | 19 | 4096 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
    1503585407999 | 233.168.257.122 | 1503585407999 | 24/Aug/2017:07:36:47 -0700 | 233.173.215.103 | GET /site/user_status.html HTTP/1.1 | 200 | 15 | 14096 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
    1503585408009 | 222.168.57.122 | 1503585408009 | 24/Aug/2017:07:36:48 -0700 | 111.249.79.93 | GET /images/track.png HTTP/1.1 | 406 | 22 | 4096 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
    1503585408019 | 122.145.8.244 | 1503585408019 | 24/Aug/2017:07:36:48 -0700 | 122.249.79.233 | GET /site/user_status.html HTTP/1.1 | 404 | 6 | 4006 | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
    1503585408029 | 222.152.45.45 | 1503585408029 | 24/Aug/2017:07:36:48 -0700 | 222.249.79.93 | GET /images/track.png HTTP/1.1 | 200 | 29 | 14096 | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
    LIMIT reached for the partition.
    Query terminated
    ```
    
    **View the events per minute**

    ```bash
    ksql> SELECT * FROM EVENTS_PER_MIN_TS LIMIT 5;
    ```

    Your output should resemble:

    ```bash
    1503585450000 | 29^�8 | 1503585450000 | 29 | 19
    1503585450000 | 37^�8 | 1503585450000 | 37 | 25
    1503585450000 | 8^�8 | 1503585450000 | 8 | 35
    1503585450000 | 36^�8 | 1503585450000 | 36 | 14
    1503585450000 | 24^�8 | 1503585450000 | 24 | 22
    LIMIT reached for the partition.
    Query terminated
    ```

    **View pages per minute**

    ```bash
    ksql> SELECT * FROM PAGES_PER_MIN LIMIT 5;
    ```

    Your output should resemble:

    ```bash
    1503585475000 | 4 : Window{start=1503585475000 end=-} | 4 | 14
    1503585480000 | 25 : Window{start=1503585480000 end=-} | 25 | 9
    1503585480000 | 16 : Window{start=1503585480000 end=-} | 16 | 6
    1503585475000 | 25 : Window{start=1503585475000 end=-} | 25 | 20
    1503585480000 | 37 : Window{start=1503585480000 end=-} | 37 | 6
    LIMIT reached for the partition.
    Query terminated    
    ```

1.  Go to your terminal and send the KSQL tables to Elasticsearch and Grafana.

    1.  From your terminal, navigate to the demo directory:

        ```
        cd ksql-clickstream-demo/demo/
        ```

    1.  Run this command to send the KSQL tables to Elasticsearch and Grafana:

        ```bash
        $ ./ksql-tables-to-grafana.sh
        ```

        Your output should resemble:

        ```bash
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
        ```

        **Important:** The `http://localhost:3000/` URL is only available inside the container. We will access the dashboard with a slightly different URL, after running the next command.

    1.  From your terminal, load the dashboard into Grafana.

        ```bash
        $ ./clickstream-analysis-dashboard.sh
        ```

        Your output should resemble:

        ```bash
        Loading Grafana ClickStream Dashboard
        {"slug":"click-stream-analysis","status":"success","version":1}
        ``` 

1.  Go to your browser and view the Grafana output at [http://localhost:3000/dashboard/db/click-stream-analysis](http://localhost:3000/dashboard/db/click-stream-analysis). You can login with user ID `admin` and password `admin`.

    **Important:** If you already have Grafana UI open, you may need to enter the specific clickstream URL: [http://localhost:3000/dashboard/db/click-stream-analysis](http://localhost:3000/dashboard/db/click-stream-analysis).

    ![Grafana UI success](grafana-success.png)

Interesting things to try:
* Understand how the `clickstream-schema.sql` file is structured. We use a DataGen.KafkaTopic.clickstream -> Stream -> Table (for window & analytics with group-by) -> Table (to Add EVENT_TS for time-index) -> ElasticSearch/Connect topic  
* Run the `LIST TOPICS;` command to see where data is persisted
* Run the KSQL CLI `history` command

### Troubleshooting

- Docker must not be running on the host machine.
- Check that Elasticsearch is running: http://localhost:9200/.
- Check the Data Sources page in Grafana.
    - If your data source is shown, select it and scroll to the bottom and click the **Save & Test** button. This will indicate whether your data source is valid.
    - If your data source is not shown, go to `/ksql/ksql-clickstream-demo/demo/` and run `./ksql-tables-to-grafana.sh`.
