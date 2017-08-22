# Clickstream Analysis

### Introduction
Clickstream analysis is the process of collecting, analyzing and reporting aggregate data about which pages a website visitor visits -- and in what order. The path the visitor takes though a website is called the clickstream.

This application focuses on building real-time analytics of users to determine:
* General website analytics, such as hit count & visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

The application makes use of standard streaming functions (i.e. min, max, etc), as well as enrichment using child tables, table-stream joins and different types of windowing functionality.

#### Prerequisites
- [Confluent 3.3.0 installed](http://docs.confluent.io/current/installation.html) locally
- [ElasticSearch installed](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html)
- [Grafana installed](http://docs.grafana.org/installation/)
- [Git](https://git-scm.com/downloads)

1.  From your terminal, start the Confluent Platform. It should be running on default port 8083.

    ```
    ./bin/confluent start
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

1.  Clone the Confluent KSQL repository.

    ```bash
    git clone https://github.com/confluentinc/ksql
    ```

2.  Change directory to the root `ksql` directory.

    ```bash
    cd ksql
    ```

1.  From your terminal, create the clickStream data using the ksql-datagen utility. This stream will run continuously until you terminate.

    ```
    bin/ksql-datagen  quickstart=clickstream format=json topic=clickstream_1 maxInterval=1000 iterations=5000
    111.168.57.122 --> ([ '111.168.57.122' | 12 | '-' | '15/Aug/2017:10:53:45 +0100' | 1502790825640 | 'GET /site/user_status.html HTTP/1.1' | '404' | '1289' | '-' | 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36' ])
    111.90.225.227 --> ([ '111.90.225.227' | 5 | '-' | '15/Aug/2017:10:53:46 +0100' | 1502790826930 | 'GET /images/logo-small.png HTTP/1.1' | '302' | '4006' | '-' | 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36' ])
    222.145.8.144 --> ([ '222.145.8.144' | 18 | '-' | '15/Aug/2017:10:53:47 +0100' | 1502790827645 | 'GET /site/user_status.html HTTP/1.1' | '200' | '4006' | '-' | 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)' ])
    ```

1.  From your terminal, create the status codes using the ksql-datagen utility. This stream runs once to populate the table.

    ```
    bin/ksql-datagen  quickstart=clickstream_codes format=json topic=clickstream_codes_1 maxInterval=100 iterations=100
    ```

    Your output should resemble:

    ```
    200 --> ([ 200 | 'Successful' ])
    302 --> ([ 302 | 'Redirect' ])
    200 --> ([ 200 | 'Successful' ])
    406 --> ([ 406 | 'Not acceptable' ])
    ...
    ```

1.  From your terminal, create a set of users using ksql-datagen utility. This stream runs once to populate the table.

    ```
    bin/ksql-datagen  quickstart=clickstream_users format=json topic=clickstream_users maxInterval=10 iterations=20
    1 --> ([ 1 | 1427769490698 | 'Abdel' | 'Adicot' | 'Frankfurt' | 'Gold' ])
    2 --> ([ 2 | 1411540260097 | 'Ferd' | 'Trice' | 'Palo Alto' | 'Platinum' ])
    3 --> ([ 3 | 1462725158453 | 'Antonio' | 'Adicot' | 'San Francisco' | 'Platinum' ])
    ```

1.  Launch the KSQL CLI in local mode.

    ```
    ./bin/ksql-cli local
    ```

    You should see the KSQL CLI welcome screen.

    ```
                           ======================================
                           =      _  __ _____  ____  _          =
                           =     | |/ // ____|/ __ \| |         =
                           =     | ' /| (___ | |  | | |         =
                           =     |  <  \___ \| |  | | |         =
                           =     | . \ ____) | |__| | |____     =
                           =     |_|\_\_____/ \___\_\______|    =
                           =                                    =
                           = Streaming Query Language for Kafka =
    Copyright 2017 Confluent Inc.                         

    CLI v0.0.1, Server v0.0.1 located at http://localhost:9098

    Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

    ksql>
    ``` 

1.  From the the KSQL CLI, load the `clickstream.sql` schema file that will run the demo app.

    **Important:** Before running this step, you must have already run ksql-datagen utility to create the clickstream data, status codes, and set of users.

    ```
    ksql> run script 'ksql-examples/examples/clickstream-analysis/clickstream-schema.sql';
    ```

    The output should resemble:

    ```
     Message                            
    ------------------------------------
     Executing statement
    ksql>
    ```

1.  From the the KSQL CLI, verify that the tables are created.

    ```
    ksql> list TABLES;
    ```

    Your output should resemble:

    ```
     Table Name             | Kafka Topic            | Format | Windowed 
    ---------------------------------------------------------------------
     ERRORS_PER_MIN_ALERT   | ERRORS_PER_MIN_ALERT   | JSON   | true     
     CLICKSTREAM_CODES_TS   | CLICKSTREAM_CODES_TS   | JSON   | false    
     CLICKSTREAM_CODES      | clickstream_codes_1    | JSON   | false    
     PAGES_PER_MIN          | PAGES_PER_MIN          | JSON   | true     
     EVENTS_PER_MIN_MAX_AVG | EVENTS_PER_MIN_MAX_AVG | JSON   | true     
     ERRORS_PER_MIN         | ERRORS_PER_MIN         | JSON   | true     
     EVENTS_PER_MIN         | EVENTS_PER_MIN         | JSON   | true  
    ```

1.  From the the KSQL CLI, verify that the streams are created.

    ```
    ksql> list STREAMS;
    ```

    Your output should resemble:

    ```
     Stream Name               | Kafka Topic               | Format 
    ----------------------------------------------------------------
     EVENTS_PER_MIN_MAX_AVG_TS | EVENTS_PER_MIN_MAX_AVG_TS | JSON   
     ERRORS_PER_MIN_TS         | ERRORS_PER_MIN_TS         | JSON   
     EVENTS_PER_MIN_TS         | EVENTS_PER_MIN_TS         | JSON   
     ERRORS_PER_MIN_ALERT_TS   | ERRORS_PER_MIN_ALERT_TS   | JSON   
     PAGES_PER_MIN_TS          | PAGES_PER_MIN_TS          | JSON   
     ENRICHED_ERROR_CODES_TS   | ENRICHED_ERROR_CODES_TS   | JSON   
     CLICKSTREAM               | clickstream_1             | JSON   
    ```

1.  From the the KSQL CLI, verify that data is being streamed through various tables and streams. This will run continuously until you terminate (CTRL+C).

    ```
    ksql> select * from CLICKSTREAM;
    ```

    Your output should resemble:

    ```
    1502152008511 | 104.152.45.45 | 1502152008511 | 07/Aug/2017:17:26:48 -0700 | 104.152.45.45 | GET /index.html HTTP/1.1 | 404 | - | Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
    1502152008691 | 54.173.165.103 | 1502152008691 | 07/Aug/2017:17:26:48 -0700 | 54.173.165.103 | GET /index.html HTTP/1.1 | 406 | - | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
    1502152009077 | 66.249.79.93 | 1502152009077 | 07/Aug/2017:17:26:49 -0700 | 66.249.79.93 | GET /site/user_status.html HTTP/1.1 | 200 | - | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
    1502152009575 | 89.203.236.146 | 1502152009575 | 07/Aug/2017:17:26:49 -0700 | 89.203.236.146 | GET /site/user_status.html HTTP/1.1 | 302 | - | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
    1502152009679 | 172.245.174.248 | 1502152009679 | 07/Aug/2017:17:26:49 -0700 | 172.245.174.248 | GET /site/user_status.html HTTP/1.1 | 406 | - | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
    ^CQuery terminated
    ksql> select * from EVENTS_PER_MIN_TS;
    1502152015000 | -]�<�� | 1502152015000 | - | 13
    1502152020000 | -]�<�  | 1502152020000 | - | 6
    ^CQuery terminated
    ksql> select * from PAGES_PER_MIN;
    1502152040000 | - : Window{start=1502152040000 end=9223372036854775807} | - | 6
    1502152040000 | - : Window{start=1502152040000 end=9223372036854775807} | - | 7
    1502152045000 | - : Window{start=1502152045000 end=9223372036854775807} | - | 4
    1502152045000 | - : Window{start=1502152045000 end=9223372036854775807} | - | 5
    ^CQuery terminated
    ksql> 
    ```

1.  From your terminal, send the KSQL Tables=>Connect=>Elastic=>Grafana.

    1.  Navigate to the examples directory:

        ```
        cd ksql-examples/examples/clickstream-analysis/
        ```

    1.  Run this command to send the KSQL tables to Elasticsearch and Grafana:

        ```
        ./ksql-tables-to-grafana.sh
        ```

        Your output should resemble:

        ```
        Loading Clickstream-Demo TABLES to Confluent-Connect => Elastic => Grafana datasource
        Loading Elastic Dynamic Template to ensure _TS fields are used for TimeStamp
        {"acknowledged":true}{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index","resource.type":"index_or_alias","resource.id":"click_user_sessions_ts","index_uuid":"_na_","index":"click_user_sessions_ts"}],"type":"index_not_found_exception","reason":"no such index","resource.type":
        ...
        ```

1.  From your terminal, load the dashboard into Grafana.

    ```
    ./clickstream-analysis-dashboard.sh
    ```

    Your output should resemble:

    ```
    Loading Grafana ClickStream Dashboard
    {"slug":"click-stream-analysis","status":"success","version":1}
    ```

1.  Go to your browser and view the Grafana output at [http://localhost:3000/dashboard/db/click-stream-analysis](http://localhost:3000/dashboard/db/click-stream-analysis). You can login with user ID `admin` and password `admin`.

    <!-- Add screenshot -->

Interesting things to try:
* Understand how the `clickstream-schema.sql` file is structured. We use a DataGen.KafkaTopic.clickstream_1 -> Stream -> Table (for window & analytics with group-by) -> Table (to Add EVENT_TS for time-index) -> ElastiSearch/Connect topic  
* Try `list topics` to see where data is persisted
* Try `list tables`
* Try `list streams`
* Type: `history`
