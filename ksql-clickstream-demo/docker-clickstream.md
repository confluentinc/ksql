# Clickstream Analysis using Docker

| [Overview](/docs#ksql-documentation) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  |
|---|----|-----|----|----|----|----|

These steps will guide you through how to setup your environment and run the clickstream analysis demo from a Docker container. For instructions without using Docker, see [this documentation](/ksql-clickstream-demo/non-docker-clickstream.md).

#### Prerequisites
- Docker 
    - [macOS](https://docs.docker.com/docker-for-mac/install/)
    - [All platforms](https://docs.docker.com/engine/installation/)
- [Git](https://git-scm.com/downloads)

1.  Please ensure that your Docker installation is configured with at least 4 GB of memory.

1.  Download and start the KSQL clickstream container. This container image is large and contains Confluent, Grafana, and Elasticsearch. Depending on your network speed, this may take up to 10-15 minutes. The `-p` flag will forward the Grafana dashboard to port 33000 on your local host.

	```bash
	$ docker run -p 33000:3000 -it confluentinc/ksql-clickstream-demo:0.5 bash
	```

	Your output should resemble:

	```bash
	Unable to find image 'confluentinc/ksql-clickstream-demo:0.5' locally
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
	root@d98186dd8d6c:/#
	```

	You should now be in the Docker container.

1. Start the container services.

	- Elasticsearch

	  ```bash
	  $ /etc/init.d/elasticsearch start
	  ```

	- Grafana

	  ```bash
	  $ /etc/init.d/grafana-server start
	  ```

	- Confluent Platform

	  ```bash
	  $ confluent start
      ```

1.  From your terminal, create the clickStream data using the ksql-datagen utility. This stream will run continuously until you terminate.

    **Tip:** Because of shell redirection, this command does not print a newline and so it might look like it's still in the foreground. The process is running as a daemon, so just press return again to see the shell prompt.

    ```bash
    $ ksql-datagen -daemon quickstart=clickstream format=json topic=clickstream maxInterval=100 iterations=500000
    ```

    Your output should resemble:

    ```
    Writing console output to /tmp/ksql-logs/ksql.out
    ```

1.  From your terminal, create the status codes using the ksql-datagen utility. This stream runs once to populate the table. 

    ```bash
    $ ksql-datagen quickstart=clickstream_codes format=json topic=clickstream_codes maxInterval=20 iterations=100
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

    ```bash
    $ ksql-datagen quickstart=clickstream_users format=json topic=clickstream_users maxInterval=10 iterations=1000
    ```

    Your output should resemble:

    ```
    1 --> ([ 1 | 'GlenAlan_23344' | 1424796387808 | 'Curran' | 'Lalonde' | 'Palo Alto' | 'Gold' ])
    2 --> ([ 2 | 'ArlyneW8ter' | 1433932319457 | 'Oriana' | 'Vanyard' | 'London' | 'Platinum' ])
    3 --> ([ 3 | 'akatz1022' | 1478233258664 | 'Ferd' | 'Trice' | 'Palo Alto' | 'Platinum' ])
    ...
    ```

1.  Launch the KSQL CLI in local mode.

    1.  Start the KSQL server.

    	```bash
    	$ ksql-server-start /etc/ksql/ksqlserver.properties > /tmp/ksql-logs/ksql-server.log 2>&1 &
    	```

    1.  Start the CLI on port 8080.

    	```bash
    	$ ksql remote http://localhost:8080
    	```

    	You should now be in the KSQL CLI.

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
    ksql> run script '/usr/share/doc/ksql-clickstream-demo/clickstream-schema.sql';
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

    ```
    ksql> list STREAMS;
    ```

    Your output should resemble:

    ```
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

	1.  Exit the KSQL CLI with `CTRL+D`.

	1.  From your terminal, navigate to the demo directory:

		```bash
		$ cd /usr/share/doc/ksql-clickstream-demo/
		```

    1.  Run this command to send the KSQL tables to Elasticsearch and Grafana:

		```bash
		$ ./ksql-tables-to-grafana.sh
		```

        Your output should resemble:

        ```
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

    1.  From your terminal, load the dashboard into Grafana.

    	```bash
		$ ./clickstream-analysis-dashboard.sh
	    ```

	    Your output should resemble:

	    ```bash
	    Loading Grafana ClickStream Dashboard
	    {"slug":"click-stream-analysis","status":"success","version":5}
	    ```

1.  Go to your browser and view the Grafana output at [http://localhost:33000/dashboard/db/click-stream-analysis](http://localhost:33000/dashboard/db/click-stream-analysis). You can login with user ID `admin` and password `admin`.

    **Important:** If you already have Grafana UI open, you may need to enter the specific clickstream URL: [http://localhost:33000/dashboard/db/click-stream-analysis](http://localhost:33000/dashboard/db/click-stream-analysis).

    ![Grafana UI success](grafana-success.png)	    

**About:** This dashboard demonstrates a series of streaming functionality where the title of each panel describes the type of stream processing required to generate the data. For example, the large chart in the middle is showing web-resource requests on a per-username basis using a Session window - where a sessions expire after 300 seconds of inactivity. Editing the panel allows you to view the datasource - which is named after the streams and tables captured in the clickstream-schema.sql file. 


**Interesting things to try:**
* Understand how the `clickstream-schema.sql` file is structured. We use a DataGen.KafkaTopic.clickstream -> Stream -> Table (for window & analytics with group-by) -> Table (to Add EVENT_TS for time-index) -> ElastiSearch/Connect topic  
* Run the `LIST TOPICS;` command to see where data is persisted
* Run the KSQL CLI `history` command

### Troubleshooting

- Check the Data Sources page in Grafana.
    - If your data source is shown, select it and scroll to the bottom and click the **Save & Test** button. This will indicate whether your data source is valid.
