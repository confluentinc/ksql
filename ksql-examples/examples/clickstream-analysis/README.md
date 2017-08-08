# ClickStream Analysis



### Prerequisites:
- Confluent 3.3.0 installed locally (default settings port:8083)
- ElasticSeach installed locally (default settings port:9200)
- Grafana installed locally (default settings port:3000)

- KSQL is downloaded and compiled [mvn package -Dmaven.test.skip=true]

_**Prior: Run Elastic and Grafana on default ports**_

1. Start Confluent Platform: 
```
confluent-3.3.0 user$  ./bin/confluent start
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

2. Run KSQL in local mode
```
:ksql user$ ./bin/ksql-cli local
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

3. Use DataGen to create the ClickStream
```
:ksql user$ bin/ksql-datagen  quickstart=clickstream format=json topic=clickstream_1 maxInterval=1000 iterations=5000
66.249.79.93 --> ([ '66.249.79.93' | '-' | '-' | '07/Aug/2017:17:02:37 -0700' | 1502150557891 | 'GET /index.html HTTP/1.1' | '407' | '4196' | '-' | 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36' ])
54.173.165.103 --> ([ '54.173.165.103' | '-' | '-' | '07/Aug/2017:17:02:38 -0700' | 1502150558520 | 'GET /site/login.html HTTP/1.1' | '405' | '278' | '-' | 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36' ])
```

4. Use DataGen to create the StatusCodes
```
:ksql user$ bin/ksql-datagen  quickstart=clickstream_codes format=json topic=clickstream_codes_1 maxInterval=1000 iterations=5000
404 --> ([ 404 | 'Page not found' ])
405 --> ([ 405 | 'Method not allowed' ])
```

5. Load the clickstream.sql schema file that will run the demo app
```
ksql> run script 'ksql-examples/examples/clickstream-analysis/clickstream-schema.sql';

 Message                            
------------------------------------
 Statement written to command topic 
ksql>
```

6. Check that TABLEs are created
```
ksql> show TABLES;

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

6. Check that STREAMs are created
```
ksql> show STREAMS;

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

7. Ensure that data is being streamed through various TABLEs and STREAMs
```
ksql> select * from CLICKSTREAM;
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

8. 'curl' the  'Connect' so that it pipes data into Elastic
```
ksql user$ cd ksql-examples/examples/clickstream-analysis/
user$ ./clickstream-schema-connect-elastic.sh 
{<<JSON RESPONSE>>} 
user$ 
```

9. Load the dashboard into Grafana
```
Navigate to: http://localhost:3000/
LHS => Dashboard => Import  => Upload .json file [choose ksql/ksql-examples/examples/clickstream-analysis/clickstream-analysis-dashboard.json ]
```

10. View the ClickStream Dashboard
```
Load [Click Stream Analysis]

```

Interesting things to try:
* Understand how the clickstream-schema.sql file is structured. We use a DataGen.KafkaTopic.clickstream_1 -> Stream -> Table (for window & analytics with group-by) -> Table (to Add EVENT_TS for time-index) -> ElastiSearch/Connect topic  
* try 'list topics' to see where data is persisted
* try 'list tables'
* try 'list streams'
* type: history
