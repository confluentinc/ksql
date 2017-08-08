# ClickStream Analysis



### Prerequisites:
- Confluent 3.3.0 installed locally (default settings)
- ElasticSeach installed locally (default settings)
- ElasticSeach installed locally (default settings)

- KSQL is downloaded and compiled

_**Prior: Run Elastic and Grafana**_

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
 






