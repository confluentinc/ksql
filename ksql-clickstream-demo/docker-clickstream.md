# Dockerized Clickstream Demo


#### Prerequisites
- [Confluent 3.3.0 installed](http://docs.confluent.io/current/installation.html) locally
- [ElasticSearch installed](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html)
- [Grafana installed](http://docs.grafana.org/installation/)
- []()




# Start container

docker run -p 33000:3000 -it confluentinc/ksql-clickstream-demo bash


# Start container services

/etc/init.d/elasticsearch start

/etc/init.d/grafana-server start

confluent start


# Data gen

ksql-datagen -daemon quickstart=clickstream format=json topic=clickstream maxInterval=100 iterations=500000
tail /tmp/ksql-logs/ksql.out  # Should see HTTP requests

ksql-datagen quickstart=clickstream_codes format=json topic=clickstream_codes maxInterval=100 
^C 

ksql-datagen quickstart=clickstream_users format=json topic=clickstream_users maxInterval=10 iterations=20


# KSQL CLI

ksql-server-start /etc/ksql/ksqlserver.properties > /tmp/ksql-logs/ksql-server.log 2>&1 &

ksql-cli remote http://localhost:8080

run script '/usr/share/doc/ksql-clickstream-demo/clickstream-schema.sql';

# Run the rest of the verification queries from the quickstart, I am skipping a few here...

select * from PAGES_PER_MIN;  
^C
^D


# Create dashboards:

cd /usr/share/doc/ksql-clickstream-demo/

./ksql-tables-to-grafana.sh

./clickstream-analysis-dashboard.sh {"slug":"click-stream-analysis","status":"success","version":5}