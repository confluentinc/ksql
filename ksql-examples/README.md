# ksql example
This package provides KSQL example queries along with data generator tools for them.
After building the project you can run the examples by following these steps:

1. Start Zookeeper and Kafka in local machine with defaut settings

2. Start the data gen for the users using the following command:

        $ ./bin/ksql-datagen quickstart=users format=json topic=user_topic_json maxInterval=1000


3. Start the data gen for pageview topic using the following command:

        $ ./bin/ksql-datagen quickstart=pageviews format=json topic=pageview_topic_json
        
4. Start the KSQL server using the following command:
        
        $ ./bin/ksql-server-start config/ksql-server.properties

4. Start the KSQL CLI using the following command:

        $ ./bin/ksql http://localhost:8088


You will be able to run the provided queries and see the results. The data gen module will continuously push new messages to the topics until you terminate them

Here are the sample queries:

```sql
CREATE STREAM pageview (viewtime bigint, pageid varchar, userid varchar) WITH (value_format = 'json', kafka_topic='pageview_topic_json');
```
```sql
CREATE TABLE users (registertime bigint, userid varchar, regionid varchar, gender varchar) WITH (value_format = 'json', kafka_topic='user_topic_json');
```

```sql
-- Enrich the pageview stream
CREATE STREAM enrichedpv AS SELECT users.userid AS userid, pageid, regionid, gender FROM pageview LEFT JOIN users ON pageview.userid = users.userid;

-- Find all the pageviews by female users
CREATE STREAM enrichedpv_female AS SELECT users.userid AS userid, pageid, regionid, gender FROM pageview LEFT JOIN users ON pageview.userid = users.userid WHERE gender = 'FEMALE';

-- Find the pageviews from reagion with id ending in _8 and _9 from the female pageview
CREATE STREAM enrichedpv_female_r8 AS SELECT * FROM enrichedpv_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';

-- Number of views for each page for tumbling window of 5 seconds
CREATE TABLE pvcount AS SELECT pageid, count(*) from enrichedpv window tumbling (size 5 second) group by pageid;

-- Number of views for each page for tumbling window of 5 minutes
CREATE TABLE pvcount_5min AS SELECT pageid, count(*) from enrichedpv window tumbling (size 5 minute) group by pageid;

-- Number of views for each each reagion and gender combination for tumbling window of 15 seconds
-- when the view count is greater than 5
CREATE TABLE pvcount_gender_region AS SELECT gender, regionid , count(*) from enrichedpv window tumbling (size 15 second) group by gender, regionid having count(*) > 5;

```
