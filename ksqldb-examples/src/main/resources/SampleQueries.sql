REGISTER TOPIC users_topic WITH (value_format = 'json', kafka_topic='user_topic_json');
REGISTER TOPIC pageview_topic WITH (value_format = 'json', kafka_topic='pageview_topic_json');

CREATE STREAM pageview (viewtime bigint, pageid varchar, userid varchar) WITH (registered_topic = 'pageview_topic');
CREATE TABLE users (registertime bigint, userid varchar, regionid varchar, gender varchar) WITH
(registered_topic = 'users_topic', KEY = 'userid');


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