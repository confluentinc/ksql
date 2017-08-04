-- lets the windows accumulate more data
set 'commit.interval.ms'='10000';
set 'cache.max.bytes.buffering'='10000000';


-- 1. SOURCE of data

DROP STREAM clickstream;
CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid varchar, agent varchar) with (kafka_topic = 'clickstream_1', value_format = 'json');

DROP TABLE clickstream_codes;
CREATE TABLE clickstream_codes (code int, definition varchar) with (key='code', kafka_topic = 'clickstream_codes_1', value_format = 'json');

-- 2. Derive the EVENTS_PER_MIN

 -- number of events per minute - think about key-for-distribution-purpose - shuffling etc - shouldnt use userid
--create table events_per_min as select userid, count(*) as events from clickstream window tumbling (size 60 second) group by userid;
create table events_per_min as select userid, count(*) as events from clickstream window  HOPPING ( size 10 second, advance by 5 second) group by userid;

-- MUST CLEAN via STREAM to avoid ClassCast to REMOVE WINDOW and derive other tables from the STREAM HACK!
CREATE STREAM events_per_min_stream_base (userId string, events bigint) with (kafka_topic='EVENTS_PER_MIN', value_format='json');

-- Enrich with rowTime
CREATE TABLE events_per_min_ts as select rowTime as event_ts, events from events_per_min_stream_base;

-- create table events_per_min_max_avg as select userid, min(events) as min, max(events) as max, sum(events)/count(events) as avg from events_per_min_stream_base  WINDOW TUMBLING (size 60 second) group by userid;
create table events_per_min_max_avg as select userid, min(events) as min, max(events) as max, sum(events)/count(events) as avg from events_per_min_stream_base  WINDOW HOPPING (size 10 second, advance by 5 second) group by userid;

create table events_per_min_max_avg_ts as select rowTime as event_ts, * from events_per_min_max_avg;


-- 3. **CLICKSTREAM STATUS_CODES**
create table clickstream_codes_ts as select rowTime as event_ts, * from clickstream_codes;


-- 4. **PAGE_VIEWS**

--create table pages_per_min as select userid, count(*) as pages from clickstream window tumbling (size 60 second) WHERE request like '%html%' group by userid ;
create table pages_per_min as select userid, count(*) as pages from clickstream WINDOW HOPPING (size 10 second, advance by 5 second) WHERE request like '%html%' group by userid ;

--  strip out the window - causes classcast on change-log
CREATE STREAM pages_min_stream_base (userId string, pages bigint) with (kafka_topic='PAGES_PER_MIN', value_format='json');
 
 -- add timestamp
CREATE TABLE pages_per_min_ts as select rowTime as event_ts, * from pages_min_stream_base;
 
 -- feed into Elastic using curl

 -- 4. **ALERT when > 'x' 404 errors**
 
-- NO WORK - HAving broken -- create table errors_per_min as select status, count(*) as errors from clickstream window hopping (size 60 second) WHERE status  > 400 group by status HAVING COUNT(*) > 4 ;


-- HAVING Filter (broken)
-- create table errors_per_min_12 as select status, count(*) as errors from clickstream window HOPPING ( size 30 second, advance by 20 second) WHERE status > 100 group by status HAVING count(*) > 5;
create table ERRORS_PER_MIN_10 as select status, count(*) as errors from clickstream window HOPPING ( size 10 second, advance by 5  second) WHERE status > 400 group by status;


CREATE STREAM errors_min_stream_base (userId string, pages bigint) with (kafka_topic='ERRORS_PER_PER_MIN_10', value_format='json');

--Enrich with timestamp
CREATE TABLE ERRORS_PER_MIN_10_TS as select rowTime as event_ts, * from errors_min_stream_base;

// join on these
clickstream_codes.code
clickstream.status

--CREATE STREAM enrichedpageview_female AS SELECT users.userid AS userid, pageid, regionid, gender FROM pageview LEFT JOIN users ON pageview.userid = users.userid WHERE gender = 'FEMALE';


CREATE STREAM enriched_error_codes_ts AS SELECT clickstream.rowTime as event_ts, status, definition FROM clickstream LEFT JOIN clickstream_codes ON clickstream.status = clickstream_codes.code;








