-- lets the windows accumulate more data
set 'commit.interval.ms'='10000';
set 'cache.max.bytes.buffering'='10000000';


-- 1. SOURCE of data

DROP STREAM clickstream;
CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid varchar, agent varchar) with (kafka_topic = 'clickstream_1', value_format = 'json');


-- 2. Derive the EVENTS_PER_MIN

 -- number of events per minute - think about key-for-distribution-purpose - shuffling etc - shouldnt use userid
--create table events_per_min as select userid, count(*) as events from clickstream window tumbling (size 60 second) group by userid;
DROP TABLE events_per_min;
create table events_per_min as select userid, count(*) as events from clickstream window  HOPPING ( size 10 second, advance by 5 second) group by userid;

-- VIEW Enrich with rowTime
DROP TABLE events_per_min_ts;
CREATE TABLE events_per_min_ts as select rowTime as event_ts, * from events_per_min;

-- VIEW
create table events_per_min_max_avg as select userid, min(events) as min, max(events) as max, sum(events)/count(events) as avg from events_per_min  WINDOW HOPPING (size 10 second, advance by 5 second) group by userid;
create table events_per_min_max_avg_ts as select rowTime as event_ts, * from events_per_min_max_avg;


-- 3. **CLICKSTREAM STATUS_CODES**
-- static table
DROP TABLE clickstream_codes;
CREATE TABLE clickstream_codes (code int, definition varchar) with (key='code', kafka_topic = 'clickstream_codes_1', value_format = 'json');

-- need to add _TS for ElasticSearch
DROP TABLE clickstream_codes_ts;
create table clickstream_codes_ts as select rowTime as event_ts, * from clickstream_codes;


-- 4. **PAGE_VIEWS**
DROP TABLE pages_per_min;
--create table pages_per_min as select userid, count(*) as pages from clickstream window tumbling (size 60 second) WHERE request like '%html%' group by userid ;
create table pages_per_min as select userid, count(*) as pages from clickstream WINDOW HOPPING (size 10 second, advance by 5 second) WHERE request like '%html%' group by userid ;

 -- VIEW add timestamp
 DROP TABLE pages_per_min_ts;
CREATE TABLE pages_per_min_ts as select rowTime as event_ts, * from pages_per_min;
 
 -- feed into Elastic using curl

 -- 4. **ALERT when > 'x' 404 errors**
 

-- HAVING Filter (broken)
-- create table errors_per_min_12 as select status, count(*) as errors from clickstream window HOPPING ( size 30 second, advance by 20 second) WHERE status > 100 group by status HAVING count(*) > 5;
DROP TABLE ERRORS_PER_MIN;
create table ERRORS_PER_MIN as select status, count(*) as errors from clickstream window HOPPING ( size 10 second, advance by 5  second) WHERE status > 400 group by status;


--VIEW Enrich with timestamp
DROP TABLE ERRORS_PER_MIN_TS;
CREATE TABLE ERRORS_PER_MIN_TS as select rowTime as event_ts, * from ERRORS_PER_MIN;

-- join on these
--clickstream_codes.code
--clickstream.status

-- VIEW
DROP STREAM enriched_error_codes_ts;
CREATE STREAM enriched_error_codes_ts AS SELECT clickstream.rowTime as event_ts, status, definition FROM clickstream LEFT JOIN clickstream_codes ON clickstream.status = clickstream_codes.code;








