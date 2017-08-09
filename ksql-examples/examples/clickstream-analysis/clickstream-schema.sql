-- lets the windows accumulate more data
set 'commit.interval.ms'='10000';
set 'cache.max.bytes.buffering'='10000000';


-- 1. SOURCE of ClickStream
DROP STREAM clickstream;
CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid varchar, agent varchar) with (kafka_topic = 'clickstream_1', value_format = 'json');


-- 2. Derive raw EVENTS_PER_MIN

 -- number of events per minute - think about key-for-distribution-purpose - shuffling etc - shouldnt use 'userid'
--create table events_per_min as select userid, count(*) as events from clickstream window tumbling (size 60 second) group by userid;
DROP TABLE events_per_min;
create table events_per_min as select userid, count(*) as events from clickstream window  HOPPING ( size 10 second, advance by 5 second) group by userid;

-- VIEW - Enrich with rowTime
DROP TABLE events_per_min_ts;
CREATE TABLE events_per_min_ts as select rowTime as event_ts, * from events_per_min;

-- VIEW
create table events_per_min_max_avg as select userid, min(events) as min, max(events) as max, sum(events)/count(events) as avg from events_per_min  WINDOW HOPPING (size 10 second, advance by 5 second) group by userid;
create table events_per_min_max_avg_ts as select rowTime as event_ts, * from events_per_min_max_avg;


-- 3. BUILD STATUS_CODES
-- static table
DROP TABLE clickstream_codes;
CREATE TABLE clickstream_codes (code int, definition varchar) with (key='code', kafka_topic = 'clickstream_codes_1', value_format = 'json');

-- need to add _TS for ElasticSearch
DROP TABLE clickstream_codes_ts;
create table clickstream_codes_ts as select rowTime as event_ts, * from clickstream_codes;


-- 4. BUILD PAGE_VIEWS
DROP TABLE pages_per_min;
create table pages_per_min as select userid, count(*) as pages from clickstream WINDOW HOPPING (size 10 second, advance by 5 second) WHERE request like '%html%' group by userid ;

 -- VIEW add timestamp
DROP TABLE pages_per_min_ts;
CREATE TABLE pages_per_min_ts as select rowTime as event_ts, * from pages_per_min;

 -- 4. URL STATUS CODES (Join AND Alert)
 
-- Use 'HAVING' Filter to show ERROR codes > 400 where count > 5
DROP TABLE ERRORS_PER_MIN_ALERT;
create TABLE ERRORS_PER_MIN_ALERT as select status, count(*) as errors from clickstream window HOPPING ( size 30 second, advance by 20 second) WHERE status > 400 group by status HAVING count(*) > 5 AND count(*) is not NULL;
DROP TABLE ERRORS_PER_MIN_ALERT_TS;
CREATE TABLE ERRORS_PER_MIN_ALERT_TS as select rowTime as event_ts, * from ERRORS_PER_MIN_ALERT;


DROP TABLE ERRORS_PER_MIN;
create table ERRORS_PER_MIN as select status, count(*) as errors from clickstream window HOPPING ( size 10 second, advance by 5  second) WHERE status > 400 group by status;


--VIEW - Enrich with timestamp
DROP TABLE ERRORS_PER_MIN_TS;
CREATE TABLE ERRORS_PER_MIN_TS as select rowTime as event_ts, * from ERRORS_PER_MIN;

-- VIEW - Enrich Codes with errors with Join to Status-Code definition
DROP STREAM ENRICHED_ERROR_CODES_TS;
CREATE STREAM ENRICHED_ERROR_CODES_TS AS SELECT clickstream.rowTime as event_ts, status, definition FROM clickstream LEFT JOIN clickstream_codes ON clickstream.status = clickstream_codes.code;


-- 5 Sessionisation using IP addresses - 300 seconds of inactivity expires the session
DROP TABLE CLICK_USER_SESSIONS;
DROP TABLE CLICK_USER_SESSIONS_TS;
create TABLE CLICK_USER_SESSIONS as SELECT ip, count(*) as events FROM clickstream window SESSION (300 second) GROUP BY ip;
create TABLE CLICK_USER_SESSIONS_TS as SELECT rowTime as event_ts, * from CLICK_USER_SESSIONS;








