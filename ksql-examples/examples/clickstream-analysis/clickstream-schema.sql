-- lets the windows accumulate more data
set 'commit.interval.ms'='2000';
set 'cache.max.bytes.buffering'='10000000';


-- 1. SOURCE of ClickStream
DROP STREAM clickstream;
CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid int, bytes bigint, agent varchar) with (kafka_topic = 'clickstream', value_format = 'json');


----------------------------------------------------------------------------------------------------------------------------
-- A series of basic clickstream-analytics
--
-- Min, Max, UDFs etc
----------------------------------------------------------------------------------------------------------------------------

 -- number of events per minute - think about key-for-distribution-purpose - shuffling etc - shouldnt use 'userid'
DROP TABLE events_per_min;
create table events_per_min as select userid, count(*) as events from clickstream window  TUMBLING (size 10 second) group by userid;

-- VIEW - Enrich with rowTime
DROP TABLE events_per_min_ts;
CREATE TABLE events_per_min_ts as select rowTime as event_ts, * from events_per_min;

-- VIEW
DROP TABLE events_per_min_max_avg;
DROP TABLE events_per_min_max_avg_ts;
create table events_per_min_max_avg as select userid, min(events) as min, max(events) as max, sum(events)/count(events) as avg from events_per_min  WINDOW TUMBLING (size 10 second) group by userid;
create table events_per_min_max_avg_ts as select rowTime as event_ts, * from events_per_min_max_avg;


-- 3. BUILD STATUS_CODES
-- static table
DROP TABLE clickstream_codes;
CREATE TABLE clickstream_codes (code int, definition varchar) with (key='code', kafka_topic = 'clickstream_codes', value_format = 'json');

-- Add _TS for Timeseries storage
DROP TABLE clickstream_codes_ts;
create table clickstream_codes_ts as select rowTime as event_ts, * from clickstream_codes;


-- 4. BUILD PAGE_VIEWS
DROP TABLE pages_per_min;
create table pages_per_min as select userid, count(*) as pages from clickstream WINDOW HOPPING (size 10 second, advance by 5 second) WHERE request like '%html%' group by userid ;

 -- Add _TS for Timeseries storage
DROP TABLE pages_per_min_ts;
CREATE TABLE pages_per_min_ts as select rowTime as event_ts, * from pages_per_min;


----------------------------------------------------------------------------------------------------------------------------
-- URL STATUS CODES (Join AND Alert)
--
--
----------------------------------------------------------------------------------------------------------------------------

-- Use 'HAVING' Filter to show ERROR codes > 400 where count > 5
DROP TABLE ERRORS_PER_MIN_ALERT;
create TABLE ERRORS_PER_MIN_ALERT as select status, count(*) as errors from clickstream window HOPPING ( size 30 second, advance by 20 second) WHERE status > 400 group by status HAVING count(*) > 5 AND count(*) is not NULL;
DROP TABLE ERRORS_PER_MIN_ALERT_TS;
CREATE TABLE ERRORS_PER_MIN_ALERT_TS as select rowTime as event_ts, * from ERRORS_PER_MIN_ALERT;


DROP TABLE ERRORS_PER_MIN;
create table ERRORS_PER_MIN as select status, count(*) as errors from clickstream window HOPPING ( size 10 second, advance by 5  second) WHERE status > 400 group by status;
DROP TABLE ERRORS_PER_MIN_TS;
CREATE TABLE ERRORS_PER_MIN_TS as select rowTime as event_ts, * from ERRORS_PER_MIN;


-- VIEW - Enrich Codes with errors with Join to Status-Code definition
DROP STREAM ENRICHED_ERROR_CODES;
DROP TABLE ENRICHED_ERROR_CODES_COUNT;
DROP STREAM ENRICHED_ERROR_CODES_TS;

--Join using a STREAM
CREATE STREAM ENRICHED_ERROR_CODES AS SELECT code, definition FROM clickstream LEFT JOIN clickstream_codes ON clickstream.status = clickstream_codes.code;
-- Aggregate (count&groupBy) using a TABLE-Window
CREATE TABLE ENRICHED_ERROR_CODES_COUNT AS SELECT code, definition, COUNT(*) AS count FROM ENRICHED_ERROR_CODES WINDOW TUMBLING (size 30 second) GROUP BY code, definition HAVING COUNT(*) > 1;
-- Enrich w rowTime timestamp to support timeseries search
CREATE TABLE ENRICHED_ERROR_CODES_TS AS SELECT rowTime as EVENT_TS, * FROM ENRICHED_ERROR_CODES_COUNT;


----------------------------------------------------------------------------------------------------------------------------
-- Clickstream users for enrichment and exception monitoring
--
--
----------------------------------------------------------------------------------------------------------------------------

-- users lookup table
DROP TABLE WEB_USERS;
CREATE TABLE WEB_USERS (user_id int, registered_At long, username varchar, first_name varchar, last_name varchar, city varchar, level varchar) with (key='user_id', kafka_topic = 'clickstream_users', value_format = 'json');

-- Clickstream enriched with user account data
DROP STREAM customer_clickstream;
CREATE STREAM customer_clickstream WITH (PARTITIONS=2) as SELECT userid, u.first_name, u.last_name, u.level, time, ip, request, status, agent FROM clickstream c LEFT JOIN web_users u ON c.userid = u.user_id;

-- Find error views by important users
--DROP STREAM platinum_customers_with_errors
--create stream platinum_customers_with_errors WITH (PARTITIONS=2) as seLECT * FROM customer_clickstream WHERE status > 400 AND level = 'Platinum';

-- Find error views by important users in one shot
--DROP STREAM platinum_errors;
--CREATE STREAM platinum_errors WITH (PARTITIONS=2) as SELECT userid, u.first_name, u.last_name, u.city, u.level, time, ip, request, status, agent FROM clickstream c LEFT JOIN web_users u ON c.userid = u.user_id WHERE status > 400 AND level = 'Platinum';
--
---- Trend of errors from important users
--DROP TABLE platinum_page_errors_per_5_min;
--CREATE TABLE platinum_errors_per_5_min AS SELECT userid, first_name, last_name, city, count(*) as running_count FROM platinum_errors WINDOW TUMBLING (SIZE 5 MINUTE) WHERE request LIKE '%html%' GROUP BY userid, first_name, last_name, city;


----------------------------------------------------------------------------------------------------------------------------
-- User experience monitoring
--
-- View IP, username and City Versus web-site-activity (hits)
----------------------------------------------------------------------------------------------------------------------------
DROP STREAM USER_CLICKSTREAM;
CREATE STREAM USER_CLICKSTREAM AS SELECT userid, u.username, ip, u.city, request, status, bytes FROM clickstream c LEFT JOIN web_users u ON c.userid = u.user_id;

-- Aggregate (count&groupBy) using a TABLE-Window
DROP TABLE USER_IP_ACTIVITY;
CREATE TABLE USER_IP_ACTIVITY AS SELECT username, ip, city, COUNT(*) AS count FROM USER_CLICKSTREAM WINDOW TUMBLING (size 60 second) GROUP BY username, ip, city HAVING COUNT(*) > 1;

-- Enrich w rowTime timestamp to support timeseries search
DROP TABLE USER_IP_ACTIVITY_TS;
CREATE TABLE USER_IP_ACTIVITY_TS AS SELECT rowTime as EVENT_TS, * FROM USER_IP_ACTIVITY;


----------------------------------------------------------------------------------------------------------------------------
-- User session monitoring
--
-- Sessionisation using IP addresses - 300 seconds of inactivity expires the session
--
----------------------------------------------------------------------------------------------------------------------------

DROP TABLE CLICK_USER_SESSIONS;
DROP TABLE CLICK_USER_SESSIONS_TS;
CREATE TABLE CLICK_USER_SESSIONS as SELECT username, count(*) as events FROM USER_CLICKSTREAM window SESSION (300 second) GROUP BY username;
CREATE TABLE CLICK_USER_SESSIONS_TS as SELECT rowTime as event_ts, * from CLICK_USER_SESSIONS;


----------------------------------------------------------------------------------------------------------------------------
-- Blog Article tracking user-session and bandwidth
--
-- Sessionisation using IP addresses - 300 seconds of inactivity expires the session
--
----------------------------------------------------------------------------------------------------------------------------

--DROP TABLE PER_USER_KBYTES;
--create TABLE PER_USER_KBYTES as SELECT username, sum(bytes)/1024 as kbytes FROM USER_CLICKSTREAM window SESSION (300 second) GROUP BY username;

--DROP TABLE PER_USER_KBYTES_TS;
--CREATE TABLE PER_USER_KBYTES_TS as select rowTime as event_ts, kbytes, username from PER_USER_KBYTES WHERE ip IS NOT NULL;

--DROP TABLE MALICIOUS_USER_SESSIONS;
--CREATE TABLE MALICIOUS_USER_SESSIONS as SELECT username, ip,  sum(bytes)/1024 as kbytes FROM USER_CLICKSTREAM window SESSION (300 second) GROUP BY username, ip  HAVING sum(bytes)/1024 > 50;
--CREATE TABLE MALICIOUS_USER_SESSIONS_TS as select rowTime as event_ts, ip, username, kbytes from MALICIOUS_USER_SESSIONS;


