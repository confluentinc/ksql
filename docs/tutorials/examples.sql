    CREATE STREAM pageviews \
      (viewtime BIGINT, \
       userid VARCHAR, \
       pageid VARCHAR) \
      WITH (KAFKA_TOPIC='pageviews', \
            VALUE_FORMAT='DELIMITED');
    CREATE TABLE users \
      (registertime BIGINT, \
       gender VARCHAR, \
       regionid VARCHAR, \
       userid VARCHAR, \
       interests array<VARCHAR>, \
       contactinfo map<VARCHAR, VARCHAR>) \
      WITH (KAFKA_TOPIC='users', \
            VALUE_FORMAT='JSON', \
            KEY = 'userid');
    CREATE STREAM pageviews_transformed \
      WITH (TIMESTAMP='viewtime', \
            PARTITIONS=5, \
            VALUE_FORMAT='JSON') AS \
      SELECT viewtime, \
             userid, \
             pageid, \
             TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HHmmss.SSS') AS timestring \
      FROM pageviews \
      PARTITION BY userid;
    CREATE STREAM pageviews_transformed_priority_1 \
      WITH (TIMESTAMP='viewtime', \
            PARTITIONS=5, \
            VALUE_FORMAT='JSON') AS \
      SELECT viewtime, \
             userid, \
             pageid, \
             TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HHmmss.SSS') AS timestring \
      FROM pageviews \
      WHERE userid='User_1' OR userid='User_2' \
      PARTITION BY userid;
    CREATE STREAM pageviews_transformed_priority_2 \
          WITH (TIMESTAMP='viewtime', \
                PARTITIONS=5, \
                VALUE_FORMAT='JSON') AS \
      SELECT viewtime, \
             userid, \
             pageid, \
             TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HHmmss.SSS') AS timestring \
      FROM pageviews \
      WHERE userid<>'User_1' AND userid<>'User_2' \
      PARTITION BY userid;
    CREATE TABLE users_5part \
        WITH (PARTITIONS=5) AS \
        SELECT * FROM USERS;
    CREATE STREAM pageviews_enriched AS \
      SELECT pv.viewtime, \
             pv.userid AS userid, \
             pv.pageid, \
             pv.timestring, \
             u.gender, \
             u.regionid, \
             u.interests, \
             u.contactinfo \
      FROM pageviews_transformed pv \
      LEFT JOIN users_5part u ON pv.userid = u.userid;
    CREATE TABLE pageviews_per_region AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      GROUP BY regionid;
    CREATE TABLE pageviews_per_region_per_minute AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW TUMBLING (SIZE 1 MINUTE) \
      GROUP BY regionid;
   CREATE TABLE pageviews_per_region_per_30secs AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW TUMBLING (SIZE 30 SECONDS) \
      WHERE UCASE(gender)='FEMALE' AND LCASE(regionid)='region_6' \
      GROUP BY regionid;
   CREATE TABLE pageviews_per_region_per_30secs10secs AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS) \
      WHERE UCASE(gender)='FEMALE' AND LCASE (regionid) LIKE '%_6' \
      GROUP BY regionid;
   CREATE TABLE pageviews_per_region_per_session AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW SESSION (60 SECONDS) \
      GROUP BY regionid;
    CREATE STREAM pageviews_interest_contact AS \
   SELECT interests[0] AS first_interest, \
             contactinfo['zipcode'] AS zipcode, \
             contactinfo['city'] AS city, \
             viewtime, \
             userid, \
             pageid, \
             timestring, \
             gender, \
             regionid \
      FROM pageviews_enriched;