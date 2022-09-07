--@test: case-expression - searched case expression
CREATE STREAM orders (ID STRING KEY, ORDERUNITS double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN orderunits < 2.0 THEN 'small' WHEN orderunits < 4.0 THEN 'medium' ELSE 'large' END AS case_resault FROM orders;
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (6.0);
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (3.0);
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (1.0);
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('large');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('medium');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('small');

--@test: case-expression - searched case with arithmetic expression in result
CREATE STREAM orders (ID STRING KEY, orderid bigint, ORDERUNITS double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN orderunits < 2.0 THEN orderid + 2 END AS case_resault FROM orders;
INSERT INTO `ORDERS` (ORDERID, ORDERUNITS) VALUES (4, 1.9);
INSERT INTO `ORDERS` (ORDERID, ORDERUNITS) VALUES (5, 1.0);
INSERT INTO `ORDERS` (ORDERID, ORDERUNITS) VALUES (5, 2.0);
ASSERT VALUES `S1` (CASE_RESAULT) VALUES (6);
ASSERT VALUES `S1` (CASE_RESAULT) VALUES (7);
ASSERT VALUES `S1` (CASE_RESAULT) VALUES (NULL);

--@test: case-expression - searched case with null in when
CREATE STREAM orders (ID STRING KEY, orderid bigint, ORDERUNITS double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN orderunits > 2.0 THEN 'foo' ELSE 'default' END AS case_resault FROM orders;
INSERT INTO `ORDERS` (ORDERID) VALUES (1);
INSERT INTO `ORDERS` (ORDERID, ORDERUNITS) VALUES (NULL, NULL);
INSERT INTO `ORDERS` (ORDERID, ORDERUNITS) VALUES (2, 4.0);
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('default');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('default');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('foo');

--@test: case-expression - searched case returning null in first branch
CREATE STREAM orders (ID STRING KEY, ORDERUNITS double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN orderunits < 2.0 THEN null WHEN orderunits < 4.0 THEN 'medium' ELSE 'large' END AS case_result FROM orders;
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (4.2);
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (3.99);
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (1.1);
ASSERT VALUES `S1` (CASE_RESULT) VALUES ('large');
ASSERT VALUES `S1` (CASE_RESULT) VALUES ('medium');
ASSERT VALUES `S1` (CASE_RESULT) VALUES (NULL);

--@test: case-expression - searched case returning null in later branch
CREATE STREAM orders (ID STRING KEY, ORDERUNITS double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN orderunits < 2.0 THEN 'small' WHEN orderunits < 4.0 THEN null ELSE 'large' END AS case_result FROM orders;
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (4.2);
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (3.99);
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (1.1);
ASSERT VALUES `S1` (CASE_RESULT) VALUES ('large');
ASSERT VALUES `S1` (CASE_RESULT) VALUES (NULL);
ASSERT VALUES `S1` (CASE_RESULT) VALUES ('small');

--@test: case-expression - searched case returning null in default branch
CREATE STREAM orders (ID STRING KEY, ORDERUNITS double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN orderunits < 2.0 THEN 'small' ELSE null END AS case_result FROM orders;
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (4.2);
INSERT INTO `ORDERS` (ORDERUNITS) VALUES (1.1);
ASSERT VALUES `S1` (CASE_RESULT) VALUES (NULL);
ASSERT VALUES `S1` (CASE_RESULT) VALUES ('small');

--@test: case-expression - searched case returning null in all branch
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Case expression. All case branches have NULL type
CREATE STREAM orders (ID STRING KEY, ORDERUNITS double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN orderunits < 2.0 THEN null ELSE null END AS case_result FROM orders;
--@test: case-expression - searched case expression with structs, multiple expression and the same type
CREATE STREAM orders (ID STRING KEY, address STRUCT <city varchar, state varchar>, itemid STRUCT<NAME VARCHAR>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM S1 AS SELECT ID, CASE WHEN ADDRESS->STATE = 'STATE_1' THEN ITEMID->NAME WHEN ADDRESS->STATE = 'STATE_3' THEN ADDRESS->CITY ELSE 'default' END AS case_resault FROM orders;
INSERT INTO `ORDERS` (ITEMID, ADDRESS) VALUES (STRUCT(NAME:='Food'), STRUCT(CITY:='CITY_0', STATE:='STATE_1'));
INSERT INTO `ORDERS` (ITEMID, ADDRESS) VALUES (STRUCT(NAME:='Produce'), STRUCT(CITY:='CITY_3', STATE:='STATE_6'));
INSERT INTO `ORDERS` (ITEMID, ADDRESS) VALUES (STRUCT(NAME:='Produce'), STRUCT(CITY:='CITY_9', STATE:='STATE_9'));
INSERT INTO `ORDERS` (ITEMID, ADDRESS) VALUES (STRUCT(NAME:='Food'), STRUCT(CITY:='CITY_3', STATE:='STATE_5'));
INSERT INTO `ORDERS` (ITEMID, ADDRESS) VALUES (STRUCT(NAME:='Produce'), STRUCT(CITY:='CITY_6', STATE:='STATE_3'));
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('Food');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('default');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('default');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('default');
ASSERT VALUES `S1` (CASE_RESAULT) VALUES ('CITY_6');

--@test: case-expression - should execute branches lazily
CREATE STREAM INPUT (ID STRING KEY, IGNORED INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CASE WHEN WHENCONDITION(true, true) THEN WHENRESULT(100, true) WHEN WHENCONDITION(true, false) THEN WHENRESULT(200, false) ELSE WHENRESULT(300, false)END FROM input;
INSERT INTO `INPUT` (id) VALUES ('abc');
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (100);

--@test: case-expression - should only execute ELSE if not matching WHENs
CREATE STREAM INPUT (ID STRING KEY, IGNORED INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, CASE WHEN WHENCONDITION(false, true) THEN WHENRESULT(100, false) WHEN WHENCONDITION(false, true) THEN WHENRESULT(200, false) ELSE WHENRESULT(300, true) END FROM input;
INSERT INTO `INPUT` (id) VALUES ('abc');
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (300);

--@test: case-expression - as param to UDAF
CREATE STREAM russell_3k_trades (tickerId VARCHAR KEY, quantity INT, price INT, buy BOOLEAN) WITH (kafka_topic='test_topic',value_format='json');
CREATE TABLE russell_3k_hourly AS SELECT tickerId, count(*) as tradeCount, sum(quantity) as tradeVolumn, min(CASE WHEN buy THEN price ELSE null END) as minBuyPrice, max(CASE WHEN buy THEN price ELSE null END) as maxBuyPrice, min(CASE WHEN buy THEN null ELSE price END) as minSellPrice, max(CASE WHEN buy THEN null ELSE price END) as maxSellPrice FROM russell_3k_trades WINDOW TUMBLING (SIZE 1 HOUR) GROUP BY tickerId;
INSERT INTO `RUSSELL_3K_TRADES` (TICKERID, quantity, price, buy) VALUES ('AEIS', 76, 120125102, true);
INSERT INTO `RUSSELL_3K_TRADES` (TICKERID, quantity, price, buy) VALUES ('AEIS', 10, 100125102, false);
ASSERT VALUES `RUSSELL_3K_HOURLY` (TICKERID, TRADECOUNT, TRADEVOLUMN, MINBUYPRICE, MAXBUYPRICE, MINSELLPRICE, MAXSELLPRICE, WINDOWSTART, WINDOWEND) VALUES ('AEIS', 1, 76, 120125102, 120125102, NULL, NULL, 0, 3600000);
ASSERT VALUES `RUSSELL_3K_HOURLY` (TICKERID, TRADECOUNT, TRADEVOLUMN, MINBUYPRICE, MAXBUYPRICE, MINSELLPRICE, MAXSELLPRICE, WINDOWSTART, WINDOWEND) VALUES ('AEIS', 2, 86, 120125102, 120125102, 100125102, 100125102, 0, 3600000);

