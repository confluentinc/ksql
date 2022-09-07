--@test: table-table-join - Should fail on right non-key attribute for inner-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got LEFT_TABLE.ID1 = RIGHT_TABLE.F2.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table JOIN right_table ON id1 = f2;
--@test: table-table-join - Should fail on right non-key attribute for inner-join -- revers join condition order
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got RIGHT_TABLE.F2 = LEFT_TABLE.ID1.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table JOIN right_table ON f2 = id1;
--@test: table-table-join - Should fail on right non-key attribute for left-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got LEFT_TABLE.ID1 = RIGHT_TABLE.F2.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table LEFT JOIN right_table ON id1 = f2;
--@test: table-table-join - Should fail on right non-key attribute for left-join -- revers join condition order
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got RIGHT_TABLE.F2 = LEFT_TABLE.ID1.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table LEFT JOIN right_table ON f2 = id1;
--@test: table-table-join - Should fail on right non-key attribute for outer-join
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got LEFT_TABLE.ID1 = RIGHT_TABLE.F2.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table FULL OUTER JOIN right_table ON id1 = f2;
--@test: table-table-join - Should fail on right non-key attribute for outer-join -- reverse join condition order
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got RIGHT_TABLE.F2 = LEFT_TABLE.ID1.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table FULL OUTER JOIN right_table ON f2 = id1;
--@test: table-table-join - Should fail on right non-key attribute for inner-join with qualifiers
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got LEFT_TABLE.ID1 = RIGHT_TABLE.F2.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table JOIN right_table ON left_table.id1 = right_table.f2;
--@test: table-table-join - Should fail on right non-key attribute for inner-join with qualifiers -- reverse join condition order
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got RIGHT_TABLE.F2 = LEFT_TABLE.ID1.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table JOIN right_table ON right_table.f2 = left_table.id1;
--@test: table-table-join - Should fail on right non-key attribute for inner-join with alias
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got LT.ID1 = RT.F2.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table AS lt JOIN right_table AS rt ON lt.id1 = rt.f2;
--@test: table-table-join - Should fail on right non-key attribute for inner-join with alias -- reverse join condition order
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: table-table joins require to join on the primary key of the right input table. Got RT.F2 = LT.ID1.
CREATE TABLE left_table (id1 BIGINT PRIMARY KEY, f1 BIGINT) WITH (kafka_topic='left_topic', format='JSON');
CREATE TABLE right_table (id2 BIGINT PRIMARY KEY, f2 BIGINT) WITH (kafka_topic='right_topic', format='JSON');
CREATE TABLE output AS SELECT id1, f1, f2 FROM left_table AS lt JOIN right_table AS rt ON rt.f2 = lt.id1;
