-- a test script used in sql-tests/test.sql
-- NOTE: do not move this test to within the sql-tests dir because it will be
-- picked up and attempted to execute as a full test by the test framework
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;