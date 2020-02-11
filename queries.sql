create stream test (foo varchar) with (kafka_topic='test',value_format='json');
create stream test_copy as select * from test;
