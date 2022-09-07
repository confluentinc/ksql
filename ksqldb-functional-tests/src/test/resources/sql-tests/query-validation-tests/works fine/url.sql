--@test: url - encode a url parameter using ENCODE_URL_PARAM - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_ENCODE_PARAM(url) as ENCODED FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', '?foo $bar', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'hello&world', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'nothing', 2);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('1', '%3Ffoo+%24bar', 0);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('2', 'hello%26world', 1);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('3', 'nothing', 2);

--@test: url - encode a url parameter using ENCODE_URL_PARAM - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_ENCODE_PARAM(url) as ENCODED FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', '?foo $bar', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'hello&world', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'nothing', 2);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('1', '%3Ffoo+%24bar', 0);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('2', 'hello%26world', 1);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('3', 'nothing', 2);

--@test: url - encode a url parameter using ENCODE_URL_PARAM - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_ENCODE_PARAM(url) as ENCODED FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', '?foo $bar', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'hello&world', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'nothing', 2);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('1', '%3Ffoo+%24bar', 0);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('2', 'hello%26world', 1);
ASSERT VALUES `OUTPUT` (K, ENCODED, ROWTIME) VALUES ('3', 'nothing', 2);

--@test: url - decode a url parameter using DECODE_URL_PARAM - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_DECODE_PARAM(url) as DECODED FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', '%3Ffoo+%24bar', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'hello%26world', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'nothing', 2);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('1', '?foo $bar', 0);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('2', 'hello&world', 1);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('3', 'nothing', 2);

--@test: url - decode a url parameter using DECODE_URL_PARAM - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_DECODE_PARAM(url) as DECODED FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', '%3Ffoo+%24bar', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'hello%26world', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'nothing', 2);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('1', '?foo $bar', 0);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('2', 'hello&world', 1);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('3', 'nothing', 2);

--@test: url - decode a url parameter using DECODE_URL_PARAM - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_DECODE_PARAM(url) as DECODED FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', '%3Ffoo+%24bar', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'hello%26world', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'nothing', 2);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('1', '?foo $bar', 0);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('2', 'hello&world', 1);
ASSERT VALUES `OUTPUT` (K, DECODED, ROWTIME) VALUES ('3', 'nothing', 2);

--@test: url - extract a fragment from a URL using URL_EXTRACT_FRAGMENT - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_FRAGMENT(url) as FRAGMENT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.nofragment.com', 1);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('1', 'fragment', 0);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('2', NULL, 1);

--@test: url - extract a fragment from a URL using URL_EXTRACT_FRAGMENT - AVRO
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_FRAGMENT(url) as FRAGMENT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.nofragment.com', 1);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('1', 'fragment', 0);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('2', NULL, 1);

--@test: url - extract a fragment from a URL using URL_EXTRACT_FRAGMENT - PROTOBUFs - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_FRAGMENT(url) as FRAGMENT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.nofragment.com', 1);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('1', 'fragment', 0);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('2', '', 1);

--@test: url - extract a fragment from a URL using URL_EXTRACT_FRAGMENT - PROTOBUFs - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_FRAGMENT(url) as FRAGMENT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.nofragment.com', 1);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('1', 'fragment', 0);
ASSERT VALUES `OUTPUT` (K, FRAGMENT, ROWTIME) VALUES ('2', '', 1);

--@test: url - extract a host from a URL using URL_EXTRACT_HOST - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_HOST(url) as HOST FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://test@confluent.io:8080', 1);
ASSERT VALUES `OUTPUT` (K, HOST, ROWTIME) VALUES ('1', 'www.test.com', 0);
ASSERT VALUES `OUTPUT` (K, HOST, ROWTIME) VALUES ('2', 'confluent.io', 1);

--@test: url - extract a host from a URL using URL_EXTRACT_HOST - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_HOST(url) as HOST FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://test@confluent.io:8080', 1);
ASSERT VALUES `OUTPUT` (K, HOST, ROWTIME) VALUES ('1', 'www.test.com', 0);
ASSERT VALUES `OUTPUT` (K, HOST, ROWTIME) VALUES ('2', 'confluent.io', 1);

--@test: url - extract a host from a URL using URL_EXTRACT_HOST - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_HOST(url) as HOST FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://test@confluent.io:8080', 1);
ASSERT VALUES `OUTPUT` (K, HOST, ROWTIME) VALUES ('1', 'www.test.com', 0);
ASSERT VALUES `OUTPUT` (K, HOST, ROWTIME) VALUES ('2', 'confluent.io', 1);

--@test: url - extract a parameter from a URL using URL_EXTRACT_PARAMETER - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PARAMETER(url,'one') as PARAM_A, URL_EXTRACT_PARAMETER(url,'two') as PARAM_B FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?one=a&two=b&three', 0);
ASSERT VALUES `OUTPUT` (K, PARAM_A, PARAM_B, ROWTIME) VALUES ('1', 'a', 'b', 0);

--@test: url - extract a parameter from a URL using URL_EXTRACT_PARAMETER - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PARAMETER(url,'one') as PARAM_A, URL_EXTRACT_PARAMETER(url,'two') as PARAM_B FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?one=a&two=b&three', 0);
ASSERT VALUES `OUTPUT` (K, PARAM_A, PARAM_B, ROWTIME) VALUES ('1', 'a', 'b', 0);

--@test: url - extract a parameter from a URL using URL_EXTRACT_PARAMETER - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PARAMETER(url,'one') as PARAM_A, URL_EXTRACT_PARAMETER(url,'two') as PARAM_B FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?one=a&two=b&three', 0);
ASSERT VALUES `OUTPUT` (K, PARAM_A, PARAM_B, ROWTIME) VALUES ('1', 'a', 'b', 0);

--@test: url - chain a call to URL_EXTRACT_PARAMETER with URL_DECODE_PARAM - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_DECODE_PARAM(URL_EXTRACT_PARAMETER(url,'two')) as PARAM FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?one=a&two=url%20encoded', 0);
ASSERT VALUES `OUTPUT` (K, PARAM, ROWTIME) VALUES ('1', 'url encoded', 0);

--@test: url - chain a call to URL_EXTRACT_PARAMETER with URL_DECODE_PARAM - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_DECODE_PARAM(URL_EXTRACT_PARAMETER(url,'two')) as PARAM FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?one=a&two=url%20encoded', 0);
ASSERT VALUES `OUTPUT` (K, PARAM, ROWTIME) VALUES ('1', 'url encoded', 0);

--@test: url - chain a call to URL_EXTRACT_PARAMETER with URL_DECODE_PARAM - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_DECODE_PARAM(URL_EXTRACT_PARAMETER(url,'two')) as PARAM FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com/?one=a&two=url%20encoded', 0);
ASSERT VALUES `OUTPUT` (K, PARAM, ROWTIME) VALUES ('1', 'url encoded', 0);

--@test: url - extract a path from a URL using URL_EXTRACT_PATH - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PATH(url) as PATH FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.test.com/path?query', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'http://test@confluent.io:8080/nested/path?query&jobs', 2);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('1', '', 0);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('2', '/path', 1);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('3', '/nested/path', 2);

--@test: url - extract a path from a URL using URL_EXTRACT_PATH - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PATH(url) as PATH FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.test.com/path?query', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'http://test@confluent.io:8080/nested/path?query&jobs', 2);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('1', '', 0);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('2', '/path', 1);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('3', '/nested/path', 2);

--@test: url - extract a path from a URL using URL_EXTRACT_PATH - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PATH(url) as PATH FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.test.com/path?query', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'http://test@confluent.io:8080/nested/path?query&jobs', 2);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('1', '', 0);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('2', '/path', 1);
ASSERT VALUES `OUTPUT` (K, PATH, ROWTIME) VALUES ('3', '/nested/path', 2);

--@test: url - extract a port from a URL using URL_EXTRACT_PORT - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PORT(url) as PORT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://test@confluent.io:8080/nested/path?query&jobs', 1);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('1', NULL, 0);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('2', 8080, 1);

--@test: url - extract a port from a URL using URL_EXTRACT_PORT - AVRO
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PORT(url) as PORT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://test@confluent.io:8080/nested/path?query&jobs', 1);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('1', NULL, 0);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('2', 8080, 1);

--@test: url - extract a port from a URL using URL_EXTRACT_PORT - PROTOBUFs - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PORT(url) as PORT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://test@confluent.io:8080/nested/path?query&jobs', 1);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('1', 0, 0);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('2', 8080, 1);

--@test: url - extract a port from a URL using URL_EXTRACT_PORT - PROTOBUFs - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PORT(url) as PORT FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://test@confluent.io:8080/nested/path?query&jobs', 1);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('1', 0, 0);
ASSERT VALUES `OUTPUT` (K, PORT, ROWTIME) VALUES ('2', 8080, 1);

--@test: url - extract a protocol from a URL using URL_EXTRACT_PROTOCOL - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PROTOCOL(url) as PROTOCOL FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'https://test@confluent.io:8080/nested/path?query&jobs', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'www.confluent.io', 2);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('1', 'http', 0);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('2', 'https', 1);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('3', NULL, 2);

--@test: url - extract a protocol from a URL using URL_EXTRACT_PROTOCOL - AVRO
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PROTOCOL(url) as PROTOCOL FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'https://test@confluent.io:8080/nested/path?query&jobs', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'www.confluent.io', 2);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('1', 'http', 0);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('2', 'https', 1);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('3', NULL, 2);

--@test: url - extract a protocol from a URL using URL_EXTRACT_PROTOCOL - PROTOBUFs - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PROTOCOL(url) as PROTOCOL FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'https://test@confluent.io:8080/nested/path?query&jobs', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'www.confluent.io', 2);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('1', 'http', 0);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('2', 'https', 1);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('3', '', 2);

--@test: url - extract a protocol from a URL using URL_EXTRACT_PROTOCOL - PROTOBUFs - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_PROTOCOL(url) as PROTOCOL FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://test@confluent.io', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'https://test@confluent.io:8080/nested/path?query&jobs', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'www.confluent.io', 2);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('1', 'http', 0);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('2', 'https', 1);
ASSERT VALUES `OUTPUT` (K, PROTOCOL, ROWTIME) VALUES ('3', '', 2);

--@test: url - extract a query from a URL using URL_EXTRACT_QUERY - JSON
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_QUERY(url) as Q FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.test.com/path?q1&q2', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'http://test@confluent.io:8080/nested/path?q=2', 2);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('4', 'http://test@confluent.io:8080/path', 3);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('1', 'query', 0);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('2', 'q1&q2', 1);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('3', 'q=2', 2);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('4', NULL, 3);

--@test: url - extract a query from a URL using URL_EXTRACT_QUERY - AVRO
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_QUERY(url) as Q FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.test.com/path?q1&q2', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'http://test@confluent.io:8080/nested/path?q=2', 2);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('4', 'http://test@confluent.io:8080/path', 3);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('1', 'query', 0);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('2', 'q1&q2', 1);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('3', 'q=2', 2);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('4', NULL, 3);

--@test: url - extract a query from a URL using URL_EXTRACT_QUERY - PROTOBUFs - PROTOBUF
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_QUERY(url) as Q FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.test.com/path?q1&q2', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'http://test@confluent.io:8080/nested/path?q=2', 2);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('4', 'http://test@confluent.io:8080/path', 3);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('1', 'query', 0);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('2', 'q1&q2', 1);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('3', 'q=2', 2);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('4', '', 3);

--@test: url - extract a query from a URL using URL_EXTRACT_QUERY - PROTOBUFs - PROTOBUF_NOSR
CREATE STREAM TEST (K STRING KEY, url VARCHAR) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE STREAM OUTPUT AS SELECT K, URL_EXTRACT_QUERY(url) as Q FROM TEST;
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('1', 'http://www.test.com?query#fragment', 0);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('2', 'http://www.test.com/path?q1&q2', 1);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('3', 'http://test@confluent.io:8080/nested/path?q=2', 2);
INSERT INTO `TEST` (K, url, ROWTIME) VALUES ('4', 'http://test@confluent.io:8080/path', 3);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('1', 'query', 0);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('2', 'q1&q2', 1);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('3', 'q=2', 2);
ASSERT VALUES `OUTPUT` (K, Q, ROWTIME) VALUES ('4', '', 3);

