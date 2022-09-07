--@test: create-type - create simple type
CREATE TYPE ADDRESS AS STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;
CREATE TABLE addresses (K STRING PRIMARY KEY, address ADDRESS) WITH (kafka_topic='test', value_format='JSON');
CREATE TABLE copy AS SELECT * FROM addresses;
INSERT INTO `ADDRESSES` (K, address) VALUES ('a', STRUCT(number:=899, street:='W. Evelyn', city:='Mountain View'));
ASSERT VALUES `COPY` (K, ADDRESS) VALUES ('a', STRUCT(NUMBER:=899, STREET:='W. Evelyn', CITY:='Mountain View'));

--@test: create-type - create nested type
CREATE TYPE ADDRESS AS STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;
CREATE TYPE PERSON AS STRUCT<name VARCHAR, address ADDRESS>;
CREATE TABLE people (K STRING PRIMARY KEY, person PERSON) WITH (kafka_topic='test', value_format='JSON');
CREATE TABLE copy AS SELECT * FROM people;
INSERT INTO `PEOPLE` (K, person) VALUES ('a', STRUCT(name:='jay', address:=STRUCT(number:=899, street:='W. Evelyn', city:='Mountain View')));
ASSERT VALUES `COPY` (K, PERSON) VALUES ('a', STRUCT(NAME:='jay', ADDRESS:=STRUCT(NUMBER:=899, STREET:='W. Evelyn', CITY:='Mountain View')));

