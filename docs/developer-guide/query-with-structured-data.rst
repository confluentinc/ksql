.. _query-with-structured-data:

Query With Structured Data
##########################

.. code:: sql


docker run -d --network tutorials_default --rm --name datagen-orders \
  confluentinc/ksql-examples:5.0.0 \
  ksql-datagen \
      bootstrap-server=kafka:39092 \
      quickstart=orders \
      format=avro \
      topic=orders \
      maxInterval=100 \
      schemaRegistryUrl=http://schema-registry:8081

CREATE STREAM orders WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='avro');

 Message
----------------
 Stream created
----------------

print 'orders';

{"ROWTIME":1543890360783,"ROWKEY":"3526","ordertime":1502818154562,"orderid":3526,"itemid":"Item_632","orderunits":1.2619795502512994,"address":{"city":"City_85","state":"State_69","zipcode":74620}}
{"ROWTIME":1543890360876,"ROWKEY":"3527","ordertime":1488179461841,"orderid":3527,"itemid":"Item_876","orderunits":4.128391857319351,"address":{"city":"City_97","state":"State_75","zipcode":81504}}
{"ROWTIME":1543890360944,"ROWKEY":"3528","ordertime":1508034306661,"orderid":3528,"itemid":"Item_357","orderunits":4.848170238630841,"address":{"city":"City_38","state":"State_67","zipcode":66000}}
Topic printing ceased


DESCRIBE ORDERS;

Name                 : ORDERS
 Field      | Type
----------------------------------------------------------------------------------
 ROWTIME    | BIGINT           (system)
 ROWKEY     | VARCHAR(STRING)  (system)
 ORDERTIME  | BIGINT
 ORDERID    | INTEGER
 ITEMID     | VARCHAR(STRING)
 ORDERUNITS | DOUBLE
 ADDRESS    | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE BIGINT>
----------------------------------------------------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;

SELECT ORDERID, ADDRESS->CITY FROM ORDERS;

6598 | City_52
6599 | City_73
6600 | City_55
^CQuery terminated

Press Ctrl+C to cancel the SELECT query.


