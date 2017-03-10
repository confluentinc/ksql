/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.datagen;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.avro.KQLGenericRowAvroSerializer;
import io.confluent.kql.util.KQLConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class AvroProducer {

  public List<String> genericRowOrdersStream(String orderKafkaTopicName) {
    long maxInterval = 10;
    int messageCount = 1000;
    List<String> orderList = new ArrayList<>();
    String schemaStr = "{"
                       + "\"namespace\": \"kql\","
                       + " \"name\": \"orders\","
                       + " \"type\": \"record\","
                       + " \"fields\": ["
                       + "     {\"name\": \"ordertime\", \"type\": \"long\"},"
                       + "     {\"name\": \"orderid\",  \"type\": \"string\"},"
                       + "     {\"name\": \"itemid\", \"type\": \"string\"},"
                       + "     {\"name\": \"orderunits\", \"type\": \"double\"}"
                       + " ]"
                       + "}";
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "ProductStreamProducers");

    KQLGenericRowAvroSerializer kqlGenericRowAvroSerializer = new KQLGenericRowAvroSerializer();
    Map map = new HashMap();
    map.put(KQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);
    kqlGenericRowAvroSerializer.configure(map, false);

    final KafkaProducer<String, GenericRow>
        producer =
        new KafkaProducer<String, GenericRow>(props, new StringSerializer(),
                                              kqlGenericRowAvroSerializer);

    for (int i = 0; i < messageCount; i++) {
      long currentTime = System.currentTimeMillis();
      List<Object> columns = new ArrayList();
      currentTime = (long) (1000 * Math.random()) + currentTime;
      // ordertime
      columns.add(Long.valueOf(currentTime));

      //orderid
      columns.add(String.valueOf(i + 1));
      orderList.add(String.valueOf(i + 1));
      //itemid
      int productId = (int) (100 * Math.random());
      columns.add("Item_" + productId);

      //units
      columns.add((double) ((int) (10 * Math.random())));
      GenericRow genericRow = new GenericRow(columns);

      ProducerRecord
          producerRecord =
          new ProducerRecord(orderKafkaTopicName, String.valueOf(currentTime), genericRow);

      producer.send(producerRecord);
      System.out.println(currentTime + " --> (" + genericRow + ")");

      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Done!");
    return orderList;
  }

  public void genericRowShipmentStream(String shipmentKafkaTopicName, List<String> orderList) {
    long maxInterval = 10;
    int messageCount = 1000;
    String schemaStr = "{"
                       + "\"namespace\": \"kql\","
                       + " \"name\": \"shipments\","
                       + " \"type\": \"record\","
                       + " \"fields\": ["
                       + "     {\"name\": \"shipmenttime\", \"type\": \"long\"},"
                       + "     {\"name\": \"shipmentid\",  \"type\": \"string\"},"
                       + "     {\"name\": \"orderid\", \"type\": \"string\"}"
                       + " ]"
                       + "}";
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "ProductStreamProducers");

    KQLGenericRowAvroSerializer kqlGenericRowAvroSerializer = new KQLGenericRowAvroSerializer();
    Map map = new HashMap();
    map.put(KQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);
    kqlGenericRowAvroSerializer.configure(map, false);

    final KafkaProducer<String, GenericRow>
        producer =
        new KafkaProducer<String, GenericRow>(props, new StringSerializer(),
                                              kqlGenericRowAvroSerializer);

    for (int i = 0; i < messageCount; i++) {
      long currentTime = System.currentTimeMillis();
      List<Object> columns = new ArrayList();
      currentTime = (long) (1000 * Math.random()) + currentTime;
      // shipment
      columns.add(Long.valueOf(currentTime));

      //shipmentid
      columns.add(String.valueOf(i + 1));
      //orderid
      String orderId = orderList.remove((int) (orderList.size() * Math.random()));
      columns.add(orderId);

      GenericRow genericRow = new GenericRow(columns);

      ProducerRecord
          producerRecord =
          new ProducerRecord(shipmentKafkaTopicName, String.valueOf(currentTime), genericRow);

      producer.send(producerRecord);
      System.out.println(currentTime + " --> (" + genericRow + ")");

      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Done!");
  }

  public void genericRowItemTable(String itemKafkaTopicName) {
    long maxInterval = 10;
    int messageCount = 100;
    String schemaStr = "{"
                       + "\"namespace\": \"kql\","
                       + " \"name\": \"items\","
                       + " \"type\": \"record\","
                       + " \"fields\": ["
                       + "     {\"name\": \"itemid\", \"type\": \"string\"},"
                       + "     {\"name\": \"name\",  \"type\": \"string\"},"
                       + "     {\"name\": \"price\", \"type\": \"double\"},"
                       + "     {\"name\": \"category\", \"type\": \"string\"}"
                       + " ]"
                       + "}";
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "ProductStreamProducers");

    KQLGenericRowAvroSerializer kqlGenericRowAvroSerializer = new KQLGenericRowAvroSerializer();
    Map map = new HashMap();
    map.put(kqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);
    kqlGenericRowAvroSerializer.configure(map, false);

    final KafkaProducer<String, GenericRow>
        producer =
        new KafkaProducer<String, GenericRow>(props, new StringSerializer(),
                                              kqlGenericRowAvroSerializer);

    for (int i = 0; i < messageCount; i++) {
      List<Object> columns = new ArrayList();
      // ItenmId
      String itemId = "Item_" + i;
      columns.add(itemId);

      //Name
      columns.add("ITEM_" + i);

      double itemPrice = (double) (100 * Math.random());
      columns.add(itemPrice);

      int categoryId = (int) (10 * Math.random());
      columns.add("Category_" + categoryId);

      GenericRow genericRow = new GenericRow(columns);

      ProducerRecord
          producerRecord = new ProducerRecord(itemKafkaTopicName, itemId, genericRow);

      producer.send(producerRecord);
      System.out.println(itemId + " --> (" + genericRow + ")");

      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Done!");
  }

  public void genericRowUsersStream(String userProfileTopic) {
    long maxInterval = 10;
    int messageCount = 100;

    String schemaStr = "{"
                       + "\"namespace\": \"kql\","
                       + " \"name\": \"users\","
                       + " \"type\": \"record\","
                       + " \"fields\": ["
                       + "     {\"name\": \"ordertime\", \"type\": \"long\"},"
                       + "     {\"name\": \"userid\",  \"type\": \"string\"},"
                       + "     {\"name\": \"regionid\", \"type\": \"string\"},"
                       + "     {\"name\": \"gender\", \"type\": \"string\"}"
                       + " ]"
                       + "}";

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "ProductStreamProducers");

    KQLGenericRowAvroSerializer kqlGenericRowAvroSerializer = new KQLGenericRowAvroSerializer();
    Map map = new HashMap();
    map.put(kqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);
    kqlGenericRowAvroSerializer.configure(map, false);

    final KafkaProducer<String, GenericRow>
        producer =
        new KafkaProducer<String, GenericRow>(props, new StringSerializer(),
                                              kqlGenericRowAvroSerializer);

    for (int i = 0; i < messageCount; i++) {
      long timestamp = System.currentTimeMillis();
      List<Object> columns = new ArrayList();

      columns.add(timestamp);
      //userId
      int userId = i;
      String userIDStr = "User_" + userId;
      columns.add(userIDStr);

      //region
      int regionId = (int) (10 * Math.random());
      columns.add("Region_" + regionId);

      if (Math.random() > 0.5) {
        columns.add("MALE");
      } else {
        columns.add("FEMALE");
      }

      GenericRow genericRow = new GenericRow(columns);

      ProducerRecord
          producerRecord =
          new ProducerRecord(userProfileTopic, String.valueOf(timestamp), genericRow);

      producer.send(producerRecord);
      System.out.println(timestamp + " --> (" + genericRow + ")");

      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Done!");
  }

  public void genericRowPageViewStream(String pageViewTopic) {
    long maxInterval = 10;
    int messageCount = 1000;

    String schemaStr = "{"
                       + "\"namespace\": \"kql\","
                       + " \"name\": \"pageview\","
                       + " \"type\": \"record\","
                       + " \"fields\": ["
                       + "     {\"name\": \"viewtime\", \"type\": \"long\"},"
                       + "     {\"name\": \"userid\",  \"type\": \"string\"},"
                       + "     {\"name\": \"pageid\", \"type\": \"string\"}"
                       + " ]"
                       + "}";

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "ProductStreamProducers");

    KQLGenericRowAvroSerializer kqlGenericRowAvroSerializer = new KQLGenericRowAvroSerializer();
    Map map = new HashMap();
    map.put(kqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);
    kqlGenericRowAvroSerializer.configure(map, false);

    final KafkaProducer<String, GenericRow>
        producer =
        new KafkaProducer<String, GenericRow>(props, new StringSerializer(),
                                              kqlGenericRowAvroSerializer);

    for (int i = 0; i < messageCount; i++) {
      long currentTime = System.currentTimeMillis();
      List<Object> columns = new ArrayList();
      currentTime = (long) (1000 * Math.random()) + currentTime;
      // time (not being used!!!)
      columns.add(currentTime);

      //userId
      int userId = (int) (100 * Math.random());
      columns.add("User_" + userId);

      //pageid
      int pageId = (int) (1000 * Math.random());
      String pageIdStr = "Page_" + pageId;
      columns.add(pageIdStr);

      GenericRow genericRow = new GenericRow(columns);

      ProducerRecord
          producerRecord =
          new ProducerRecord(pageViewTopic, String.valueOf(currentTime), genericRow);

      producer.send(producerRecord);
      System.out.println(currentTime + " --> (" + genericRow + ")");

      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Done!");
  }

  public static void main(String[] args) {
    AvroProducer avroProducer = new AvroProducer();
//    avroProducer.genericRowItemTable("items_topic");
    List<String> orders = avroProducer.genericRowOrdersStream("orders_topic");
//    avroProducer.genericRowShipmentStream("shipments",orders);

//    new AvroProducer().genericRowUserStream();
//    new AvroProducer().genericRowPageViewStream();
  }
}
