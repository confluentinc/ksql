/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.datagen;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.csv.KQLCsvSerializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CsvProducer {

  final KafkaProducer<String, GenericRow>
      producer;

  public CsvProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "ProductStreamProducers");

    KQLCsvSerializer kqlCsvSerializer = new KQLCsvSerializer();

    producer = new KafkaProducer<String, GenericRow>(props, new StringSerializer(),
                                                     kqlCsvSerializer);
  }


  public void genericRowOrdersStream(String orderKafkaTopicName) {
    long maxInterval = 10;
    int messageCount = 1000;

    for (int i = 0; i < messageCount; i++) {
      long currentTime = System.currentTimeMillis();
      List<Object> columns = new ArrayList();
      currentTime = (long) (1000 * Math.random()) + currentTime;
      // ordertime
      columns.add(Long.valueOf(currentTime));

      //orderid
      columns.add(String.valueOf(i + 1));
      //itemid
      int productId = (int) (10 * Math.random());
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
  }


  public void genericRowUsersStream(String userProfileTopic) {
    long maxInterval = 10;
    int messageCount = 100;

    long currentTime = System.currentTimeMillis();
    for (int i = 0; i < messageCount; i++) {
      currentTime = (long) (1000 * Math.random()) + currentTime;
      List<Object> columns = new ArrayList();
      // time (not being used!!!)
      columns.add(currentTime);

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

      ProducerRecord producerRecord = new ProducerRecord(userProfileTopic, userIDStr, genericRow);

      producer.send(producerRecord);
      System.out.println(i + " : " + userIDStr + " --> (" + genericRow + ")");

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

    long currentTime = System.currentTimeMillis();
    for (int i = 0; i < messageCount; i++) {
      currentTime = (long) (1000 * Math.random()) + currentTime;
      List<Object> columns = new ArrayList();
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

      ProducerRecord producerRecord = new ProducerRecord(pageViewTopic, pageIdStr, genericRow);

      producer.send(producerRecord);
      System.out.println(i + " : " + pageIdStr + " --> (" + genericRow + ")");
      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Done!");
  }

  public static void main(String[] args) {
    new CsvProducer().genericRowOrdersStream("Orders_csv");
  }

}
