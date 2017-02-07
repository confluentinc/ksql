/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.datagen;

public class DataGen {


  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage:");
      System.out.println("DataGen format=<format> topic=<topic name>");
    }

    String format = args[0].split("=")[1].trim().toUpperCase();
    String topicName = args[1].split("=")[1].trim().toUpperCase();

    if (format.equalsIgnoreCase("JSON")) {
      JsonProducer jsonProducer = new JsonProducer();
      if (topicName.equalsIgnoreCase("orders")) {
        jsonProducer.genericRowOrdersStream("orders_kafka_topic_json");
      } else if (topicName.equalsIgnoreCase("users")) {
        jsonProducer.genericRowUsersStream("users_kafka_topic_json");
      } else if (topicName.equalsIgnoreCase("pageview")) {
        jsonProducer.genericRowPageViewStream("pageview_kafka_topic_json");
      }
    } else if (format.equalsIgnoreCase("AVRO")) {
      AvroProducer avroProducer = new AvroProducer();
      if (topicName.equalsIgnoreCase("orders")) {
        avroProducer.genericRowOrdersStream("orders_kafka_topic_avro");
      } else if (topicName.equalsIgnoreCase("users")) {
        avroProducer.genericRowUsersStream("users_kafka_topic_avro");
      } else if (topicName.equalsIgnoreCase("pageview")) {
        avroProducer.genericRowPageViewStream("pageview_kafka_topic_avro");
      }
    } else if (format.equalsIgnoreCase("CSV")) {
      CsvProducer csvProducer = new CsvProducer();
      if (topicName.equalsIgnoreCase("orders")) {
        csvProducer.genericRowOrdersStream("orders_kafka_topic_csv");
      } else if (topicName.equalsIgnoreCase("users")) {
        csvProducer.genericRowUsersStream("users_kafka_topic_csv");
      } else if (topicName.equalsIgnoreCase("pageview")) {
        csvProducer.genericRowPageViewStream("pageview_kafka_topic_csv");
      }
    }
  }
}
