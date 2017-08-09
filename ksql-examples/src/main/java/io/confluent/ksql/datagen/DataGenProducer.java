/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.datagen;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.physical.GenericRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;

public abstract class DataGenProducer {




  // Max 100 ms between messsages.
  public static final long INTER_MESSAGE_MAX_INTERVAL = 500;

  public void populateTopic(
      Properties props,
      Generator generator,
      String kafkaTopicName,
      String key,
      int messageCount,
      long maxInterval
  ) {
    if (maxInterval < 0) {
      maxInterval = INTER_MESSAGE_MAX_INTERVAL;
    }
    Schema avroSchema = generator.schema();
    org.apache.kafka.connect.data.Schema kafkaSchema = new AvroData(1).toConnectSchema(avroSchema);

    Serializer<GenericRow> serializer = getSerializer(avroSchema, kafkaSchema, kafkaTopicName);

    final KafkaProducer<String, GenericRow> producer = new KafkaProducer<>(props, new StringSerializer(), serializer);

    SessionManager sessionManager = new SessionManager();

    for (int i = 0; i < messageCount; i++) {
      Object generatedObject = generator.generate();

      if (!(generatedObject instanceof GenericRecord)) {
        throw new RuntimeException(String.format(
            "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
            generatedObject.getClass().getName()
        ));
      }
      GenericRecord randomAvroMessage = (GenericRecord) generatedObject;

      List<Object> genericRowValues = new ArrayList<>();

      SimpleDateFormat timeformatter = null;

      /**
       * Populate the record entries
       */
      for (Schema.Field field : avroSchema.getFields()) {

          String isSession = field.schema().getProp("session");
          String timeFormatFromLong = field.schema().getProp("format_as_time");
          if (isSession != null) {
            String currentValue = (String) randomAvroMessage.get(field.name());
            String newCurrentValue = handleSessionisationOfValue(sessionManager, currentValue);


            genericRowValues.add(newCurrentValue);

          } else if (timeFormatFromLong != null) {
              Date date = new Date(System.currentTimeMillis());
              if (timeFormatFromLong.equals("unix_long")) {
                genericRowValues.add(date.getTime());
              } else {
                if (timeformatter == null) {
                  timeformatter = new SimpleDateFormat(timeFormatFromLong);
                }
                genericRowValues.add(timeformatter.format(date));
              }
        } else {
            genericRowValues.add(randomAvroMessage.get(field.name()));
        }
      }

      GenericRow genericRow = new GenericRow(genericRowValues);

      String keyString = genericRowValues.iterator().next().toString();// randomAvroMessage.get(key).toString();

      ProducerRecord<String, GenericRow> producerRecord = new ProducerRecord<>(kafkaTopicName, keyString, genericRow);
      producer.send(producerRecord);
      System.err.println(keyString + " --> (" + genericRow + ")");
      try {
        Thread.sleep((long)(maxInterval * Math.random()));
      } catch (InterruptedException e) {
        // Ignore the exception.
      }
    }
    producer.flush();
    producer.close();
  }


  /**
   * If the sessionId is new Create a Session
   * If the sessionId is active - return the value
   * If the sessionId has expired - use a known token that is not expired
   * @param sessionManager
   * @param currentValue
   * @return
   */
  Set<String> accummulatedSessionTokens = new HashSet<String>();

  private String handleSessionisationOfValue(SessionManager sessionManager, String currentValue) {

    // superset of all values
    accummulatedSessionTokens.add(currentValue);

    /**
     * handle known sessions
     */
    if (sessionManager.isActive(currentValue)) {
      if (sessionManager.isExpired(currentValue)) {
        sessionManager.isActiveAndExpire(currentValue);
        return currentValue;
      } else {
        return currentValue;
      }
    }
    /**
     * If session count maxed out - reuse session tokens
     */
    if (sessionManager.getActiveSessionCount() > sessionManager.getMaxSessions()) {
      return sessionManager.getRandomActiveToken();
    }

    /**
     * Force expiring tokens to expire
     */
    String expired = sessionManager.getActiveSessionThatHasExpired();
    if (expired != null) {
      return expired;
    }

    /**
     * Use accummulated SessionTokens-tokens, or recycle old tokens or blow-up
     */
    String value = getRandomToken(accummulatedSessionTokens);
    while (value != null && sessionManager.isActive(value) || sessionManager.isExpired(value)) {
      value = getRandomToken(accummulatedSessionTokens);
    }

    if (value != null) {
      sessionManager.newSession(value);
    } else {
      value = sessionManager.recycleOldestExpired();
      if (value == null) {
          new RuntimeException("Ran out of tokens to rejuice - increase session-duration (300s), reduce-number of sessions(5), number of tokens in the avro template");
        }
        sessionManager.newSession(value);
        return value;
    }
    return currentValue;

  }

  private String getRandomToken(Set<String> collected){
    if (collected.size() == 0) return null;
    List<String> values = new ArrayList<>(collected);
    int index = (int) (Math.random() * values.size());
    String value = values.remove(index);
    collected.remove(value);
    return value;
  }


  protected abstract Serializer<GenericRow> getSerializer(
      Schema avroSchema,
      org.apache.kafka.connect.data.Schema kafkaSchema,
      String topicName
  );

}