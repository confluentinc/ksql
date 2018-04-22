/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.datagen;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.GenericRow;

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

    final KafkaProducer<String, GenericRow> producer = new KafkaProducer<>(
        props,
        new StringSerializer(),
        serializer
    );

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
      String sessionisationValue = null;
      for (Schema.Field field : avroSchema.getFields()) {

        boolean isSession = field.schema().getProp("session") != null;
        boolean isSessionSiblingIntHash =
            field.schema().getProp("session-sibling-int-hash") != null;
        String timeFormatFromLong = field.schema().getProp("format_as_time");

        if (isSession) {
          String currentValue = (String) randomAvroMessage.get(field.name());
          String newCurrentValue = handleSessionisationOfValue(sessionManager, currentValue);
          sessionisationValue = newCurrentValue;

          genericRowValues.add(newCurrentValue);
        } else if (isSessionSiblingIntHash && sessionisationValue != null) {

          // super cheeky hack to link int-ids to session-values - if anything fails then we use
          // the 'avro-gen' randomised version
          handleSessionSiblingField(
              randomAvroMessage,
              genericRowValues,
              sessionisationValue,
              field
          );

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

      String keyString = randomAvroMessage.get(key).toString();

      ProducerRecord<String, GenericRow> producerRecord = new ProducerRecord<>(
          kafkaTopicName,
          keyString,
          genericRow
      );
      producer.send(producerRecord);
      System.err.println(keyString + " --> (" + genericRow + ")");
      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        // Ignore the exception.
      }
    }
    producer.flush();
    producer.close();
  }

  private void handleSessionSiblingField(
      GenericRecord randomAvroMessage,
      List<Object> genericRowValues,
      String sessionisationValue,
      Schema.Field field
  ) {
    try {
      Schema.Type type = field.schema().getType();
      if (type == Schema.Type.INT) {
        genericRowValues.add(mapSessionValueToSibling(sessionisationValue, field));
      } else {
        genericRowValues.add(randomAvroMessage.get(field.name()));
      }
    } catch (Exception err) {
      genericRowValues.add(randomAvroMessage.get(field.name()));
    }
  }

  Map<String, Integer> sessionMap = new HashMap<>();
  Set<Integer> allocatedIds = new HashSet<>();

  private int mapSessionValueToSibling(String sessionisationValue, Schema.Field field) {

    if (!sessionMap.containsKey(sessionisationValue)) {

      LinkedHashMap properties =
          (LinkedHashMap) field.schema().getObjectProps().get("arg.properties");
      Integer max = (Integer) ((LinkedHashMap) properties.get("range")).get("max");

      int vvalue = Math.abs(sessionisationValue.hashCode() % max);

      int foundValue = -1;
      // used - search for another
      if (allocatedIds.contains(vvalue)) {
        for (int i = 0; i < max; i++) {
          if (!allocatedIds.contains(i)) {
            foundValue = i;
          }
        }
        if (foundValue == -1) {
          System.out.println(
              "Failed to allocate Id :"
              + sessionisationValue
              + ", reusing "
              + vvalue
          );
          foundValue = vvalue;
        }
        vvalue = foundValue;
      }
      allocatedIds.add(vvalue);
      sessionMap.put(sessionisationValue, vvalue);
    }
    return sessionMap.get(sessionisationValue);

  }

  Set<String> allTokens = new HashSet<String>();

  /**
   * If the sessionId is new Create a Session
   * If the sessionId is active - return the value
   * If the sessionId has expired - use a known token that is not expired
   *
   * @param sessionManager a SessionManager
   * @param currentValue current token
   * @return session token
   */
  private String handleSessionisationOfValue(SessionManager sessionManager, String currentValue) {

    // superset of all values
    allTokens.add(currentValue);

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
    String value = null;
    for (String token : allTokens) {
      if (value == null) {
        if (!sessionManager.isActive(token) && !sessionManager.isExpired(token)) {
          value = token;
        }
      }
    }

    if (value != null) {
      sessionManager.newSession(value);
    } else {
      value = sessionManager.recycleOldestExpired();
      if (value == null) {
        throw new RuntimeException(
            "Ran out of tokens to rejuice - increase session-duration (300s), reduce-number of "
            + "sessions(5), number of tokens in the avro template");
      }
      sessionManager.newSession(value);
      return value;
    }
    return currentValue;
  }

  private String getRandomToken(Set<String> collected) {
    if (collected.size() == 0) {
      return null;
    }
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
