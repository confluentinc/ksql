/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.datagen;

import static io.confluent.ksql.datagen.DataGenSchemaUtil.getOptionalSchema;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class RowGenerator {

  static final ConnectSchema KEY_SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field(SchemaUtil.ROWKEY_NAME, org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private final Set<String> allTokens = new HashSet<>();
  private final Map<String, Integer> sessionMap = new HashMap<>();
  private final Set<Integer> allocatedIds = new HashSet<>();
  private final Generator generator;
  private final AvroData avroData;
  private final LogicalSchema ksqlSchema;
  private final SessionManager sessionManager = new SessionManager();
  private final String key;

  public RowGenerator(final Generator generator, final String key) {
    this.generator = Objects.requireNonNull(generator, "generator");
    this.avroData = new AvroData(1);
    this.key = Objects.requireNonNull(key, "key");
    this.ksqlSchema = LogicalSchema.of(
        KEY_SCHEMA,
        getOptionalSchema(avroData.toConnectSchema(generator.schema()))
    );

    if (!ksqlSchema.findValueField(key).isPresent()) {
      throw new IllegalArgumentException("key field does not exist in schema: " + key);
    }
  }

  public LogicalSchema schema() {
    return ksqlSchema;
  }

  public Pair<Struct, GenericRow> generateRow() {

    final Object generatedObject = generator.generate();

    if (!(generatedObject instanceof GenericRecord)) {
      throw new RuntimeException(String.format(
          "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
          generatedObject.getClass().getName()
      ));
    }
    final GenericRecord randomAvroMessage = (GenericRecord) generatedObject;

    final List<Object> genericRowValues = new ArrayList<>();

    SimpleDateFormat timeformatter = null;

    /*
     * Populate the record entries
     */
    String sessionisationValue = null;
    for (final Schema.Field field : generator.schema().getFields()) {

      final boolean isSession = field.schema().getProp("session") != null;
      final boolean isSessionSiblingIntHash =
          field.schema().getProp("session-sibling-int-hash") != null;
      final String timeFormatFromLong = field.schema().getProp("format_as_time");

      if (isSession) {
        final String currentValue = (String) randomAvroMessage.get(field.name());
        final String newCurrentValue = handleSessionisationOfValue(sessionManager, currentValue);
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
        final Date date = new Date(System.currentTimeMillis());
        if (timeFormatFromLong.equals("unix_long")) {
          genericRowValues.add(date.getTime());
        } else {
          if (timeformatter == null) {
            timeformatter = new SimpleDateFormat(timeFormatFromLong);
          }
          genericRowValues.add(timeformatter.format(date));
        }
      } else {
        final Object value = randomAvroMessage.get(field.name());
        if (value instanceof Record) {
          final Field ksqlField = ksqlSchema.valueSchema().field(field.name());
          final Record record = (Record) value;
          final Object ksqlValue = avroData.toConnectData(record.getSchema(), record).value();
          genericRowValues.add(DataGenSchemaUtil.getOptionalValue(ksqlField.schema(), ksqlValue));
        } else {
          genericRowValues.add(value);
        }
      }
    }

    final String keyString = avroData.toConnectData(
        randomAvroMessage.getSchema().getField(key).schema(),
        randomAvroMessage.get(key)).value().toString();

    final Struct key = new Struct(KEY_SCHEMA);
    key.put(SchemaUtil.ROWKEY_NAME, keyString);

    return Pair.of(key, new GenericRow(genericRowValues));
  }

  /**
   * If the sessionId is new Create a Session If the sessionId is active - return the value If the
   * sessionId has expired - use a known token that is not expired
   *
   * @param sessionManager a SessionManager
   * @param currentValue current token
   * @return session token
   */
  private String handleSessionisationOfValue(
      final SessionManager sessionManager,
      final String currentValue) {

    // superset of all values
    allTokens.add(currentValue);

    /*
     * handle known sessions
     */
    if (sessionManager.isActive(currentValue)) {
      sessionManager.isActiveAndExpire(currentValue);
      return currentValue;
    }
    /*
     * If session count maxed out - reuse session tokens
     */
    if (sessionManager.getActiveSessionCount() > sessionManager.getMaxSessions()) {
      return sessionManager.getRandomActiveToken();
    }

    /*
     * Force expiring tokens to expire
     */
    final String expired = sessionManager.getActiveSessionThatHasExpired();
    if (expired != null) {
      return expired;
    }

    /*
     * Use accummulated SessionTokens-tokens, or recycle old tokens or blow-up
     */
    String value = null;
    for (final String token : allTokens) {
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

  private void handleSessionSiblingField(
      final GenericRecord randomAvroMessage,
      final List<Object> genericRowValues,
      final String sessionisationValue,
      final Schema.Field field
  ) {
    try {
      final Schema.Type type = field.schema().getType();
      if (type == Schema.Type.INT) {
        genericRowValues.add(mapSessionValueToSibling(sessionisationValue, field));
      } else {
        genericRowValues.add(randomAvroMessage.get(field.name()));
      }
    } catch (final Exception err) {
      genericRowValues.add(randomAvroMessage.get(field.name()));
    }
  }

  private int mapSessionValueToSibling(final String sessionisationValue, final Schema.Field field) {

    if (!sessionMap.containsKey(sessionisationValue)) {

      final LinkedHashMap<?, ?> properties =
          (LinkedHashMap) field.schema().getObjectProps().get("arg.properties");
      final Integer max = (Integer) ((LinkedHashMap) properties.get("range")).get("max");

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
}
