/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde;

import com.google.common.collect.Sets;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import io.confluent.ksql.serde.unwrapped.UnwrappedDeserializer;
import io.confluent.ksql.serde.unwrapped.UnwrappedSerializer;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class SerdeUtils {

  private SerdeUtils() {
  }

  public static void throwOnUnsupportedFeatures(
      final SerdeFeatures requestedFeatures,
      final Set<SerdeFeature> supportedFeatures
  ) {
    final Set<SerdeFeature> unsupported = Sets
        .difference(requestedFeatures.all(), supportedFeatures);

    if (!unsupported.isEmpty()) {
      throw new IllegalArgumentException("Unsupported features: " + unsupported);
    }
  }

  /**
   * Applies any single column unwrapping.
   *
   * @param schema the schema of the key or value columns. Must be a {@code Struct}.
   * @param features the serde features to apply.
   * @return If the supplied {@code features} includes {@link SerdeFeature#UNWRAP_SINGLES} the
   *         returned schema will be unwrapped, i.e. an anonymous type.
   */
  public static ConnectSchema applySinglesUnwrapping(
      final Schema schema,
      final SerdeFeatures features
  ) {
    if (!features.enabled(SerdeFeature.UNWRAP_SINGLES)) {
      return (ConnectSchema) schema;
    }

    final boolean singleField = schema.fields().size() == 1;
    if (!singleField) {
      throw new IllegalArgumentException("Unwrapping only valid for single columns");
    }

    return (ConnectSchema) schema.fields().get(0).schema();
  }

  public static Schema wrapSingle(final Schema fieldSchema, final boolean isKey) {
    final String fieldName = isKey ? "ROWKEY" : "ROWVAL";
    return SchemaBuilder.struct()
        .field(fieldName, fieldSchema)
        .build();
  }

  public static <T> Serializer<List<?>> unwrappedSerializer(
      final Serializer<T> inner,
      final Class<T> type
  ) {
    return new UnwrappedSerializer<>(inner, type);
  }

  public static Deserializer<List<?>> unwrappedDeserializer(final Deserializer<?> inner) {
    return new UnwrappedDeserializer(inner);
  }

  @SuppressWarnings("unchecked")
  public static <T> T safeCast(final Object value, final Class<T> javaType) {
    if (value != null && !javaType.isAssignableFrom(value.getClass())) {
      throw new SerializationException("value does not match expected type. "
          + "expected: " + javaType.getSimpleName()
          + ", but got: " + value.getClass().getSimpleName());
    }

    return (T) value;
  }

  public static void throwOnSchemaJavaTypeMismatch(
      final Schema schema,
      final Class<?> javaType
  ) {
    final Class<?> schemaType = SchemaConverters.connectToJavaTypeConverter().toJavaType(schema);
    if (!schemaType.equals(javaType)) {
      throw new IllegalArgumentException("schema does not match expected java type. "
          + "Expected: " + javaType + ", but got " + schemaType);
    }
  }

  public static void throwOnColumnCountMismatch(
      final int expectedCount,
      final int actualCount,
      final boolean serialization,
      final String topic
  ) {
    if (expectedCount == actualCount) {
      return;
    }

    throw new SerializationException("Column count mismatch on "
        + (serialization ? "serialization" : "deserialization")
        + ". topic: " + topic
        + ", expected: " + expectedCount
        + ", got: " + actualCount
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> T castToTargetType(final Object val, final Class<T> type) {
    if (val != null && !type.isAssignableFrom(val.getClass())) {
      throw new SerializationException("Data deserialized to wrong type. "
          + "Expected type: " + type + ", but got: " + val);
    }

    return (T) val;
  }

  public static Time returnTimeOrThrow(final long time) {
    if (time >= 0 && time <= TimeUnit.DAYS.toMillis(1)) {
      return new Time(time);
    } else {
      throw new IllegalArgumentException(
          "Time values must use number of milliseconds greater than 0 and less than "
              + TimeUnit.DAYS.toMillis(1) + ".");
    }
  }

  public static Date getDateFromEpochDays(final long days) {
    return new Date(TimeUnit.DAYS.toMillis(days));
  }

  public static int toEpochDays(final Date date) {
    final long days = TimeUnit.MILLISECONDS.toDays(date.getTime());
    if (date.getTime() == TimeUnit.DAYS.toMillis(days)) {
      return (int) days;
    } else {
      throw new IllegalArgumentException(
          "Date type should not have any time fields set to non-zero values.");
    }
  }

  public static ParsedSchema getParsedSchemaById(
      final Supplier<SchemaRegistryClient> srFactory,
      final int schemaId
  ) {
    try {
      return srFactory.get().getSchemaById(schemaId);
    } catch (RestClientException | IOException e) {
      throw new KsqlException("Failed to fetch schema for serialization from Schema Registry "
          + "using schema id: " + schemaId, e);
    }
  }

  public static int getLatestSchemaId(
      final Supplier<SchemaRegistryClient> srFactory,
      final String subjectName
  ) {
    try {
      return srFactory.get().getLatestSchemaMetadata(subjectName).getId();
    } catch (RestClientException | IOException e) {
      throw new KsqlException("Failed to fetch schema for serialization from Schema Registry "
          + "using schema subject: " + subjectName, e);
    }
  }

  public static Schema getAndTranslateSchemaById(
      final Supplier<SchemaRegistryClient> srFactory,
      final int schemaId,
      final ConnectSchemaTranslator translator
  ) {
    try {
      final ParsedSchema parsedSchema = srFactory.get().getSchemaById(schemaId);
      return translator.toConnectSchema(parsedSchema);
    } catch (RestClientException | IOException e) {
      throw new KsqlException("Failed to fetch schema for serialization from Schema Registry "
          + "using schema id: " + schemaId, e);
    }
  }
}
