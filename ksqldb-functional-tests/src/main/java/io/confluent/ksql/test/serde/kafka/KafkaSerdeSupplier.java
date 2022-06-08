/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.serde.kafka;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.serde.SerdeSupplier;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaSerdeSupplier implements SerdeSupplier<Object> {

  private final LogicalSchema schema;

  public KafkaSerdeSupplier(final LogicalSchema schema) {
    this.schema = requireNonNull(schema, "schema");
  }

  @Override
  public Serializer<Object> getSerializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new RowSerializer();
  }

  @Override
  public Deserializer<Object> getDeserializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new RowDeserializer();
  }

  private Serde<?> getSerde(final boolean isKey) {
    final List<Column> columns = isKey ? schema.key() : schema.value();
    if (columns.isEmpty()) {
      // Actual serde should be Serdes.Void(), but the test framework uses strings to allow tests
      // to pass in data that should be ignored:
      return Serdes.String();
    }

    if (columns.size() != 1) {
      throw new IllegalStateException("KAFKA format only supports single column schemas.");
    }

    return getSerde(columns.get(0).type());
  }

  private static Serde<?> getSerde(final SqlType sqlType) {
    switch (sqlType.baseType()) {
      case INTEGER:
        return Serdes.Integer();
      case BIGINT:
        return Serdes.serdeFrom(
            new TestNumberSerializer<>(Serdes.Long().serializer(), Long.class, Number::longValue),
            new TestBigIntDeserializer(Serdes.Long().deserializer())
        );
      case DOUBLE:
        return Serdes.serdeFrom(
            new TestNumberSerializer<>(Serdes.Double().serializer(), Double.class,
                Number::doubleValue),
            new TestDoubleDeserializer(Serdes.Double().deserializer())
        );
      case STRING:
        return Serdes.String();
      case BYTES:
        return Serdes.serdeFrom(
            new TestByteBufferSerializer(Serdes.ByteBuffer().serializer()),
            new TestByteBufferDeserializer(Serdes.ByteBuffer().deserializer())
        );
      default:
        throw new IllegalStateException("Unsupported type for KAFKA format");
    }
  }

  private final class RowSerializer implements Serializer<Object> {

    private Serializer<Object> delegate;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      delegate = (Serializer)getSerde(isKey).serializer();
      delegate.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final Object value) {
      return delegate.serialize(topic, value);
    }
  }

  private final class RowDeserializer implements Deserializer<Object> {

    private Deserializer<?> delegate;
    private String type;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      this.type = isKey ? "key" : "value";
      delegate = getSerde(isKey).deserializer();
      delegate.configure(configs, isKey);
    }

    @Override
    public Object deserialize(final String topic, final byte[] bytes) {
      try {
        return delegate.deserialize(topic, bytes);
      } catch (final Exception e) {
        throw new TestFrameworkException("Failed to deserialize " + type + ". "
            + e.getMessage(), e);
      }
    }
  }

  /**
   * Serializer that handles coercion to {@link Double}.
   *
   * <p>The QTT tests are written in JSON. When the input values are read from the JSON file
   * numbers can be deserialized as an {@link Integer}, {@link Long}, or {@link BigDecimal}. The
   * value needs converting to the correct type before serializing.
   */
  private static class TestNumberSerializer<T extends Number> implements Serializer<Object> {

    private final Class<T> type;
    private final Serializer<T> inner;
    private final Function<Number, T> coercer;

    TestNumberSerializer(
        final Serializer<T> inner,
        final Class<T> type,
        final Function<Number, T> coercer
    ) {
      this.inner = requireNonNull(inner, "inner");
      this.type = requireNonNull(type, "type");
      this.coercer = requireNonNull(coercer, "coercer");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topicName, final Object value) {
      final T coerced = coerce(value);
      return inner.serialize(topicName, coerced);
    }

    @Override
    public void close() {
      inner.close();
    }

    private T coerce(final Object value) {
      if (value == null) {
        return null;
      }

      if (value instanceof Number) {
        return coercer.apply((Number) value);
      }

      throw new TestFrameworkException("Can't serialize " + value + " as " + type.getSimpleName());
    }
  }

  /**
   * The QTT tests are written in JSON. When the input values are read from the JSON file, we
   * need to convert from base64 strings to bytebuffers since JSON can't inherently represent byte
   * arrays
   */
  private static class TestByteBufferSerializer implements Serializer<Object> {

    private final Serializer<ByteBuffer> inner;


    TestByteBufferSerializer(
        final Serializer<ByteBuffer> inner
    ) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topicName, final Object value) {
      final ByteBuffer coerced = coerce(value);
      return inner.serialize(topicName, coerced);
    }

    @Override
    public void close() {
      inner.close();
    }

    private ByteBuffer coerce(final Object value) {
      if (value == null) {
        return null;
      }

      if (value instanceof String) {
        return ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8));
      }

      throw new TestFrameworkException("Can't serialize " + value + " as BYTEBUFFER");
    }
  }

  /**
   * Deserializer that handles coercion from {@link Long} to {@link Integer}.
   *
   * <p>The QTT tests are written in JSON. When the expected values are read from the JSON file
   * small numbers are deserialized as {@link Integer Integers}. This is not an issue. However, the
   * deserializer needs to return the same if the actual value matches the expected value.
   */
  private static class TestBigIntDeserializer implements Deserializer<Object> {

    private final Deserializer<Long> inner;

    TestBigIntDeserializer(final Deserializer<Long> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public Number deserialize(final String topicName, final byte[] bytes) {
      final Long deserialized = inner.deserialize(topicName, bytes);
      if (deserialized == null) {
        return null;
      }

      if (Integer.MIN_VALUE < deserialized && deserialized < Integer.MAX_VALUE) {
        return (int) (long) deserialized;
      }

      return deserialized;
    }

    @Override
    public void close() {
      inner.close();
    }
  }

  /**
   * Deserializer that handles coercion from {@link Double} to {@link BigDecimal}.
   *
   * <p>The QTT tests are written in JSON. When the expected values are read from the JSON file
   * doubles are deserialized as {@link BigDecimal BigDecimals}. This is not an issue as BigDecimal
   * is more accurate. However, the deserializer needs to return a `BigDecimal` so that the actual
   * value matches the expected value.
   */
  private static class TestDoubleDeserializer implements Deserializer<Object> {

    private final Deserializer<Double> inner;

    TestDoubleDeserializer(final Deserializer<Double> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public BigDecimal deserialize(final String topicName, final byte[] bytes) {
      final Double deserialized = inner.deserialize(topicName, bytes);
      if (deserialized == null) {
        return null;
      }

      return BigDecimal.valueOf(deserialized);
    }

    @Override
    public void close() {
      inner.close();
    }
  }

  /**
   * The QTT tests are written in JSON, so we have to write a custom deserializer to convert
   * bytebuffers to base64 strings. This is because our tests are written as JSON files,
   * which can't inherently represent byte arrays
   */
  private static class TestByteBufferDeserializer implements Deserializer<Object> {

    private final Deserializer<ByteBuffer> inner;

    TestByteBufferDeserializer(final Deserializer<ByteBuffer> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public String deserialize(final String topicName, final byte[] bytes) {
      final ByteBuffer deserialized = inner.deserialize(topicName, bytes);
      if (deserialized == null) {
        return null;
      }

      return new String(deserialized.array(), StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(final String topic, final Headers headers, final byte[] data) {
      return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
      inner.close();
    }
  }
}