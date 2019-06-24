/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

/**
 * Performs validation of a CREATE statement's WITH clause.
 */
@Immutable
public final class CreateSourceProperties {

  private static final Set<String> VALID_PROPERTIES = ImmutableSet.<String>builder()
      .add(DdlConfig.VALUE_FORMAT_PROPERTY.toUpperCase())
      .add(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY.toUpperCase())
      .add(DdlConfig.KEY_NAME_PROPERTY.toUpperCase())
      .add(DdlConfig.WINDOW_TYPE_PROPERTY.toUpperCase())
      .add(DdlConfig.TIMESTAMP_NAME_PROPERTY.toUpperCase())
      .add(KsqlConstants.AVRO_SCHEMA_ID.toUpperCase())
      .add(DdlConfig.TIMESTAMP_FORMAT_PROPERTY.toUpperCase())
      .add(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME.toUpperCase())
      .add(KsqlConstants.SOURCE_NUMBER_OF_PARTITIONS.toUpperCase())
      .add(KsqlConstants.SOURCE_NUMBER_OF_REPLICAS.toUpperCase())
      .add(DdlConfig.WRAP_SINGLE_VALUE.toUpperCase())
      .build();

  private static final java.util.Map<String, SerdeFactory<Windowed<String>>> WINDOW_TYPES =
      ImmutableMap.of(
        "SESSION", () -> WindowedSerdes.sessionWindowedSerdeFrom(String.class),
        "TUMBLING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class),
        "HOPPING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class)
      );

  // required
  private final Property<String> kafkaTopic;
  private final Property<Format> valueFormat;

  // optional
  private final Property<String> key;
  private final Property<String> windowType;
  private final Property<String> timestampName;
  private final Property<String> timestampFormat;
  private final Property<String> avroSchemaName;
  private final Property<Integer> avroSchemaId;
  private final Property<Integer> partitions;
  private final Property<Short> replicas;
  private final Property<Boolean> wrapSingleValues;

  public CreateSourceProperties(final Map<String, Literal> original) {
    final Map<String, Literal> properties = original
        .entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey().toUpperCase(), Entry::getValue));

    properties.keySet()
        .stream()
        .filter(cfg -> !VALID_PROPERTIES.contains(cfg))
        .findFirst()
        .ifPresent(config -> {
          throw new KsqlException("Invalid config variable in the WITH clause: " + config);
        });

    valueFormat = Property.from(
        DdlConfig.VALUE_FORMAT_PROPERTY, properties, val -> Format.of(val.toString()));
    if (!valueFormat.isPresent()) {
      throw new KsqlException("Topic format(VALUE_FORMAT) should be set in WITH clause.");
    }

    kafkaTopic = Property.from(
        DdlConfig.KAFKA_TOPIC_NAME_PROPERTY,
        properties,
        ((Function<Object, String>) Object::toString).andThen(StringUtil::cleanQuotes));
    if (!kafkaTopic.isPresent()) {
      throw new KsqlException(
          "Corresponding Kafka topic (KAFKA_TOPIC) should be set in WITH clause.");
    }

    key = Property.from(DdlConfig.KEY_NAME_PROPERTY, properties);
    timestampName = Property.from(
        DdlConfig.TIMESTAMP_NAME_PROPERTY,
        properties,
        ((Function<Object, String>) Object::toString).andThen(StringUtil::cleanQuotes));
    timestampFormat = Property.from(
        DdlConfig.TIMESTAMP_FORMAT_PROPERTY,
        properties,
        ((Function<Object, String>) Object::toString).andThen(StringUtil::cleanQuotes));

    windowType = Property.from(
        DdlConfig.WINDOW_TYPE_PROPERTY,
        properties,
        ((Function<Object, String>) Object::toString).andThen(String::toUpperCase));
    if (windowType.isPresent() && !WINDOW_TYPES.containsKey(windowType.value)) {
      throw new KsqlException(
          "WINDOW_TYPE property is not set correctly. value: " + windowType.value
              + ", validValues; " + WINDOW_TYPES.keySet());
    }

    avroSchemaId = Property.from(
        KsqlConstants.AVRO_SCHEMA_ID,
        properties,
        id -> Integer.parseInt(id.toString()));
    avroSchemaName = Property.from(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME, properties);

    partitions = Property.from(
        KsqlConstants.SOURCE_NUMBER_OF_PARTITIONS,
        properties,
        partitions -> Integer.parseInt(partitions.toString())
    );

    replicas = Property.from(
        KsqlConstants.SOURCE_NUMBER_OF_REPLICAS,
        properties,
        replicas -> Short.parseShort(replicas.toString())
    );

    wrapSingleValues = Property.from(
        DdlConfig.WRAP_SINGLE_VALUE,
        properties,
        v -> Boolean.parseBoolean(v.toString())
    );
  }

  public String toString() {
    return Stream.<Property<?>>builder()
        .add(valueFormat)
        .add(kafkaTopic)
        .add(key)
        .add(timestampName)
        .add(timestampFormat)
        .add(windowType)
        .add(avroSchemaId)
        .add(avroSchemaName)
        .add(partitions)
        .add(replicas)
        .add(wrapSingleValues)
        .build()
        .filter(Property::isPresent)
        .map(Object::toString)
        .collect(Collectors.joining(", "));
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  private CreateSourceProperties(
      final Property<String> timestampName,
      final Property<String> windowType,
      final Property<String> key,
      final Property<String> kafkaTopic,
      final Property<Format> valueFormat,
      final Property<String> avroSchemaName,
      final Property<Integer> avroSchemaId,
      final Property<String> timestampFormat,
      final Property<Integer> partitions,
      final Property<Short> replicas,
      final Property<Boolean> wrapSingleValues
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");
    this.kafkaTopic = Objects.requireNonNull(kafkaTopic, "kafkaTopic");
    this.key = Objects.requireNonNull(key, "key");
    this.timestampName = Objects.requireNonNull(timestampName, "timestampName");
    this.timestampFormat = Objects.requireNonNull(timestampFormat, "timestampFormat");
    this.windowType = Objects.requireNonNull(windowType, "windowType");
    this.avroSchemaId = Objects.requireNonNull(avroSchemaId, "avroSchemaId");
    this.avroSchemaName = Objects.requireNonNull(avroSchemaName, "avroSchemaName");
    this.partitions = Objects.requireNonNull(partitions, "partitions");
    this.replicas = Objects.requireNonNull(replicas, "replicas");
    this.wrapSingleValues = Objects.requireNonNull(wrapSingleValues, "wrapSingleValues");
  }

  public Format getValueFormat() {
    return valueFormat.value;
  }

  public String getKafkaTopic() {
    return kafkaTopic.value;
  }

  public Optional<String> getKeyField() {
    return Optional.ofNullable(key.value);
  }

  public Optional<SerdeFactory<Windowed<String>>> getWindowType() {
    return Optional.ofNullable(windowType.value).map(WINDOW_TYPES::get);
  }

  public Optional<String> getTimestampName() {
    return Optional.ofNullable(timestampName.value);
  }

  public Optional<Integer> getAvroSchemaId() {
    return Optional.ofNullable(avroSchemaId.value);
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(timestampFormat.value);
  }

  public Optional<String> getValueAvroSchemaName() {
    return Optional.ofNullable(avroSchemaName.value);
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(partitions.value);
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(replicas.value);
  }

  public Optional<Boolean> getWrapSingleValues() {
    return Optional.ofNullable(wrapSingleValues.value);
  }

  public CreateSourceProperties withSchemaId(final int id) {
    return new CreateSourceProperties(
        timestampName,
        windowType,
        key,
        kafkaTopic,
        valueFormat,
        avroSchemaName,
        new Property<>(
            KsqlConstants.AVRO_SCHEMA_ID,
            new StringLiteral(Integer.toString(id)),
            ignored -> id),
        timestampFormat,
        partitions,
        replicas,
        wrapSingleValues
    );
  }

  public CreateSourceProperties withPartitionsAndReplicas(
      final int partitions,
      final short replicas
  ) {
    return new CreateSourceProperties(
        timestampName,
        windowType,
        key,
        kafkaTopic,
        valueFormat,
        avroSchemaName,
        avroSchemaId,
        timestampFormat,
        new Property<>(
            KsqlConstants.SOURCE_NUMBER_OF_PARTITIONS,
            new IntegerLiteral(partitions),
            ignored -> partitions),
        new Property<>(
            KsqlConstants.SOURCE_NUMBER_OF_REPLICAS,
            new IntegerLiteral(replicas), ignored ->
            replicas),
        wrapSingleValues
    );
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateSourceProperties that = (CreateSourceProperties) o;
    return Objects.equals(kafkaTopic, that.kafkaTopic)
        && Objects.equals(valueFormat, that.valueFormat)
        && Objects.equals(key, that.key)
        && Objects.equals(windowType, that.windowType)
        && Objects.equals(timestampName, that.timestampName)
        && Objects.equals(timestampFormat, that.timestampFormat)
        && Objects.equals(avroSchemaName, that.avroSchemaName)
        && Objects.equals(avroSchemaId, that.avroSchemaId)
        && Objects.equals(partitions, that.partitions)
        && Objects.equals(replicas, that.replicas)
        && Objects.equals(wrapSingleValues, that.wrapSingleValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        kafkaTopic,
        valueFormat,
        key,
        windowType,
        timestampName,
        timestampFormat,
        avroSchemaName,
        avroSchemaId,
        partitions,
        replicas,
        wrapSingleValues
    );
  }

  private static final class Property<T> {

    private final T value;
    private final String name;
    private final Literal original;

    private Property(final String name, final Literal literal , final Function<Object, T> extract) {
      this.original = literal;
      this.value = literal == null ? null : extract.apply(literal.getValue());
      this.name = name;
    }

    public static <T> Property<T> from(
        final String name,
        final Map<String, Literal> literal,
        final Function<Object, T> extract
    ) {
      return new Property<>(name, literal.get(name), extract);
    }

    public static Property<String> from(
        final String name,
        final Map<String, Literal> literal
    ) {
      return new Property<>(name, literal.get(name), Object::toString);
    }

    public boolean isPresent() {
      return value != null;
    }


    @Override
    public String toString() {
      return name + "=" + original;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Property<?> that = (Property<?>) o;
      return Objects.equals(value, that.value)
          && Objects.equals(name, that.name)
          && Objects.equals(original, that.original);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, name, original);
    }
  }

}
