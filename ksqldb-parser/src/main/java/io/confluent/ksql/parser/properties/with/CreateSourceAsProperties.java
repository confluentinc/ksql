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

package io.confluent.ksql.parser.properties.with;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.ColumnReferenceParser;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CommonCreateConfigs.ProtobufNullableConfigValues;
import io.confluent.ksql.properties.with.CreateAsConfigs;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRFormat;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;

/**
 * Performs validation of a CREATE AS statement's WITH clause.
 */
@Immutable
public final class CreateSourceAsProperties {

  private final PropertiesConfig props;
  private final boolean unwrapProtobufPrimitives;

  public static CreateSourceAsProperties none() {
    return new CreateSourceAsProperties(ImmutableMap.of(), false);
  }

  public static CreateSourceAsProperties from(final Map<String, Literal> literals) {
    try {
      return new CreateSourceAsProperties(literals, false);
    } catch (final ConfigException e) {
      final String message = e.getMessage().replace(
          "configuration",
          "property"
      );

      throw new KsqlException(message, e);
    }
  }

  private CreateSourceAsProperties(
      final Map<String, Literal> originals,
      final boolean unwrapProtobufPrimitives
  ) {
    this.props = new PropertiesConfig(CreateAsConfigs.CONFIG_METADATA, originals);
    this.unwrapProtobufPrimitives = unwrapProtobufPrimitives;

    CommonCreateConfigs.validateKeyValueFormats(props.originals());
    props.validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
  }

  public Optional<String> getKafkaTopic() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY));
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS));
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(props.getShort(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS));
  }

  public Optional<Long> getRetentionInMillis() {
    return Optional.ofNullable(props.getLong(CommonCreateConfigs.SOURCE_TOPIC_RETENTION_IN_MS));
  }

  public Optional<String> getCleanupPolicy() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.SOURCE_TOPIC_CLEANUP_POLICY));
  }

  public Optional<ColumnName> getTimestampColumnName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY))
        .map(ColumnReferenceParser::parse);
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY));
  }

  public SerdeFeatures getValueSerdeFeatures() {
    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    final Boolean wrapping = props.getBoolean(CommonCreateConfigs.WRAP_SINGLE_VALUE);
    if (wrapping != null) {
      builder.add(wrapping ? SerdeFeature.WRAP_SINGLES : SerdeFeature.UNWRAP_SINGLES);
    }

    return SerdeFeatures.from(builder.build());
  }

  public Optional<String> getKeyFormat() {
    final String keyFormat = getFormatName()
        .orElse(props.getString(CommonCreateConfigs.KEY_FORMAT_PROPERTY));
    return Optional.ofNullable(keyFormat);
  }

  public Optional<String> getValueFormat() {
    final String valueFormat = getFormatName()
        .orElse(props.getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY));
    return Optional.ofNullable(valueFormat);
  }

  @SuppressWarnings("MethodMayBeStatic")
  public Map<String, String> getKeyFormatProperties(
      final String name,
      final String keyFormat
  ) {
    final Builder<String, String> builder = ImmutableMap.builder();
    final String schemaName = props.getString(CommonCreateConfigs.KEY_SCHEMA_FULL_NAME);
    if (schemaName != null) {
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, schemaName);
    } else if (AvroFormat.NAME.equalsIgnoreCase(keyFormat)) {
      // ensure that the schema name for the key is unique to the sink - this allows
      // users to always generate valid, non-conflicting avro record definitions in
      // generated Java classes (https://github.com/confluentinc/ksql/issues/6465)
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, AvroFormat.getKeySchemaName(name));
    }

    final String delimiter = props.getString(CommonCreateConfigs.KEY_DELIMITER_PROPERTY);
    if (delimiter != null) {
      builder.put(DelimitedFormat.DELIMITER, delimiter);
    }

    if (ProtobufFormat.NAME.equalsIgnoreCase(keyFormat) && unwrapProtobufPrimitives) {
      builder.put(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP);
    }

    handleNullableProtobufProperty(keyFormat, builder,
        CommonCreateConfigs.KEY_PROTOBUF_NULLABLE_REPRESENTATION);

    final Optional<Integer> keySchemaId = getKeySchemaId();
    keySchemaId.ifPresent(id -> builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(id)));

    return builder.build();
  }

  public Map<String, String> getValueFormatProperties(final String valueFormat) {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    final String avroSchemaName = props.getString(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME);
    final String schemaName =
        avroSchemaName == null ? props.getString(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME)
            : avroSchemaName;
    if (schemaName != null) {
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, schemaName);
    }

    final String delimiter = props.getString(CommonCreateConfigs.VALUE_DELIMITER_PROPERTY);
    if (delimiter != null) {
      builder.put(DelimitedFormat.DELIMITER, delimiter);
    }

    if (ProtobufFormat.NAME.equalsIgnoreCase(valueFormat) && unwrapProtobufPrimitives) {
      builder.put(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP);
    }

    handleNullableProtobufProperty(valueFormat, builder,
        CommonCreateConfigs.VALUE_PROTOBUF_NULLABLE_REPRESENTATION);

    final Optional<Integer> valueSchemaId = getValueSchemaId();
    valueSchemaId.ifPresent(id ->
        builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(id)));

    return builder.build();
  }

  private void handleNullableProtobufProperty(final String valueFormat,
      final Builder<String, String> builder, final String propertyName) {
    if (ProtobufFormat.NAME.equalsIgnoreCase(valueFormat)
        || ProtobufNoSRFormat.NAME.equalsIgnoreCase(valueFormat)) {

      final String nullableRep = props.getString(propertyName);
      if (nullableRep != null) {
        switch (ProtobufNullableConfigValues.valueOf(nullableRep)) {
          case WRAPPER:
            builder.put(ProtobufProperties.NULLABLE_REPRESENTATION,
                ProtobufProperties.NULLABLE_AS_WRAPPER);
            break;
          case OPTIONAL:
            builder.put(ProtobufProperties.NULLABLE_REPRESENTATION,
                ProtobufProperties.NULLABLE_AS_OPTIONAL);
            break;
          default:
            throw new RuntimeException(String.format(
                "Unexpected nullable representation %s. This indicates an implementation error.",
                nullableRep));
        }
      }

    } else {
      // Reject protobuf options for non-protobuf formats
      if (props.getString(propertyName) != null) {
        throw new KsqlException(
            String.format("Property %s can only be enabled with protobuf format",
                propertyName));
      }
    }
  }

  public CreateSourceAsProperties withKeyValueSchemaName(
      final Optional<String> keySchemaName,
      final Optional<String> valueSchemaName
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    keySchemaName.ifPresent(
        s -> originals.put(CommonCreateConfigs.KEY_SCHEMA_FULL_NAME, new StringLiteral(s)));
    valueSchemaName.ifPresent(
        s -> originals.put(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME, new StringLiteral(s)));

    return new CreateSourceAsProperties(originals, unwrapProtobufPrimitives);
  }

  public Optional<Integer> getKeySchemaId() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.KEY_SCHEMA_ID));
  }

  public Optional<Integer> getValueSchemaId() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.VALUE_SCHEMA_ID));
  }

  public CreateSourceAsProperties withTopic(
      final String name,
      final int partitions,
      final short replicas,
      final long retentionMs
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(name));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));
    originals.put(CommonCreateConfigs.SOURCE_TOPIC_RETENTION_IN_MS, new LongLiteral(retentionMs));

    return new CreateSourceAsProperties(originals, unwrapProtobufPrimitives);
  }

  public CreateSourceAsProperties withUnwrapProtobufPrimitives(
      final boolean unwrapProtobufPrimitives
  ) {
    return new CreateSourceAsProperties(props.copyOfOriginalLiterals(), unwrapProtobufPrimitives);
  }

  public CreateSourceAsProperties withCleanupPolicy(final String cleanupPolicy) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(
        CommonCreateConfigs.SOURCE_TOPIC_CLEANUP_POLICY,
        new StringLiteral(cleanupPolicy));

    return new CreateSourceAsProperties(originals, unwrapProtobufPrimitives);
  }

  @Override
  public String toString() {
    return props.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateSourceAsProperties that = (CreateSourceAsProperties) o;
    return Objects.equals(props, that.props)
        && unwrapProtobufPrimitives == that.unwrapProtobufPrimitives;
  }

  @Override
  public int hashCode() {
    return Objects.hash(props, unwrapProtobufPrimitives);
  }

  private Optional<String> getFormatName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.FORMAT_PROPERTY));
  }
}
