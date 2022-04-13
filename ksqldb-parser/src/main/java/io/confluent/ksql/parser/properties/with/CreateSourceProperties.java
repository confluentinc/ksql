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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.ColumnReferenceParser;
import io.confluent.ksql.parser.DurationParser;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigException;

/**
 * Performs validation of a CREATE statement's WITH clause.
 */
@Immutable
public final class CreateSourceProperties {

  private final PropertiesConfig props;
  @EffectivelyImmutable
  private final transient Function<String, Duration> durationParser;
  private final boolean unwrapProtobufPrimitives;

  public static CreateSourceProperties from(final Map<String, Literal> literals) {
    try {
      return new CreateSourceProperties(literals, DurationParser::parse, false);
    } catch (final ConfigException e) {
      final String message = e.getMessage().replace(
          "configuration",
          "property"
      );

      throw new KsqlException(message, e);
    }
  }

  @VisibleForTesting
  CreateSourceProperties(
      final Map<String, Literal> originals,
      final Function<String, Duration> durationParser,
      final boolean unwrapProtobufPrimitives
  ) {
    this.props = new PropertiesConfig(CreateConfigs.CONFIG_METADATA, originals);
    this.durationParser = Objects.requireNonNull(durationParser, "durationParser");
    this.unwrapProtobufPrimitives = unwrapProtobufPrimitives;

    CommonCreateConfigs.validateKeyValueFormats(props.originals());
    props.validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
    validateWindowInfo();
  }

  public String getKafkaTopic() {
    return props.getString(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY);
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS));
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(props.getShort(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS));
  }

  public Optional<WindowType> getWindowType() {
    try {
      return Optional.ofNullable(props.getString(CreateConfigs.WINDOW_TYPE_PROPERTY))
          .map(WindowType::of);
    } catch (final Exception e) {
      throw new KsqlException("Error in WITH clause property '"
          + CreateConfigs.WINDOW_TYPE_PROPERTY + "': " + e.getMessage(),
          e);
    }
  }

  public Optional<Duration> getWindowSize() {
    try {
      return Optional.ofNullable(props.getString(CreateConfigs.WINDOW_SIZE_PROPERTY))
          .map(durationParser);
    } catch (final Exception e) {
      throw new KsqlException("Error in WITH clause property '"
          + CreateConfigs.WINDOW_SIZE_PROPERTY + "': " + e.getMessage()
          + System.lineSeparator()
          + "Example valid value: '10 SECONDS'",
          e);
    }
  }

  public Optional<ColumnName> getTimestampColumnName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY))
        .map(ColumnReferenceParser::parse);
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY));
  }

  public Optional<Integer> getKeySchemaId() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.KEY_SCHEMA_ID));
  }

  public Optional<Integer> getValueSchemaId() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.VALUE_SCHEMA_ID));
  }

  public Optional<String> getKeySchemaFullName() {
    final String schemaFullName = props.getString(CommonCreateConfigs.KEY_SCHEMA_FULL_NAME);
    if (schemaFullName == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(Strings.emptyToNull(schemaFullName.trim()));
  }

  public Optional<String> getValueSchemaFullName() {
    final String schemaFullName = props.getString(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME);
    if (schemaFullName == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(Strings.emptyToNull(schemaFullName.trim()));
  }

  public Optional<FormatInfo> getKeyFormat(final SourceName name) {
    final String keyFormat = getFormatName()
        .orElse(props.getString(CommonCreateConfigs.KEY_FORMAT_PROPERTY));
    return Optional.ofNullable(keyFormat)
        .map(format -> FormatInfo.of(format, getKeyFormatProperties(keyFormat, name.text())));
  }

  public Map<String, String> getKeyFormatProperties(final String keyFormat, final String name) {
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

    final Optional<Integer> keySchemaId = getKeySchemaId();
    keySchemaId.ifPresent(id -> builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(id)));

    return builder.build();
  }

  public Optional<FormatInfo> getValueFormat() {
    final String valueFormat = getFormatName()
        .orElse(props.getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY));
    return Optional.ofNullable(valueFormat)
        .map(format -> FormatInfo.of(format, getValueFormatProperties(valueFormat)));
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

    final Optional<Integer> valueSchemaId = getValueSchemaId();
    valueSchemaId.ifPresent(id ->
        builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(id)));

    return builder.build();
  }

  public SerdeFeatures getValueSerdeFeatures() {
    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    final Boolean wrapping = props.getBoolean(CommonCreateConfigs.WRAP_SINGLE_VALUE);
    if (wrapping != null) {
      builder.add(wrapping ? SerdeFeature.WRAP_SINGLES : SerdeFeature.UNWRAP_SINGLES);
    }

    return SerdeFeatures.from(builder.build());
  }

  public CreateSourceProperties withKeyValueSchemaName(
      final Optional<String> keySchemaName,
      final Optional<String> valueSchemaName
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    keySchemaName.ifPresent(
        s -> originals.put(CommonCreateConfigs.KEY_SCHEMA_FULL_NAME, new StringLiteral(s)));
    valueSchemaName.ifPresent(
        s -> originals.put(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME, new StringLiteral(s)));

    return new CreateSourceProperties(originals, durationParser, unwrapProtobufPrimitives);
  }

  public CreateSourceProperties withPartitions(
      final int partitions
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));

    return new CreateSourceProperties(originals, durationParser, unwrapProtobufPrimitives);
  }

  public CreateSourceProperties withFormats(final String keyFormat, final String valueFormat) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.KEY_FORMAT_PROPERTY, new StringLiteral(keyFormat));
    originals.put(CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral(valueFormat));

    return new CreateSourceProperties(originals, durationParser, unwrapProtobufPrimitives);
  }

  public CreateSourceProperties withUnwrapProtobufPrimitives(
      final boolean unwrapProtobufPrimitives
  ) {
    return new CreateSourceProperties(
        props.copyOfOriginalLiterals(),
        durationParser,
        unwrapProtobufPrimitives
    );
  }

  public Map<String, Literal> copyOfOriginalLiterals() {
    return props.copyOfOriginalLiterals();
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
    final CreateSourceProperties that = (CreateSourceProperties) o;
    return Objects.equals(props, that.props)
        && unwrapProtobufPrimitives == that.unwrapProtobufPrimitives;
  }

  @Override
  public int hashCode() {
    return Objects.hash(props, unwrapProtobufPrimitives);
  }

  private void validateWindowInfo() {
    final Optional<WindowType> windowType = getWindowType();
    final Optional<Duration> windowSize = getWindowSize();

    final boolean requiresSize = windowType.isPresent() && windowType.get() != WindowType.SESSION;

    if (requiresSize && !windowSize.isPresent()) {
      throw new KsqlException(windowType.get() + " windows require '"
          + CreateConfigs.WINDOW_SIZE_PROPERTY + "' to be provided in the WITH clause. "
          + "For example: '" + CreateConfigs.WINDOW_SIZE_PROPERTY + "'='10 SECONDS'");
    }

    if (!requiresSize && windowSize.isPresent()) {
      throw new KsqlException("'" + CreateConfigs.WINDOW_SIZE_PROPERTY + "' "
          + "should not be set for SESSION windows.");
    }
  }

  private Optional<String> getFormatName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.FORMAT_PROPERTY));
  }

}
