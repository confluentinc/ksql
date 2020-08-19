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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.ColumnReferenceParser;
import io.confluent.ksql.parser.DurationParser;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
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

  public static CreateSourceProperties from(final Map<String, Literal> literals) {
    try {
      return new CreateSourceProperties(literals, DurationParser::parse);
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
      final Function<String, Duration> durationParser
  ) {
    this.props = new PropertiesConfig(CreateConfigs.CONFIG_METADATA, originals);
    this.durationParser = Objects.requireNonNull(durationParser, "durationParser");

    props.validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
    validateWindowInfo();
  }

  public Format getValueFormat() {
    return FormatFactory.of(getFormatInfo());
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

  public Optional<Integer> getSchemaId() {
    return Optional.ofNullable(props.getInt(CreateConfigs.SCHEMA_ID));
  }

  public FormatInfo getFormatInfo() {
    return FormatInfo.of(
        props.getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY),
        getFormatProperties());
  }

  private Map<String, String> getFormatProperties() {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    final String schemaName = props.getString(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME);
    if (schemaName != null) {
      builder.put(AvroFormat.FULL_SCHEMA_NAME, schemaName);
    }

    final String delimiter = props.getString(CommonCreateConfigs.VALUE_DELIMITER_PROPERTY);
    if (delimiter != null) {
      builder.put(DelimitedFormat.DELIMITER, delimiter);
    }

    return builder.build();
  }

  public SerdeOptions getSerdeOptions() {
    final ImmutableSet.Builder<SerdeOption> builder = ImmutableSet.builder();

    final Boolean wrapping = props.getBoolean(CommonCreateConfigs.WRAP_SINGLE_VALUE);
    if (wrapping != null) {
      builder.add(wrapping ? SerdeOption.WRAP_SINGLE_VALUES : SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return SerdeOptions.of(builder.build());
  }

  public CreateSourceProperties withSchemaId(final int id) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CreateConfigs.SCHEMA_ID, new IntegerLiteral(id));

    return new CreateSourceProperties(originals, durationParser);
  }

  public CreateSourceProperties withPartitionsAndReplicas(
      final int partitions,
      final short replicas
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));

    return new CreateSourceProperties(originals, durationParser);
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
    return Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(props);
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
}
