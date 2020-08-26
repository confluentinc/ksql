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
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.ColumnReferenceParser;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateAsConfigs;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
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

  public static CreateSourceAsProperties none() {
    return new CreateSourceAsProperties(ImmutableMap.of());
  }

  public static CreateSourceAsProperties from(final Map<String, Literal> literals) {
    try {
      return new CreateSourceAsProperties(literals);
    } catch (final ConfigException e) {
      final String message = e.getMessage().replace(
          "configuration",
          "property"
      );

      throw new KsqlException(message, e);
    }
  }

  private CreateSourceAsProperties(final Map<String, Literal> originals) {
    this.props = new PropertiesConfig(CreateAsConfigs.CONFIG_METADATA, originals);

    props.validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
  }

  public Optional<Format> getValueFormat() {
    return getFormatInfo().map(FormatFactory::of);
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

  public Optional<ColumnName> getTimestampColumnName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY))
        .map(ColumnReferenceParser::parse);
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY));
  }

  public SerdeOptions getSerdeOptions() {
    final ImmutableSet.Builder<SerdeOption> builder = ImmutableSet.builder();

    final Boolean wrapping = props.getBoolean(CommonCreateConfigs.WRAP_SINGLE_VALUE);
    if (wrapping != null) {
      builder.add(wrapping ? SerdeOption.WRAP_SINGLE_VALUES : SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return SerdeOptions.of(builder.build());
  }

  public Optional<FormatInfo> getFormatInfo() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY))
        .map(format -> FormatInfo.of(format, getFormatProperties()));
  }

  public Map<String, String> getFormatProperties() {
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

  public CreateSourceAsProperties withTopic(
      final String name,
      final int partitions,
      final short replicas
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(name));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));

    return new CreateSourceAsProperties(originals);
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
    return Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(props);
  }
}
