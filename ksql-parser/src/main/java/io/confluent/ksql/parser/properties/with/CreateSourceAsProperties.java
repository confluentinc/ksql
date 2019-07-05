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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateAsConfigs;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;

/**
 * Performs validation of a CREATE AS statement's WITH clause.
 */
@Immutable
public final class CreateSourceAsProperties extends WithClauseProperties {

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
    super(CreateAsConfigs.CONFIG_METADATA, originals);

    validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
  }

  public Optional<Format> getValueFormat() {
    return Optional.ofNullable(getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY))
        .map(String::toUpperCase)
        .map(Format::valueOf);
  }

  public Optional<String> getKafkaTopic() {
    return Optional.ofNullable(getString(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY));
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(getInt(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS));
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(getShort(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS));
  }


  public Optional<String> getTimestampColumnName() {
    return Optional.ofNullable(getString(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY));
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(getString(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY));
  }

  public Optional<String> getValueAvroSchemaName() {
    return Optional.ofNullable(getString(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME));
  }

  public Optional<Boolean> getWrapSingleValues() {
    return Optional.ofNullable(getBoolean(CommonCreateConfigs.WRAP_SINGLE_VALUE));
  }

  public CreateSourceAsProperties withTopic(
      final String name,
      final int partitions,
      final short replicas
  ) {
    final Map<String, Literal> originals = copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(name));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));

    return new CreateSourceAsProperties(originals);
  }
}
