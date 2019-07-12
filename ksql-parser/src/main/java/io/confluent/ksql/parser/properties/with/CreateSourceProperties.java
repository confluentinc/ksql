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
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

/**
 * Performs validation of a CREATE statement's WITH clause.
 */
@Immutable
public final class CreateSourceProperties extends WithClauseProperties {

  private static final java.util.Map<String, SerdeFactory<Windowed<String>>> WINDOW_TYPES =
      ImmutableMap.of(
          "SESSION", () -> WindowedSerdes.sessionWindowedSerdeFrom(String.class),
          "TUMBLING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class),
          "HOPPING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class)
      );

  public static CreateSourceProperties from(final Map<String, Literal> literals) {
    try {
      return new CreateSourceProperties(literals);
    } catch (final ConfigException e) {
      final String message = e.getMessage().replace(
          "configuration",
          "property"
      );

      throw new KsqlException(message, e);
    }
  }

  private CreateSourceProperties(final Map<String, Literal> originals) {
    super(CreateConfigs.CONFIG_METADATA, originals);

    validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
  }

  public Format getValueFormat() {
    return Format.valueOf(getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY).toUpperCase());
  }

  public String getKafkaTopic() {
    return getString(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY);
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(getInt(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS));
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(getShort(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS));
  }

  public Optional<String> getKeyField() {
    return Optional.ofNullable(getString(CreateConfigs.KEY_NAME_PROPERTY));
  }

  public Optional<SerdeFactory<Windowed<String>>> getWindowType() {
    return Optional.ofNullable(getString(CreateConfigs.WINDOW_TYPE_PROPERTY))
        .map(String::toUpperCase)
        .map(WINDOW_TYPES::get);
  }

  public Optional<String> getTimestampColumnName() {
    return Optional.ofNullable(getString(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY));
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(getString(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY));
  }

  public Optional<Integer> getAvroSchemaId() {
    return Optional.ofNullable(getInt(CreateConfigs.AVRO_SCHEMA_ID));
  }

  public Optional<String> getValueAvroSchemaName() {
    return Optional.ofNullable(getString(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME));
  }

  public Optional<Boolean> getWrapSingleValues() {
    return Optional.ofNullable(getBoolean(CommonCreateConfigs.WRAP_SINGLE_VALUE));
  }

  public CreateSourceProperties withSchemaId(final int id) {
    final Map<String, Literal> originals = copyOfOriginalLiterals();
    originals.put(CreateConfigs.AVRO_SCHEMA_ID, new IntegerLiteral(id));

    return new CreateSourceProperties(originals);
  }

  public CreateSourceProperties withPartitionsAndReplicas(
      final int partitions,
      final short replicas
  ) {
    final Map<String, Literal> originals = copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));

    return new CreateSourceProperties(originals);
  }
}
