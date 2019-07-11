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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.configdef.ConfigValidators;
import io.confluent.ksql.configdef.ConfigValidators.ValidCaseInsensitiveString;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

/**
 * Performs validation of a CREATE statement's WITH clause.
 */
@Immutable
public final class CreateSourceProperties extends AbstractConfig {

  private static final String KAFKA_TOPIC_NAME_PROPERTY = DdlConfig.KAFKA_TOPIC_NAME_PROPERTY;
  private static final String VALUE_FORMAT_PROPERTY = DdlConfig.VALUE_FORMAT_PROPERTY;
  private static final String KEY_NAME_PROPERTY = DdlConfig.KEY_NAME_PROPERTY;
  private static final String TIMESTAMP_NAME_PROPERTY = DdlConfig.TIMESTAMP_NAME_PROPERTY;
  private static final String TIMESTAMP_FORMAT_PROPERTY = DdlConfig.TIMESTAMP_FORMAT_PROPERTY;
  private static final String WRAP_SINGLE_VALUE = DdlConfig.WRAP_SINGLE_VALUE;
  private static final String VALUE_AVRO_SCHEMA_FULL_NAME = DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME;
  private static final String SOURCE_NUMBER_OF_PARTITIONS =
      KsqlConstants.SOURCE_NUMBER_OF_PARTITIONS;
  private static final String SOURCE_NUMBER_OF_REPLICAS = KsqlConstants.SOURCE_NUMBER_OF_REPLICAS;
  private static final String WINDOW_TYPE_PROPERTY = DdlConfig.WINDOW_TYPE_PROPERTY;
  private static final String AVRO_SCHEMA_ID = KsqlConstants.AVRO_SCHEMA_ID;

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          KAFKA_TOPIC_NAME_PROPERTY,
          ConfigDef.Type.STRING,
          ConfigDef.NO_DEFAULT_VALUE,
          new NonEmptyString(),
          Importance.HIGH,
          "The topic that stores the data of the source"
      ).define(
          VALUE_FORMAT_PROPERTY,
          ConfigDef.Type.STRING,
          ConfigDef.NO_DEFAULT_VALUE,
          ConfigValidators.enumValues(Format.class),
          Importance.HIGH,
          "The format of the serialized value"
      ).define(
          KEY_NAME_PROPERTY,
          ConfigDef.Type.STRING,
          null,
          Importance.MEDIUM,
          "The name of a field within the Kafka record value that matches the key. "
              + "This may be used by KSQL to avoid unnecessary repartitions."
      ).define(
          TIMESTAMP_NAME_PROPERTY,
          ConfigDef.Type.STRING,
          null,
          Importance.MEDIUM,
          "The name of a field within the Kafka record value that contains the "
              + "timestamp KSQL should use inplace of the default Kafka record timestamp. "
              + "By default, KSQL requires the timestamp to be a `BIGINT`. Alternatively, you can "
              + "supply '" + DdlConfig.TIMESTAMP_FORMAT_PROPERTY + "' to control how the field is "
              + "parsed"
      ).define(
          TIMESTAMP_FORMAT_PROPERTY,
          ConfigDef.Type.STRING,
          null,
          Importance.MEDIUM,
          "If supplied, defines the format of the '"
              + DdlConfig.TIMESTAMP_NAME_PROPERTY + "' field. The format can be any supported by "
              + "the Java DateTimeFormatter class"
      ).define(
          WRAP_SINGLE_VALUE,
          ConfigDef.Type.BOOLEAN,
          null,
          Importance.LOW,
          "Controls how values are deserialized where the value schema contains only a"
              + " single field.  If set to true, KSQL expects the field to have been serialized as "
              + "a named field within a record. If set to false, KSQL expects the field to have "
              + "been serialized as an anonymous value."
      ).define(
          VALUE_AVRO_SCHEMA_FULL_NAME,
          ConfigDef.Type.STRING,
          null,
          Importance.LOW,
          "The fully qualified name of the Avro schema to use"
      ).define(
          SOURCE_NUMBER_OF_PARTITIONS,
          ConfigDef.Type.INT,
          null,
          Importance.LOW,
          "The number of partitions in the backing topic. This property must be set if "
              + "creating a source without an existing topic (the command will fail if the topic "
              + "does not exist)"
      ).define(
          SOURCE_NUMBER_OF_REPLICAS,
          ConfigDef.Type.SHORT,
          null,
          Importance.LOW,
          "The number of replicas in the backing topic. If this property is not set "
              + "but '" + KsqlConstants.SOURCE_NUMBER_OF_PARTITIONS + "' is set, then the default "
              + "Kafka cluster configuration for replicas will be used for creating a new topic."
      ).define(
          WINDOW_TYPE_PROPERTY,
          ConfigDef.Type.STRING,
          null,
          ValidCaseInsensitiveString.in("SESSION", "HOPPING", "TUMBLING", null),
          Importance.LOW,
          "If the data is windowed, i.e. was created using KSQL using a query that "
              + "contains a ``WINDOW`` clause, then the property can be used to provide the "
              + "window type. Valid values are SESSION, HOPPING or TUMBLING."
      ).define(
          AVRO_SCHEMA_ID,
          ConfigDef.Type.INT,
          null,
          Importance.LOW,
          "Undocumented feature"
      );

  private static final Set<String> CONFIG_NAMES = CONFIG_DEF.names().stream()
      .map(String::toUpperCase)
      .collect(Collectors.toSet());

  private static final List<String> ORDERED_CONFIG_NAMES;

  private static final Set<String> SHORT_CONFIG_NAMES = CONFIG_DEF.configKeys().entrySet().stream()
      .filter(e -> e.getValue().type() == ConfigDef.Type.SHORT)
      .map(Entry::getKey)
      .collect(Collectors.toSet());

  static {
    final List<String> names = new ArrayList<>(CONFIG_NAMES);
    names.sort(Comparator.naturalOrder());
    ORDERED_CONFIG_NAMES = ImmutableList.copyOf(names);
  }

  private static final java.util.Map<String, SerdeFactory<Windowed<String>>> WINDOW_TYPES =
      ImmutableMap.of(
        "SESSION", () -> WindowedSerdes.sessionWindowedSerdeFrom(String.class),
        "TUMBLING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class),
        "HOPPING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class)
      );

  private final ImmutableMap<String, Literal> originalLiterals;

  public static CreateSourceProperties from(final Map<String, Literal> literals) {
    throwOnUnknownProperty(literals);

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
    super(CONFIG_DEF, toValues(Objects.requireNonNull(originals, "originals")), false);
    this.originalLiterals = ImmutableMap.copyOf(originals.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toUpperCase(), Map.Entry::getValue)));
  }

  @Override
  public String toString() {
    return ORDERED_CONFIG_NAMES.stream()
        .filter(originalLiterals::containsKey)
        .map(name -> name + "=" + originalLiterals.get(name))
        .collect(Collectors.joining(", "));
  }

  public Format getValueFormat() {
    return Format.valueOf(getString(VALUE_FORMAT_PROPERTY).toUpperCase());
  }

  public String getKafkaTopic() {
    return getString(KAFKA_TOPIC_NAME_PROPERTY);
  }

  public Optional<String> getKeyField() {
    return Optional.ofNullable(getString(KEY_NAME_PROPERTY));
  }

  public Optional<SerdeFactory<Windowed<String>>> getWindowType() {
    return Optional.ofNullable(getString(WINDOW_TYPE_PROPERTY))
        .map(String::toUpperCase)
        .map(WINDOW_TYPES::get);
  }

  public Optional<String> getTimestampName() {
    return Optional.ofNullable(getString(TIMESTAMP_NAME_PROPERTY));
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(getString(TIMESTAMP_FORMAT_PROPERTY));
  }

  public Optional<Integer> getAvroSchemaId() {
    return Optional.ofNullable(getInt(AVRO_SCHEMA_ID));
  }

  public Optional<String> getValueAvroSchemaName() {
    return Optional.ofNullable(getString(VALUE_AVRO_SCHEMA_FULL_NAME));
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(getInt(SOURCE_NUMBER_OF_PARTITIONS));
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(getShort(SOURCE_NUMBER_OF_REPLICAS));
  }

  public Optional<Boolean> getWrapSingleValues() {
    return Optional.ofNullable(getBoolean(WRAP_SINGLE_VALUE));
  }

  public CreateSourceProperties withSchemaId(final int id) {
    final Map<String, Literal> originals = new HashMap<>(originalLiterals);
    originals.put(AVRO_SCHEMA_ID, new IntegerLiteral(id));

    return new CreateSourceProperties(originals);
  }

  public CreateSourceProperties withPartitionsAndReplicas(
      final int partitions,
      final short replicas
  ) {
    final Map<String, Literal> originals = new HashMap<>(originalLiterals);
    originals.put(SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));

    return new CreateSourceProperties(originals);
  }

  private static Map<String, Object> toValues(final Map<String, Literal> literals) {
    final Map<String, Object> values = literals.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toUpperCase(), e -> e.getValue().getValue()));

    SHORT_CONFIG_NAMES.forEach(configName -> {
      final Object rf = values.get(configName);
      if (rf instanceof Number) {
        values.put(configName, ((Number) rf).shortValue());
      }
    });

    return values;
  }

  private static void throwOnUnknownProperty(final Map<String, ?> originals) {
    final Set<String> providedNames = originals.keySet().stream()
        .map(String::toUpperCase)
        .collect(Collectors.toSet());

    final SetView<String> onlyInProvided = Sets.difference(providedNames, CONFIG_NAMES);
    if (!onlyInProvided.isEmpty()) {
      throw new KsqlException("Invalid config variable(s) in the WITH clause: "
          + String.join(",", onlyInProvided));
    }
  }
}
