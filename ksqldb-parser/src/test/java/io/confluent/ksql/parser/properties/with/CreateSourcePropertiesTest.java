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

import static com.google.common.collect.ImmutableMap.of;
import static io.confluent.ksql.parser.properties.with.CreateSourceAsProperties.from;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_SCHEMA_FULL_NAME;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_SCHEMA_ID;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_SCHEMA_ID;
import static io.confluent.ksql.properties.with.CreateConfigs.WINDOW_SIZE_PROPERTY;
import static io.confluent.ksql.properties.with.CreateConfigs.WINDOW_TYPE_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateSourcePropertiesTest {

  private static final java.util.Map<String, Literal> MINIMUM_VALID_PROPS = ImmutableMap.of(
      CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("foo")
  );

  @Mock
  private Function<String, Duration> durationParser;

  @Test
  public void shouldSetMinimumValidProps() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(MINIMUM_VALID_PROPS);

    // Then:
    assertThat(properties.getKafkaTopic(), is("foo"));
  }

  @Test
  public void shouldReturnOptionalEmptyForMissingProps() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(MINIMUM_VALID_PROPS);

    // Then:
    assertThat(properties.getTimestampColumnName(), is(Optional.empty()));
    assertThat(properties.getTimestampFormat(), is(Optional.empty()));
    assertThat(properties.getWindowType(), is(Optional.empty()));
    assertThat(properties.getKeySchemaId(), is(Optional.empty()));
    assertThat(properties.getValueSchemaId(), is(Optional.empty()));
    assertThat(properties.getKeyFormat(SourceName.of("foo")), is(Optional.empty()));
    assertThat(properties.getValueFormat(), is(Optional.empty()));
    assertThat(properties.getReplicas(), is(Optional.empty()));
    assertThat(properties.getPartitions(), is(Optional.empty()));
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of()));
    assertThat(properties.getKeySchemaFullName(), is(Optional.empty()));
    assertThat(properties.getValueSchemaFullName(), is(Optional.empty()));
  }

  @Test
  public void shouldReturnOptionalEmptyForPropsWithSpaces() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(KEY_SCHEMA_FULL_NAME, new StringLiteral("     "))
            .put(VALUE_SCHEMA_FULL_NAME, new StringLiteral("     "))
            .build());

    // Then:
    assertThat(properties.getKeySchemaFullName(), is(Optional.empty()));
    assertThat(properties.getValueSchemaFullName(), is(Optional.empty()));
  }

  @Test
  public void shouldSetValidKeyAndValueFullSchemaName() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.KEY_SCHEMA_FULL_NAME, new StringLiteral("io.com.key"))
            .put(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME, new StringLiteral("io.com.val"))
            .build());

    // Then:
    assertThat(properties.getKeySchemaFullName(), is(Optional.of("io.com.key")));
    assertThat(properties.getValueSchemaFullName(), is(Optional.of("io.com.val")));
  }

  @Test
  public void shouldSetValidTimestampName() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("ts"))
            .build());

    // Then:
    assertThat(properties.getTimestampColumnName(), is(Optional.of(ColumnName.of("TS"))));
  }

  @Test
  public void shouldSetValidTimestampFormat() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(
                CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY,
                new StringLiteral("yyyy-MM-dd'T'HH:mm:ss.SSS")
            )
            .build());

    // Then:
    assertThat(properties.getTimestampFormat(), is(Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSS")));
  }

  @Test
  public void shouldThrowOnConstructionInvalidTimestampFormat() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> from(
            of(TIMESTAMP_FORMAT_PROPERTY, new StringLiteral("invalid")))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid datetime format for config:TIMESTAMP_FORMAT, reason:Unknown pattern letter: i"));
  }

  @Test
  public void shouldThrowOnConstructionOnUnknownWindowType() {
    // Given:
    final Map<String, Literal> props = ImmutableMap.<String, Literal>builder()
        .putAll(MINIMUM_VALID_PROPS)
        .put(CreateConfigs.WINDOW_TYPE_PROPERTY, new StringLiteral("Unknown"))
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(props)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid value Unknown for property WINDOW_TYPE: "
        + "String must be one of: SESSION, HOPPING, TUMBLING, null"));
  }

  @Test
  public void shouldThrowOnConstructionOnInvalidDuration() {
    // Given:
    final Map<String, Literal> props = ImmutableMap.<String, Literal>builder()
        .putAll(MINIMUM_VALID_PROPS)
        .put(CreateConfigs.WINDOW_TYPE_PROPERTY, new StringLiteral("HOPPING"))
        .put(CreateConfigs.WINDOW_SIZE_PROPERTY, new StringLiteral("2 HOURS"))
        .build();

    when(durationParser.apply(any())).thenThrow(new IllegalArgumentException("a failure reason"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new CreateSourceProperties(props, durationParser, false)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Error in WITH clause property 'WINDOW_SIZE': "
        + "a failure reason"
        + System.lineSeparator()
        + "Example valid value: '10 SECONDS'"));
  }

  @Test
  public void shouldSetHoppingWindow() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CreateConfigs.WINDOW_TYPE_PROPERTY, new StringLiteral("HoppIng"))
            .put(CreateConfigs.WINDOW_SIZE_PROPERTY, new StringLiteral("10 Minutes"))
            .build());

    // Then:
    assertThat(properties.getWindowType(), is(Optional.of(WindowType.HOPPING)));
    assertThat(properties.getWindowSize(), is(Optional.of(Duration.ofMinutes(10))));
  }

  @Test
  public void shouldThrowOnHoppingWindowWithOutSize() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(WINDOW_TYPE_PROPERTY, new StringLiteral("hopping"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), containsString("HOPPING windows require 'WINDOW_SIZE' to be provided in the WITH clause. "
        + "For example: 'WINDOW_SIZE'='10 SECONDS'"));
  }

  @Test
  public void shouldSetTumblingWindow() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CreateConfigs.WINDOW_TYPE_PROPERTY, new StringLiteral("TUMBLING"))
            .put(CreateConfigs.WINDOW_SIZE_PROPERTY, new StringLiteral("1 SECOND"))
            .build());

    // Then:
    assertThat(properties.getWindowType(), is(Optional.of(WindowType.TUMBLING)));
    assertThat(properties.getWindowSize(), is(Optional.of(Duration.ofSeconds(1))));
  }

  @Test
  public void shouldThrowOnTumblingWindowWithOutSize() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(WINDOW_TYPE_PROPERTY, new StringLiteral("tumbling"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), containsString("TUMBLING windows require 'WINDOW_SIZE' to be provided in the WITH clause. "
        + "For example: 'WINDOW_SIZE'='10 SECONDS'"));
  }

  @Test
  public void shouldSetSessionWindow() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CreateConfigs.WINDOW_TYPE_PROPERTY, new StringLiteral("SESSION"))
            .build());

    // Then:
    assertThat(properties.getWindowType(), is(Optional.of(WindowType.SESSION)));
  }

  @Test
  public void shouldThrowOnSessionWindowWithSize() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(WINDOW_TYPE_PROPERTY, new StringLiteral("SESSION"))
                .put(WINDOW_SIZE_PROPERTY, new StringLiteral("2 MILLISECONDS"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), containsString("'WINDOW_SIZE' should not be set for SESSION windows."));
  }

  @Test
  public void shouldSetValidSchemaIds() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(KEY_SCHEMA_ID, new StringLiteral("1"))
            .put(VALUE_SCHEMA_ID, new StringLiteral("2"))
            .build());

    // Then:
    assertThat(properties.getKeySchemaId(), is(Optional.of(1)));
    assertThat(properties.getValueSchemaId(), is(Optional.of(2)));
  }

  @Test
  public void shouldSetValidAvroSchemaName() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("AvRo"))
            .put(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema"))
            .build());

    // Then:
    assertThat(properties.getValueFormat()
        .map(FormatInfo::getProperties)
        .map(props -> props.get(ConnectProperties.FULL_SCHEMA_NAME)),
        is(Optional.of("schema")));
  }

  @Test
  public void shouldSetValueFullSchemaName() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("Protobuf"))
            .put(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME, new StringLiteral("schema"))
            .build());

    // Then:
    assertThat(properties.getValueFormat()
        .map(FormatInfo::getProperties)
        .map(props -> props.get(ConnectProperties.FULL_SCHEMA_NAME)),
        is(Optional.of("schema")));
  }

  @Test
  public void shouldSetReplicasFromNumber() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(2))
            .build());

    // Then:
    assertThat(properties.getReplicas(), is(Optional.of((short) 2)));
  }

  @Test
  public void shouldSetPartitions() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(2))
            .build());

    // Then:
    assertThat(properties.getPartitions(), is(Optional.of(2)));
  }

  @Test
  public void shouldSetWrapSingleValues() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.WRAP_SINGLE_VALUE, new BooleanLiteral("true"))
            .build());

    // Then:
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)));
  }

  @Test
  public void shouldSetNumericPropertyFromStringLiteral() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new StringLiteral("3"))
            .build());

    // Then:
    assertThat(properties.getReplicas(), is(Optional.of((short) 3)));
  }

  @Test
  public void shouldSetBooleanPropertyFromStringLiteral() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.WRAP_SINGLE_VALUE, new StringLiteral("true"))
            .build());

    // Then:
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)));
  }

  @Test
  public void shouldHandleNonUpperCasePropNames() {
    // When:
    final CreateSourceProperties properties = CreateSourceProperties.from(
        ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(CommonCreateConfigs.WRAP_SINGLE_VALUE.toLowerCase(), new StringLiteral("false"))
            .build());

    // Then:
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldFailIfNoKafkaTopicName() {
    // Given:
    final HashMap<String, Literal> props = new HashMap<>(MINIMUM_VALID_PROPS);
    props.remove(KAFKA_TOPIC_NAME_PROPERTY);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(props)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Missing required property \"KAFKA_TOPIC\" which has no default value."));
  }

  @Test
  public void shouldFailIfInvalidWindowConfig() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(WINDOW_TYPE_PROPERTY, new StringLiteral("bar"))
                .build()
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid value bar for property WINDOW_TYPE: String must be one of: SESSION, HOPPING, TUMBLING"));
  }

  @Test
  public void shouldFailIfInvalidConfig() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put("foo", new StringLiteral("bar"))
                .build()
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid config variable(s) in the WITH clause: FOO"));
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldProperlyImplementEqualsAndHashCode() {
    new EqualsTester()
        .addEqualityGroup(
            CreateSourceProperties.from(MINIMUM_VALID_PROPS),
            CreateSourceProperties.from(MINIMUM_VALID_PROPS))
        .addEqualityGroup(
            CreateSourceProperties.from(ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema"))
                .build()))
        .addEqualityGroup(
            CreateSourceProperties.from(MINIMUM_VALID_PROPS)
                .withUnwrapProtobufPrimitives(true))
        .testEquals();
  }

  @Test
  public void shouldIncludeOnlyProvidedPropsInToString() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put("Wrap_Single_value", new StringLiteral("True"))
            .build());

    // When:
    final String sql = props.toString();

    // Then:
    assertThat(sql, is("KAFKA_TOPIC='foo', WRAP_SINGLE_VALUE='True'"));
  }

  @Test
  public void shouldNotQuoteNonStringPropValues() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put("Wrap_Single_value", new BooleanLiteral("true"))
            .build());

    // When:
    final String sql = props.toString();

    // Then:
    assertThat(sql, containsString("WRAP_SINGLE_VALUE=true"));
  }

  @Test
  public void shouldGetKeyAndValueFormatFromFormat() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(KEY_FORMAT_PROPERTY, new StringLiteral("KAFKA"))
            .put(VALUE_FORMAT_PROPERTY, new StringLiteral("AVRO"))
            .build());

    // When / Then:
    assertThat(props.getKeyFormat(SourceName.of("foo")).get().getFormat(), is("KAFKA"));
    assertThat(props.getValueFormat().get().getFormat(), is("AVRO"));
  }

  @Test
  public void shouldGetKeyAndValueSchemaIdFromFormat() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("AVRO"))
            .put(KEY_SCHEMA_ID, new IntegerLiteral(123))
            .put(VALUE_SCHEMA_ID, new IntegerLiteral(456))
            .build());

    // When / Then:
    assertThat(props.getKeyFormat(
        SourceName.of("foo")).get().getProperties(),
        hasEntry(ConnectProperties.SCHEMA_ID, "123"));
    assertThat(props.getValueFormat().get().getProperties(),
        hasEntry(ConnectProperties.SCHEMA_ID, "456"));
  }

  @Test
  public void shouldSetAvroNameOnAvroKeyFormat() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("AVRO"))
            .build());

    // When / Then:
    assertThat(props.getKeyFormat(SourceName.of("foo")).get().getFormat(), is("AVRO"));
    assertThat(props.getKeyFormat(
        SourceName.of("foo")).get().getProperties(),
        hasEntry(ConnectProperties.FULL_SCHEMA_NAME, "io.confluent.ksql.avro_schemas.FooKey"));
  }

  @Test
  public void shouldSetFullKeySchemaName() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("Json_sr"))
            .put(KEY_SCHEMA_FULL_NAME, new StringLiteral("KeySchema"))
            .build());

    // When / Then:
    assertThat(props.getKeyFormat(SourceName.of("foo")).get().getFormat(), is("JSON_SR"));
    assertThat(props.getKeyFormat(
        SourceName.of("foo")).get().getProperties(),
        hasEntry(ConnectProperties.FULL_SCHEMA_NAME, "KeySchema"));
  }

  @Test
  public void shouldSetAvroNameOnAvroKeyFormatWithKeySchemaProperty() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("AVRO"))
            .put(KEY_SCHEMA_FULL_NAME, new StringLiteral("KeySchemaName"))
            .build());

    // When // Then:
    assertThat(props.getKeyFormat(SourceName.of("foo")).get().getFormat(), is("AVRO"));
    assertThat(props.getKeyFormat(
        SourceName.of("foo")).get().getProperties(),
        hasEntry(ConnectProperties.FULL_SCHEMA_NAME, "KeySchemaName"));
  }

  @Test
  public void shouldThrowIfKeyFormatAndFormatProvided() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(KEY_FORMAT_PROPERTY, new StringLiteral("KAFKA"))
                .put(FORMAT_PROPERTY, new StringLiteral("JSON"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Cannot supply both 'KEY_FORMAT' and 'FORMAT' properties, "
        + "as 'FORMAT' sets both key and value formats."));
    assertThat(e.getMessage(), containsString("Either use just 'FORMAT', or use 'KEY_FORMAT' and 'VALUE_FORMAT'."));
  }

  @Test
  public void shouldThrowIfValueSchemaNameAndAvroSchemaNameProvided() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(VALUE_SCHEMA_FULL_NAME, new StringLiteral("value_schema"))
                .put(VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("value_schema"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), is("Cannot supply both 'VALUE_AVRO_SCHEMA_FULL_NAME' "
        + "and 'VALUE_SCHEMA_FULL_NAME' properties. Please only set 'VALUE_SCHEMA_FULL_NAME'."));
  }

  @Test
  public void shouldThrowIfValueFormatAndFormatProvided() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceProperties.from(
            ImmutableMap.<String, Literal>builder()
                .putAll(MINIMUM_VALID_PROPS)
                .put(VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"))
                .put(FORMAT_PROPERTY, new StringLiteral("KAFKA"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Cannot supply both 'VALUE_FORMAT' and 'FORMAT' properties, "
        + "as 'FORMAT' sets both key and value formats."));
    assertThat(e.getMessage(), containsString("Either use just 'FORMAT', or use 'KEY_FORMAT' and 'VALUE_FORMAT'."));
  }

  @Test
  public void shouldGetProtobufKeyFormatPropertiesWithUnwrapping() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("PROTOBUF"))
            .build())
        .withUnwrapProtobufPrimitives(true);

    // When / Then:
    assertThat(props.getKeyFormat(SourceName.of("foo")).get().getFormat(), is("PROTOBUF"));
    assertThat(props.getKeyFormat(
        SourceName.of("foo")).get().getProperties(),
        hasEntry(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP));
  }

  @Test
  public void shouldGetProtobufKeyFormatPropertiesWithoutUnwrapping() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("PROTOBUF"))
            .build());

    // When / Then:
    assertThat(props.getKeyFormat(SourceName.of("foo")).get().getFormat(), is("PROTOBUF"));
    assertThat(props.getKeyFormat(
        SourceName.of("foo")).get().getProperties(),
        not(hasKey(ProtobufProperties.UNWRAP_PRIMITIVES)));
  }

  @Test
  public void shouldGetProtobufValueFormatPropertiesWithUnwrapping() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("PROTOBUF"))
            .build())
        .withUnwrapProtobufPrimitives(true);

    // When / Then:
    assertThat(props.getValueFormat().get().getFormat(), is("PROTOBUF"));
    assertThat(props.getValueFormat().get().getProperties(),
        hasEntry(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP));
  }

  @Test
  public void shouldGetProtobufValueFormatPropertiesWithoutUnwrapping() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties
        .from(ImmutableMap.<String, Literal>builder()
            .putAll(MINIMUM_VALID_PROPS)
            .put(FORMAT_PROPERTY, new StringLiteral("PROTOBUF"))
            .build());

    // When / Then:
    assertThat(props.getValueFormat().get().getFormat(), is("PROTOBUF"));
    assertThat(props.getValueFormat().get().getProperties(),
        not(hasKey(ProtobufProperties.UNWRAP_PRIMITIVES)));
  }
}