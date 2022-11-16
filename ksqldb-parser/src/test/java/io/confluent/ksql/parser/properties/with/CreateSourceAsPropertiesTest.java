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
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_PROTOBUF_NULLABLE_REPRESENTATION;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_SCHEMA_FULL_NAME;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.KEY_SCHEMA_ID;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_PROTOBUF_NULLABLE_REPRESENTATION;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_SCHEMA_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CommonCreateConfigs.ProtobufNullableConfigValues;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class CreateSourceAsPropertiesTest {

  @Test
  public void shouldHandleNoProperties() {
    assertThat(CreateSourceAsProperties.from(ImmutableMap.of()),
        is(CreateSourceAsProperties.none()));
  }

  @Test
  public void shouldReturnOptionalEmptyForMissingProps() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(ImmutableMap.of());

    // Then:
    assertThat(properties.getKafkaTopic(), is(Optional.empty()));
    assertThat(properties.getTimestampColumnName(), is(Optional.empty()));
    assertThat(properties.getTimestampFormat(), is(Optional.empty()));
    assertThat(properties.getKeyFormat(), is(Optional.empty()));
    assertThat(properties.getValueFormat(), is(Optional.empty()));
    assertThat(properties.getReplicas(), is(Optional.empty()));
    assertThat(properties.getPartitions(), is(Optional.empty()));
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of()));
  }

  @Test
  public void shouldSetValidTimestampName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("ts")));

    // Then:
    assertThat(properties.getTimestampColumnName(), is(Optional.of(ColumnName.of("TS"))));
  }

  @Test
  public void shouldSetValidQualifiedTimestampName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("a.ts")));

    // Then:
    assertThat(properties.getTimestampColumnName(), is(Optional.of(ColumnName.of("TS"))));
  }

  @Test
  public void shouldSetValidTimestampFormat() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(
            CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY,
            new StringLiteral("yyyy-MM-dd'T'HH:mm:ss.SSS")
        )
    );

    // Then:
    assertThat(properties.getTimestampFormat(), is(Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSS")));
  }

  @Test
  public void shouldThrowOnInvalidTimestampFormat() {
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
  public void shouldAutomaticallySetAvroSchemaNameForKey() {
    // Given:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(KEY_FORMAT_PROPERTY, new StringLiteral("AVRO"))
    );

    // When:
    final String avroSchemaName = properties.getKeyFormatProperties("name", AvroFormat.NAME)
        .get(ConnectProperties.FULL_SCHEMA_NAME);

    // Then:
    assertThat(avroSchemaName, is("io.confluent.ksql.avro_schemas.NameKey"));
  }

  @Test
  public void shouldSetAvroSchemaNameForKey() {
    // Given:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(KEY_FORMAT_PROPERTY, new StringLiteral("AVRO"),
            KEY_SCHEMA_FULL_NAME, new StringLiteral("KeySchemaName"))
    );

    // When:
    final String avroSchemaName = properties.getKeyFormatProperties("name", AvroFormat.NAME)
        .get(ConnectProperties.FULL_SCHEMA_NAME);

    // Then:
    assertThat(avroSchemaName, is("KeySchemaName"));
  }

  @Test
  public void shouldNotSetAvroSchemaNameForNonAvroKey() {
    // Given:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(KEY_FORMAT_PROPERTY, new StringLiteral("JSON"))
    );

    // When:
    final String avroSchemaName = properties.getKeyFormatProperties("name", JsonFormat.NAME)
        .get(ConnectProperties.FULL_SCHEMA_NAME);

    // Then:
    assertThat(avroSchemaName, nullValue());
  }

  @Test
  public void shouldSetValidAvroSchemaName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema")));

    // Then:
    assertThat(properties.getValueFormatProperties(AvroFormat.NAME).get(ConnectProperties.FULL_SCHEMA_NAME),
        is("schema"));
  }

  @Test
  public void shouldSetValueFullSchemaName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.<String, Literal>builder()
            .put(CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("Protobuf"))
            .put(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME, new StringLiteral("schema"))
            .build());

    // Then:
    assertThat(properties.getValueFormatProperties(ProtobufFormat.NAME),
        hasEntry(ConnectProperties.FULL_SCHEMA_NAME, "schema"));
  }

  @Test
  public void shouldSetKeyFullSchemaName() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.<String, Literal>builder()
            .put(FORMAT_PROPERTY, new StringLiteral("Json_sr"))
            .put(KEY_SCHEMA_FULL_NAME, new StringLiteral("KeySchema"))
            .build());

    // Then:
    assertThat(props.getKeyFormatProperties("json_sr", "foo"),
        hasEntry(ConnectProperties.FULL_SCHEMA_NAME, "KeySchema"));
  }

  @Test
  public void shouldSetReplicasFromNumber() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(2)));

    // Then:
    assertThat(properties.getReplicas(), is(Optional.of((short) 2)));
  }

  @Test
  public void shouldSetPartitions() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(2)));

    // Then:
    assertThat(properties.getPartitions(), is(Optional.of(2)));
  }

  @Test
  public void shouldSetWrapSingleValues() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.WRAP_SINGLE_VALUE, new BooleanLiteral("true")));

    // Then:
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)));
  }

  @Test
  public void shouldSetNumericPropertyFromStringLiteral() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new StringLiteral("3")));

    // Then:
    assertThat(properties.getReplicas(), is(Optional.of((short) 3)));
  }

  @Test
  public void shouldSetBooleanPropertyFromStringLiteral() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.WRAP_SINGLE_VALUE, new StringLiteral("true")));

    // Then:
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)));
  }

  @Test
  public void shouldHandleNonUpperCasePropNames() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.WRAP_SINGLE_VALUE.toLowerCase(), new StringLiteral("false")));

    // Then:
    assertThat(properties.getValueSerdeFeatures(), is(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }


  @Test
  public void shouldFailIfInvalidConfig() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> from(
            of("foo", new StringLiteral("bar"))
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid config variable(s) in the WITH clause: FOO"));
  }

  @Test
  public void shouldProperlyImplementEqualsAndHashCode() {
    final ImmutableMap<String, Literal> someConfig = ImmutableMap
        .of(VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema"));

    new EqualsTester()
        .addEqualityGroup(
            CreateSourceAsProperties.from(someConfig),
            CreateSourceAsProperties.from(someConfig)
        )
        .addEqualityGroup(
            CreateSourceAsProperties.from(ImmutableMap.of())
        )
        .addEqualityGroup(
            CreateSourceAsProperties.from(ImmutableMap.of())
                .withUnwrapProtobufPrimitives(true)
        )
        .testEquals();
  }

  @Test
  public void shouldIncludeOnlyProvidedPropsInToString() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            "Wrap_Single_value", new StringLiteral("True"),
            "KAFKA_TOPIC", new StringLiteral("foo")));

    // When:
    final String sql = props.toString();

    // Then:
    assertThat(sql, is("KAFKA_TOPIC='foo', WRAP_SINGLE_VALUE='True'"));
  }

  @Test
  public void shouldNotQuoteNonStringPropValues() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of("Wrap_Single_value", new BooleanLiteral("true")));

    // When:
    final String sql = props.toString();

    // Then:
    assertThat(sql, containsString("WRAP_SINGLE_VALUE=true"));
  }

  @Test
  public void shouldGetKeyAndValueFormatFromFormat() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            KEY_FORMAT_PROPERTY, new StringLiteral("KAFKA"),
            VALUE_FORMAT_PROPERTY, new StringLiteral("AVRO")));

    // When / Then:
    assertThat(props.getKeyFormat(), is(Optional.of("KAFKA")));
    assertThat(props.getValueFormat(), is(Optional.of("AVRO")));
  }

  @Test
  public void shouldThrowIfValueSchemaNameAndAvroSchemaNameProvided() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceAsProperties.from(
            ImmutableMap.<String, Literal>builder()
                .put(VALUE_SCHEMA_FULL_NAME, new StringLiteral("value_schema"))
                .put(VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("value_schema"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), is("Cannot supply both 'VALUE_AVRO_SCHEMA_FULL_NAME' "
        + "and 'VALUE_SCHEMA_FULL_NAME' properties. Please only set 'VALUE_SCHEMA_FULL_NAME'."));
  }

  @Test
  public void shouldThrowIfKeyFormatAndFormatProvided() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceAsProperties.from(
            ImmutableMap.<String, Literal>builder()
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
  public void shouldGetKeyAndValueSchemaIdFromFormat() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.<String, Literal>builder()
            .put(FORMAT_PROPERTY, new StringLiteral("AVRO"))
            .put(KEY_SCHEMA_ID, new IntegerLiteral(123))
            .put(VALUE_SCHEMA_ID, new IntegerLiteral(456))
            .build());

    // When / Then:
    assertThat(props.getKeyFormatProperties("foo", "Avro"),
        hasEntry(ConnectProperties.SCHEMA_ID, "123"));
    assertThat(props.getValueFormatProperties(AvroFormat.NAME), hasEntry(ConnectProperties.SCHEMA_ID, "456"));
  }

  @Test
  public void shouldThrowIfValueFormatAndFormatProvided() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceAsProperties.from(
            ImmutableMap.<String, Literal>builder()
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
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(FORMAT_PROPERTY, new StringLiteral("PROTOBUF")))
        .withUnwrapProtobufPrimitives(true);

    // When / Then:
    assertThat(props.getKeyFormatProperties("foo", "PROTOBUF"),
        hasEntry(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP));
  }

  @Test
  public void shouldGetProtobufKeyFormatPropertiesWithoutUnwrapping() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(FORMAT_PROPERTY, new StringLiteral("PROTOBUF")));

    // When / Then:
    assertThat(props.getKeyFormatProperties("foo", "PROTOBUF"),
        not(hasKey(ProtobufProperties.UNWRAP_PRIMITIVES)));
  }

  @Test
  public void shouldGetProtobufValueFormatPropertiesWithUnwrapping() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(FORMAT_PROPERTY, new StringLiteral("PROTOBUF")))
        .withUnwrapProtobufPrimitives(true);

    // When / Then:
    assertThat(props.getValueFormatProperties("PROTOBUF"),
        hasEntry(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP));
  }

  @Test
  public void shouldGetProtobufValueFormatPropertiesWithoutUnwrapping() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(FORMAT_PROPERTY, new StringLiteral("PROTOBUF")));

    // When / Then:
    assertThat(props.getValueFormatProperties("PROTOBUF"),
        not(hasKey(ProtobufProperties.UNWRAP_PRIMITIVES)));
  }

  @Test
  public void shouldThrowIfSourceConnectorPropertyProvided() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CreateSourceAsProperties.from(
            ImmutableMap.<String, Literal>builder()
                .put(CreateConfigs.SOURCED_BY_CONNECTOR_PROPERTY, new StringLiteral("whatever"))
                .build())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid config variable(s) in the WITH clause: SOURCED_BY_CONNECTOR"));
  }

  @Test
  public void shouldGetProtobufKeyFormatPropertiesWithNullableAsWrapper() {
    shouldGetKeyFormatPropertiesWithNullableAsWrapper("PROTOBUF");
  }

  @Test
  public void shouldGetProtobufNoSRKeyFormatPropertiesWithNullableAsWrapper() {
    shouldGetKeyFormatPropertiesWithNullableAsWrapper("PROTOBUF_NOSR");
  }

  private void shouldGetKeyFormatPropertiesWithNullableAsWrapper(final String format) {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            FORMAT_PROPERTY, new StringLiteral(format),
            KEY_PROTOBUF_NULLABLE_REPRESENTATION, new StringLiteral(
                ProtobufNullableConfigValues.WRAPPER.name())
        ));

    // When / Then:
    assertThat(props.getKeyFormatProperties("foo", format),
        hasEntry(ProtobufProperties.NULLABLE_REPRESENTATION,
            ProtobufProperties.NULLABLE_AS_WRAPPER));
  }

  @Test
  public void shouldGetProtobufKeyFormatPropertiesWithNullableAsOptional() {
    shouldGetKeyFormatPropertiesWithNullableAsOptional("PROTOBUF");
  }

  @Test
  public void shouldGetProtobufNoSRKeyFormatPropertiesWithNullableAsOptional() {
    shouldGetKeyFormatPropertiesWithNullableAsOptional("PROTOBUF_NOSR");
  }

  private void shouldGetKeyFormatPropertiesWithNullableAsOptional(final String format) {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            FORMAT_PROPERTY, new StringLiteral(format),
            KEY_PROTOBUF_NULLABLE_REPRESENTATION, new StringLiteral(
                ProtobufNullableConfigValues.OPTIONAL.name())));

    // When / Then:
    assertThat(props.getKeyFormatProperties("foo", format),
        hasEntry(ProtobufProperties.NULLABLE_REPRESENTATION,
            ProtobufProperties.NULLABLE_AS_OPTIONAL));
  }

  @Test
  public void shouldGetProtobufKeyFormatPropertiesWithoutNullableRepresentation() {
    shouldGetKeyFormatPropertiesWithoutNullableRepresentation("PROTOBUF");
  }

  @Test
  public void shouldGetProtobufNoSRKeyFormatPropertiesWithoutNullableRepresentation() {
    shouldGetKeyFormatPropertiesWithoutNullableRepresentation("PROTOBUF_NOSR");
  }

  private void shouldGetKeyFormatPropertiesWithoutNullableRepresentation(final String format) {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(FORMAT_PROPERTY, new StringLiteral(format)));

    // When / Then:
    assertThat(props.getKeyFormatProperties("foo", format),
        not(hasKey(ProtobufProperties.NULLABLE_REPRESENTATION)));
  }

  @Test
  public void shouldGetProtobufValueFormatPropertiesWithNullableAsWrapper() {
    shouldGetValueFormatPropertiesWithNullableAsWrapper("PROTOBUF");
  }

  @Test
  public void shouldGetProtobufNoSRValueFormatPropertiesWithNullableAsWrapper() {
    shouldGetValueFormatPropertiesWithNullableAsWrapper("PROTOBUF_NOSR");
  }

  private void shouldGetValueFormatPropertiesWithNullableAsWrapper(final String format) {
    // Given:

    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            FORMAT_PROPERTY, new StringLiteral(format),
            VALUE_PROTOBUF_NULLABLE_REPRESENTATION, new StringLiteral(
                ProtobufNullableConfigValues.WRAPPER.name())));

    // When / Then:
    assertThat(props.getValueFormatProperties(format),
        hasEntry(ProtobufProperties.NULLABLE_REPRESENTATION,
            ProtobufProperties.NULLABLE_AS_WRAPPER));
  }

  @Test
  public void shouldGetProtobufValueFormatPropertiesWithNullableAsOptional() {
    shouldGetValueFormatPropertiesWithNullableAsOptional("PROTOBUF");
  }

  @Test
  public void shouldGetProtobufNoSRValueFormatPropertiesWithNullableAsOptional() {
    shouldGetValueFormatPropertiesWithNullableAsOptional("PROTOBUF_NOSR");
  }

  private void shouldGetValueFormatPropertiesWithNullableAsOptional(final String format) {
    // Given:

    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            FORMAT_PROPERTY, new StringLiteral(format),
            VALUE_PROTOBUF_NULLABLE_REPRESENTATION, new StringLiteral(
                ProtobufNullableConfigValues.OPTIONAL.name())));

    // When / Then:
    assertThat(props.getValueFormatProperties(format),
        hasEntry(ProtobufProperties.NULLABLE_REPRESENTATION,
            ProtobufProperties.NULLABLE_AS_OPTIONAL));
  }

  @Test
  public void shouldGetProtobufValueFormatPropertiesWithoutNullableRepresentation() {
    shouldGetValueFormatPropertiesWithoutNullableRepresentation("PROTOBUF");
  }

  @Test
  public void shouldGetProtobufNoSRValueFormatPropertiesWithoutNullableRepresentation() {
    shouldGetValueFormatPropertiesWithoutNullableRepresentation("PROTOBUF_NOSR");
  }

  private void shouldGetValueFormatPropertiesWithoutNullableRepresentation(final String format) {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(FORMAT_PROPERTY, new StringLiteral(format)));

    // When / Then:
    assertThat(props.getValueFormatProperties(format),
        not(hasKey(ProtobufProperties.NULLABLE_REPRESENTATION)));
  }

  @Test
  public void shouldFailWhenProtobufPropertiesAreUsedOnOtherKeyFormats() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            FORMAT_PROPERTY, new StringLiteral("JSON"),
            KEY_PROTOBUF_NULLABLE_REPRESENTATION, new StringLiteral(
                ProtobufNullableConfigValues.OPTIONAL.name())));

    // When / Then:
    final Exception e = assertThrows(KsqlException.class,
        () -> props.getKeyFormatProperties("", "JSON"));
    assertThat(e.getMessage(), is(equalTo(
        "Property KEY_PROTOBUF_NULLABLE_REPRESENTATION can only be enabled with protobuf format")));
  }

  @Test
  public void shouldFailWhenProtobufPropertiesAreUsedOnOtherValueFormats() {
    // Given:
    final CreateSourceAsProperties props = CreateSourceAsProperties
        .from(ImmutableMap.of(
            FORMAT_PROPERTY, new StringLiteral("JSON"),
            VALUE_PROTOBUF_NULLABLE_REPRESENTATION, new StringLiteral(
                ProtobufNullableConfigValues.OPTIONAL.name())));

    // When / Then:
    final Exception e = assertThrows(KsqlException.class,
        () -> props.getValueFormatProperties("JSON"));
    assertThat(e.getMessage(), is(equalTo(
        "Property VALUE_PROTOBUF_NULLABLE_REPRESENTATION can only be enabled with protobuf format")));
  }
}