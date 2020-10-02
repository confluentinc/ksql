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
import static io.confluent.ksql.properties.with.CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY;
import static io.confluent.ksql.properties.with.CommonCreateConfigs.VALUE_FORMAT_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
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
  public void shouldSetValidAvroSchemaName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema")));

    // Then:
    assertThat(properties.getValueFormatProperties().get(AvroFormat.FULL_SCHEMA_NAME), is("schema"));
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
        .of(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema"));

    new EqualsTester()
        .addEqualityGroup(
            CreateSourceAsProperties.from(someConfig),
            CreateSourceAsProperties.from(someConfig)
        )
        .addEqualityGroup(
            CreateSourceAsProperties.from(ImmutableMap.of())
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
}