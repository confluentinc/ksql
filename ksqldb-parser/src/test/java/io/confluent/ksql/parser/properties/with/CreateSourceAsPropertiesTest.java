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
import static io.confluent.ksql.properties.with.CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY;
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
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Test;

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
    assertThat(properties.getValueFormat(), is(Optional.empty()));
    assertThat(properties.getTimestampColumnName(), is(Optional.empty()));
    assertThat(properties.getTimestampFormat(), is(Optional.empty()));
    assertThat(properties.getFormatInfo(), is(Optional.empty()));
    assertThat(properties.getReplicas(), is(Optional.empty()));
    assertThat(properties.getPartitions(), is(Optional.empty()));
    assertThat(properties.getSerdeOptions(), is(SerdeOptions.of()));
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
    assertThat(e.getMessage(), containsString("Invalid datatime format for config:TIMESTAMP_FORMAT, reason:Unknown pattern letter: i"));
  }

  @Test
  public void shouldSetValidAvroSchemaName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema")));

    // Then:
    assertThat(properties.getFormatProperties().get(AvroFormat.FULL_SCHEMA_NAME), is("schema"));
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
    assertThat(properties.getSerdeOptions(), is(SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES)));
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
    assertThat(properties.getSerdeOptions(), is(SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldHandleNonUpperCasePropNames() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.WRAP_SINGLE_VALUE.toLowerCase(), new StringLiteral("false")));

    // Then:
    assertThat(properties.getSerdeOptions(), is(SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
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
}