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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CreateSourceAsPropertiesTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
    assertThat(properties.getValueAvroSchemaName(), is(Optional.empty()));
    assertThat(properties.getReplicas(), is(Optional.empty()));
    assertThat(properties.getPartitions(), is(Optional.empty()));
    assertThat(properties.getWrapSingleValues(), is(Optional.empty()));
  }

  @Test
  public void shouldSetValidTimestampName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("ts")));

    // Then:
    assertThat(properties.getTimestampColumnName(), is(Optional.of("ts")));
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
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid datatime format for config:TIMESTAMP_FORMAT, reason:Unknown pattern letter: i");

    // When:
    CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY, new StringLiteral("invalid")));
  }

  @Test
  public void shouldSetValidAvroSchemaName() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("schema")));

    // Then:
    assertThat(properties.getValueAvroSchemaName(), is(Optional.of("schema")));
  }

  @Test
  public void shouldCleanQuotesForStrings() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME, new StringLiteral("'schema'")));

    // Then:
    assertThat(properties.getValueAvroSchemaName(), is(Optional.of("schema")));
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
    assertThat(properties.getWrapSingleValues(), is(Optional.of(true)));
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
    assertThat(properties.getWrapSingleValues(), is(Optional.of(true)));
  }

  @Test
  public void shouldHandleNonUpperCasePropNames() {
    // When:
    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(
        ImmutableMap.of(CommonCreateConfigs.WRAP_SINGLE_VALUE.toLowerCase(), new StringLiteral("false")));

    // Then:
    assertThat(properties.getWrapSingleValues(), is(Optional.of(false)));
  }


  @Test
  public void shouldFailIfInvalidConfig() {
    // Expect:
    expectedException.expectMessage("Invalid config variable(s) in the WITH clause: FOO");
    expectedException.expect(KsqlException.class);

    // When:
    CreateSourceAsProperties.from(
        ImmutableMap.of("foo", new StringLiteral("bar"))
    );
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