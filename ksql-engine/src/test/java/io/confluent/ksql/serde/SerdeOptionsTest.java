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

package io.confluent.ksql.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerdeOptionsTest {

  private static final String TOPIC_NAME = "some topic";

  private static final LogicalSchema SINGLE_FIELD_SCHEMA = LogicalSchema.of(SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
      .build());

  private static final LogicalSchema MULTI_FIELD_SCHEMA = LogicalSchema.of(SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
      .field("f1", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build());

  private static final List<String> SINGLE_COLUMN_NAME = ImmutableList.of("bob");
  private static final List<String> MULTI_FIELD_NAMES = ImmutableList.of("bob", "vic");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private CreateSourceProperties withProperties;

  private final Map<String, Expression> properties = new HashMap<>();
  private final Set<SerdeOption> singleFieldDefaults = new HashSet<>();
  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(ImmutableMap.of());

    properties.clear();
    singleFieldDefaults.clear();
    when(withProperties.getValueFormat()).thenReturn(Format.JSON);
  }

  @Test
  public void shouldBuildDefaultsWithWrappedSingleValuesByDefault() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildDefaults(ksqlConfig);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldBuildDefaultsWithWrappedSingleValuesIfConfigured() {
    // When:
    final Set<SerdeOption> result = SerdeOptions.buildDefaults(ksqlConfig);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldBuildDefaultsWithUnwrappedSingleValuesIfConfigured() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildDefaults(ksqlConfig);

    // Then:
    assertThat(result, hasItem(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeConfigForCreateStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    when(withProperties.getWrapSingleValues()).thenReturn(Optional.of(false));

    // When:
    final Set<SerdeOption> result = SerdeOptions
        .buildForCreateStatement(SINGLE_FIELD_SCHEMA, withProperties, ksqlConfig);

    // Then:
    assertThat(result, hasItem(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldGetSingleValueWrappingFromConfigForCreateStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final Set<SerdeOption> result = SerdeOptions
        .buildForCreateStatement(SINGLE_FIELD_SCHEMA, withProperties, ksqlConfig);

    // Then:
    assertThat(result, hasItem(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldSingleValueWrappingFromDefaultConfigForCreateStatement() {
    // When:
    final Set<SerdeOption> result = SerdeOptions
        .buildForCreateStatement(SINGLE_FIELD_SCHEMA, withProperties, ksqlConfig);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromConfigForMultiFieldsForCreateStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final Set<SerdeOption> result = SerdeOptions
        .buildForCreateStatement(MULTI_FIELD_SCHEMA, withProperties, ksqlConfig);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateStatement() {
    // Given:
    when(withProperties.getWrapSingleValues()).thenReturn(Optional.of(true));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas");

    // When:
    SerdeOptions.buildForCreateStatement(MULTI_FIELD_SCHEMA, withProperties, ksqlConfig);
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForDelimitedForCreateStatement() {
    // Given:
    when(withProperties.getValueFormat()).thenReturn(Format.DELIMITED);
    when(withProperties.getWrapSingleValues()).thenReturn(Optional.of(false));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' can not be used with format 'DELIMITED' as it does not support wrapping");

    // When:
    SerdeOptions.buildForCreateStatement(SINGLE_FIELD_SCHEMA, withProperties, ksqlConfig);
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeDefaultsForCreateAsStatement() {
    // Given:
    properties.put(DdlConfig.WRAP_SINGLE_VALUE, new BooleanLiteral("true"));
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions
        .buildForCreateAsStatement(SINGLE_COLUMN_NAME, properties, Format.AVRO, singleFieldDefaults);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromDefaultsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions
        .buildForCreateAsStatement(SINGLE_COLUMN_NAME, properties, Format.JSON, singleFieldDefaults);

    // Then:
    assertThat(result, hasItem(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromDefaultForMultiFieldsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions
        .buildForCreateAsStatement(MULTI_FIELD_NAMES, properties, Format.AVRO, singleFieldDefaults);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateAsStatement() {
    // Given:
    properties.put(DdlConfig.WRAP_SINGLE_VALUE, new BooleanLiteral("true"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas");

    // When:
    SerdeOptions
        .buildForCreateAsStatement(MULTI_FIELD_NAMES, properties, Format.JSON, singleFieldDefaults);
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForDelimitedForCreateAsStatement() {
    // Given:
    properties.put(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral(Format.DELIMITED.toString()));
    properties.put(DdlConfig.WRAP_SINGLE_VALUE, new BooleanLiteral("true"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' can not be used with format 'DELIMITED' as it does not support wrapping");

    // When:
    SerdeOptions
        .buildForCreateAsStatement(SINGLE_COLUMN_NAME, properties, Format.DELIMITED, singleFieldDefaults);
  }
}