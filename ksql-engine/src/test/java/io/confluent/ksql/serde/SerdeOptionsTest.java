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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerdeOptionsTest {

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

  private final Set<SerdeOption> singleFieldDefaults = new HashSet<>();
  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(ImmutableMap.of());
    singleFieldDefaults.clear();
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

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        Format.JSON,
        Optional.of(false),
        ksqlConfig
    );

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
    final Set<SerdeOption> result = SerdeOptions.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        Format.JSON,
        Optional.empty(),
        ksqlConfig
    );

    // Then:
    assertThat(result, hasItem(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldSingleValueWrappingFromDefaultConfigForCreateStatement() {
    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        Format.JSON,
        Optional.empty(),
        ksqlConfig
    );

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
    final Set<SerdeOption> result = SerdeOptions.buildForCreateStatement(
        MULTI_FIELD_SCHEMA,
        Format.JSON,
        Optional.empty(),
        ksqlConfig
    );

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateStatement() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas");

    // When:
    SerdeOptions.buildForCreateStatement(
        MULTI_FIELD_SCHEMA,
        Format.JSON,
        Optional.of(true),
        ksqlConfig
    );
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForDelimitedForCreateStatement() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' can not be used with format 'DELIMITED' as it does not support wrapping");

    // When:
    SerdeOptions.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        Format.DELIMITED,
        Optional.of(false),
        ksqlConfig
    );
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeDefaultsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateAsStatement(
        SINGLE_COLUMN_NAME, Format.AVRO, Optional.of(true), singleFieldDefaults);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromDefaultsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateAsStatement(
        SINGLE_COLUMN_NAME, Format.JSON, Optional.empty(), singleFieldDefaults);

    // Then:
    assertThat(result, hasItem(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromDefaultForMultiFieldsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateAsStatement(
        MULTI_FIELD_NAMES, Format.AVRO, Optional.empty(), singleFieldDefaults);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateAsStatement() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas");

    // When:
    SerdeOptions.buildForCreateAsStatement(
        MULTI_FIELD_NAMES,
        Format.JSON,
        Optional.of(true),
        singleFieldDefaults
    );
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForDelimitedForCreateAsStatement() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' can not be used with format 'DELIMITED' as it does not support wrapping");

    // When:
    SerdeOptions.buildForCreateAsStatement(
        SINGLE_COLUMN_NAME,
        Format.DELIMITED,
        Optional.of(true),
        singleFieldDefaults
    );
  }
}