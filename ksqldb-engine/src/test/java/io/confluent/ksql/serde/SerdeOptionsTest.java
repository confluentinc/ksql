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

import static io.confluent.ksql.serde.FormatFactory.DELIMITED;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.SerdeOptions.buildForCreateAsStatement;
import static io.confluent.ksql.serde.SerdeOptions.buildForCreateStatement;
import static java.util.Optional.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerdeOptionsTest {

  private static final LogicalSchema SINGLE_FIELD_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema MULTI_FIELD_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("f1"), SqlTypes.DOUBLE)
      .build();

  private static final List<ColumnName> SINGLE_COLUMN_NAME = ImmutableList.of(ColumnName.of("bob"));
  private static final List<ColumnName> MULTI_FIELD_NAMES = ImmutableList.of(ColumnName.of("bob"), ColumnName.of("vic"));

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
        FormatFactory.JSON,
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
        FormatFactory.JSON,
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
        FormatFactory.JSON,
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
        FormatFactory.JSON,
        Optional.empty(),
        ksqlConfig
    );

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateStatement() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildForCreateStatement(
            MULTI_FIELD_SCHEMA,
            JSON,
            of(true),
            ksqlConfig
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas"));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForDelimitedForCreateStatement() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildForCreateStatement(
            SINGLE_FIELD_SCHEMA,
            DELIMITED,
            of(false),
            ksqlConfig
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("'WRAP_SINGLE_VALUE' can not be used with format 'DELIMITED' as it does not support wrapping"));
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeDefaultsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateAsStatement(
        SINGLE_COLUMN_NAME, FormatFactory.AVRO, Optional.of(true), singleFieldDefaults);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromDefaultsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateAsStatement(
        SINGLE_COLUMN_NAME, FormatFactory.JSON, Optional.empty(), singleFieldDefaults);

    // Then:
    assertThat(result, hasItem(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromDefaultForMultiFieldsForCreateAsStatement() {
    // Given:
    singleFieldDefaults.add(SerdeOption.UNWRAP_SINGLE_VALUES);

    // When:
    final Set<SerdeOption> result = SerdeOptions.buildForCreateAsStatement(
        MULTI_FIELD_NAMES, FormatFactory.AVRO, Optional.empty(), singleFieldDefaults);

    // Then:
    assertThat(result, not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateAsStatement() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildForCreateAsStatement(
            MULTI_FIELD_NAMES,
            JSON,
            of(true),
            singleFieldDefaults
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("'WRAP_SINGLE_VALUE' is only valid for single-field value schemas"));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForDelimitedForCreateAsStatement() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildForCreateAsStatement(
            SINGLE_COLUMN_NAME,
            DELIMITED,
            of(true),
            singleFieldDefaults
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("'WRAP_SINGLE_VALUE' can not be used with format 'DELIMITED' as it does not support wrapping"));
  }
}