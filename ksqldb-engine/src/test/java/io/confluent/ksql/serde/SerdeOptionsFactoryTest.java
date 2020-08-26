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
import static io.confluent.ksql.serde.FormatFactory.PROTOBUF;
import static io.confluent.ksql.serde.SerdeOptionsFactory.buildForCreateAsStatement;
import static io.confluent.ksql.serde.SerdeOptionsFactory.buildForCreateStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerdeOptionsFactoryTest {

  private static final LogicalSchema SINGLE_FIELD_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema MULTI_FIELD_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("f1"), SqlTypes.DOUBLE)
      .build();

  private static final List<ColumnName> SINGLE_COLUMN_NAME = ImmutableList.of(ColumnName.of("bob"));
  private static final List<ColumnName> MULTI_FIELD_NAMES = ImmutableList.of(ColumnName.of("bob"), ColumnName.of("vic"));

  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(ImmutableMap.of());
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeConfigForCreateStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        FormatFactory.JSON,
        SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromConfigForCreateStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        FormatFactory.JSON,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldDefaultToNoSingleValueWrappingIfNoExplicitAndNoConfigDefault() {
    // When:
    final SerdeOptions result = SerdeOptionsFactory.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        FormatFactory.JSON,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of()));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromDefaultConfigIfFormatDoesNotSupportIt() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.buildForCreateStatement(
        SINGLE_FIELD_SCHEMA,
        FormatFactory.KAFKA,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of()));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromConfigForMultiFieldsForCreateStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.buildForCreateStatement(
        MULTI_FIELD_SCHEMA,
        FormatFactory.JSON,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of()));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateStatement() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildForCreateStatement(
            MULTI_FIELD_SCHEMA,
            JSON,
            SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES),
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
            PROTOBUF,
            SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES),
            ksqlConfig
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Format 'PROTOBUF' does not support 'WRAP_SINGLE_VALUE' set to 'false'."));
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeDefaultsForCreateAsStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeOptions result = buildForCreateAsStatement(
        SINGLE_COLUMN_NAME,
        FormatFactory.AVRO,
        SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromDefaultsForCreateAsStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeOptions result = buildForCreateAsStatement(
        SINGLE_COLUMN_NAME,
        FormatFactory.JSON,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromDefaultForMultiFieldsForCreateAsStatement() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeOptions result = buildForCreateAsStatement(
        MULTI_FIELD_NAMES,
        FormatFactory.AVRO,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result, is(SerdeOptions.of()));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiFieldForCreateAsStatement() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildForCreateAsStatement(
            MULTI_FIELD_NAMES,
            JSON,
            SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES),
            ksqlConfig
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
            SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES),
            ksqlConfig
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Format 'DELIMITED' does not support 'WRAP_SINGLE_VALUE' set to 'true'."));
  }
}