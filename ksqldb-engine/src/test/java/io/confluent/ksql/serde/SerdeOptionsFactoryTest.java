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

import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.serde.FormatFactory.PROTOBUF;
import static io.confluent.ksql.serde.SerdeOptionsFactory.build;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerdeOptionsFactoryTest {

  private static final LogicalSchema SINGLE_FIELD_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema MULTI_FIELD_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("f1"), SqlTypes.DOUBLE)
      .build();

  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(ImmutableMap.of());
  }

  @Test
  public void shouldGetSingleValueWrappingFromPropertiesBeforeConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        SINGLE_FIELD_SCHEMA,
        KAFKA,
        FormatFactory.JSON,
        SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.VALUE_WRAPPING_OPTIONS),
        is(Optional.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        SINGLE_FIELD_SCHEMA,
        KAFKA,
        FormatFactory.JSON,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.VALUE_WRAPPING_OPTIONS),
        is(Optional.of(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldDefaultToNoSingleValueWrappingIfNoExplicitAndNoConfigDefault() {
    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        SINGLE_FIELD_SCHEMA,
        KAFKA,
        FormatFactory.JSON,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.VALUE_WRAPPING_OPTIONS), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromDefaultConfigIfFormatDoesNotSupportIt() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        SINGLE_FIELD_SCHEMA,
        KAFKA,
        KAFKA,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.VALUE_WRAPPING_OPTIONS), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromConfigForMultiFields() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        MULTI_FIELD_SCHEMA,
        FormatFactory.AVRO,
        FormatFactory.JSON,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.VALUE_WRAPPING_OPTIONS), is(Optional.empty()));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiField() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> build(
            MULTI_FIELD_SCHEMA,
            KAFKA,
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
  public void shouldThrowIfWrapSingleValuePresentForDelimited() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> build(
            SINGLE_FIELD_SCHEMA,
            KAFKA,
            PROTOBUF,
            SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES),
            ksqlConfig
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Format 'PROTOBUF' does not support 'WRAP_SINGLE_VALUE' set to 'false'."));
  }

  @Test
  public void shouldSetUnwrappedKeysIfKeyFormatSupportsBothWrappingAndUnwrapping() {
    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        SINGLE_FIELD_SCHEMA,
        JSON,
        PROTOBUF,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.KEY_WRAPPING_OPTIONS),
        is(Optional.of(SerdeOption.UNWRAP_SINGLE_KEYS)));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIfKeyFormatsSupportsOnlyWrapping() {
    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        SINGLE_FIELD_SCHEMA,
        PROTOBUF,
        PROTOBUF,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.KEY_WRAPPING_OPTIONS), is(Optional.empty()));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIfKeyFormatsSupportsOnlyUnwrapping() {
    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        SINGLE_FIELD_SCHEMA,
        KAFKA,
        PROTOBUF,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.KEY_WRAPPING_OPTIONS), is(Optional.empty()));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIfFormatSupportsBothButIsKeyLess() {
    // Given:
    final LogicalSchema keyLessSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        keyLessSchema,
        KAFKA,
        PROTOBUF,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.KEY_WRAPPING_OPTIONS), is(Optional.empty()));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIfFormatSupportsBothButIsMultipleKeyColumns() {
    // Given:
    final LogicalSchema multiKeySchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.BIGINT)
        .keyColumn(ColumnName.of("k1"), SqlTypes.BIGINT)
        .build();

    // When:
    final SerdeOptions result = SerdeOptionsFactory.build(
        multiKeySchema,
        KAFKA,
        PROTOBUF,
        SerdeOptions.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeOptions.KEY_WRAPPING_OPTIONS), is(Optional.empty()));
  }
}