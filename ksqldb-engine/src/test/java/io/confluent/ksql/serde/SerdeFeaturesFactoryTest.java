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
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerdeFeaturesFactoryTest {

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
  public void shouldGetSingleValueWrappingFromPropertiesBeforeConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildValueFeatures(
        SINGLE_FIELD_SCHEMA,
        JSON,
        SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES),
        is(Optional.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldGetSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildValueFeatures(
        SINGLE_FIELD_SCHEMA,
        JSON,
        SerdeFeatures.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES),
        is(Optional.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldDefaultToNoSingleValueWrappingIfNoExplicitAndNoConfigDefault() {
    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildValueFeatures(
        SINGLE_FIELD_SCHEMA,
        JSON,
        SerdeFeatures.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromDefaultConfigIfFormatDoesNotSupportIt() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildValueFeatures(
        SINGLE_FIELD_SCHEMA,
        KAFKA,
        SerdeFeatures.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetSingleValueWrappingFromConfigForMultiFields() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildValueFeatures(
        MULTI_FIELD_SCHEMA,
        JSON,
        SerdeFeatures.of(),
        ksqlConfig
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES), is(Optional.empty()));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForMultiField() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> SerdeFeaturesFactory.buildValueFeatures(
            MULTI_FIELD_SCHEMA,
            JSON,
            SerdeFeatures.of(SerdeFeature.WRAP_SINGLES),
            ksqlConfig
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas"));
  }

  @Test
  public void shouldThrowIfWrapSingleValuePresentForFormatThatDoesNotSupportIt() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> SerdeFeaturesFactory.buildValueFeatures(
            SINGLE_FIELD_SCHEMA,
            PROTOBUF,
            SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
            ksqlConfig
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Format 'PROTOBUF' does not support 'WRAP_SINGLE_VALUE' set to 'false'."));
  }

  @Test
  public void shouldSetUnwrappedKeysIfKeyFormatSupportsBothWrappingAndUnwrapping() {
    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildKeyFeatures(
        SINGLE_FIELD_SCHEMA,
        JSON
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES),
        is(Optional.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIfKeyFormatsSupportsOnlyWrapping() {
    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildKeyFeatures(
        SINGLE_FIELD_SCHEMA,
        PROTOBUF
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES), is(Optional.empty()));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIfKeyFormatsSupportsOnlyUnwrapping() {
    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildKeyFeatures(
        SINGLE_FIELD_SCHEMA,
        KAFKA
    );

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES), is(Optional.empty()));
  }

  @Test
  public void shouldSetUnwrappedKeysIfInternalTopicHasKeyFormatSupportsBothWrappingAndUnwrapping() {
    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildInternal(JSON);

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES),
        is(Optional.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIfInternalTopicHasKeyFormatsSupportsOnlyWrapping() {
    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildInternal(PROTOBUF);

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES), is(Optional.empty()));
  }

  @Test
  public void shouldNotSetUnwrappedKeysIInternalTopicHasfKeyFormatsSupportsOnlyUnwrapping() {
    // When:
    final SerdeFeatures result = SerdeFeaturesFactory.buildInternal(KAFKA);

    // Then:
    assertThat(result.findAny(SerdeFeatures.WRAPPING_FEATURES), is(Optional.empty()));
  }
}