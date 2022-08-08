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

package io.confluent.ksql.schema.ksql;

import static io.confluent.ksql.schema.ksql.ColumnMatchers.headersColumn;
import static io.confluent.ksql.schema.ksql.ColumnMatchers.keyColumn;
import static io.confluent.ksql.schema.ksql.ColumnMatchers.valueColumn;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWOFFSET_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWTIME_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWEND_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWSTART_NAME;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BOOLEAN;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BYTES;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DOUBLE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.INTEGER;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"UnstableApiUsage", "unchecked"})
@RunWith(MockitoJUnitRunner.class)
public class LogicalSchemaTest {

  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");
  private static final ColumnName KEY = ColumnName.of("key");
  private static final ColumnName V0 = ColumnName.of("v0");
  private static final ColumnName V1 = ColumnName.of("v1");
  private static final ColumnName F0 = ColumnName.of("f0");
  private static final ColumnName F1 = ColumnName.of("f1");
  private static final ColumnName H0 = ColumnName.of("h0");
  private static final ColumnName H1 = ColumnName.of("h1");
  private static final ColumnName VALUE = ColumnName.of("value");

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueColumn(F0, STRING)
      .keyColumn(K0, BIGINT)
      .valueColumn(F1, BIGINT)
      .headerColumn(H0, Optional.empty())
      .build();

  private static final LogicalSchema SCHEMA_WITH_EXTRACTED_HEADERS = LogicalSchema.builder()
      .valueColumn(F0, STRING)
      .keyColumn(K0, BIGINT)
      .valueColumn(F1, BIGINT)
      .headerColumn(H0, Optional.of("key0"))
      .headerColumn(H1, Optional.of("key1"))
      .build();

  private static final SqlType HEADERS_TYPE = SqlArray.of(
      SqlStruct.builder()
          .field("KEY", SqlTypes.STRING)
          .field("VALUE", SqlTypes.BYTES).build());

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsProperly() {

    final LogicalSchema aSchema = LogicalSchema.builder()
        .keyColumn(KEY, BIGINT)
        .valueColumn(VALUE, STRING)
        .build();

    new EqualsTester()
        .addEqualityGroup(
            LogicalSchema.builder()
                .keyColumn(K0, BIGINT)
                .valueColumn(V0, STRING)
                .build(),

            LogicalSchema.builder()
                .keyColumn(K0, BIGINT)
                .valueColumn(V0, STRING)
                .build()
        )
        .addEqualityGroup(
            LogicalSchema.builder()
                .valueColumn(V0, STRING)
                .keyColumn(K0, BIGINT)
                .build()
        )
        .addEqualityGroup(
            LogicalSchema.builder()
                .keyColumn(K0, DOUBLE)
                .valueColumn(V0, STRING)
                .build()
        )
        .addEqualityGroup(
            LogicalSchema.builder()
                .keyColumn(K0, BIGINT)
                .valueColumn(V0, DOUBLE)
                .build()
        )
        .addEqualityGroup(
            aSchema,
            aSchema
                .withPseudoAndKeyColsInValue(false)
                .withoutPseudoAndKeyColsInValue(),

            aSchema
                .withPseudoAndKeyColsInValue(true)
                .withoutPseudoAndKeyColsInValue()
        )
        .addEqualityGroup(
            aSchema.withPseudoAndKeyColsInValue(true)
        )
        .addEqualityGroup(
            aSchema.withPseudoAndKeyColsInValue(false)
        )
        .testEquals();
  }

  @Test
  public void shouldExposeColumnIndexWithinValueNamespace() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .keyColumn(K0, BIGINT)
        .valueColumn(F1, BIGINT)
        .keyColumn(K1, BOOLEAN)
        .build();

    // When:
    final List<Column> result = schema.columns();

    // Then:
    assertThat(result, contains(
        Column.of(F0, STRING, Namespace.VALUE, 0),
        Column.of(K0, BIGINT, Namespace.KEY, 0),
        Column.of(F1, BIGINT, Namespace.VALUE, 1),
        Column.of(K1, BOOLEAN, Namespace.KEY, 1)
    ));
  }

  @Test
  public void shouldGetColumnByName() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueColumn(F0);

    // Then:
    assertThat(result, is(Optional.of(
        Column.of(F0, STRING, Namespace.VALUE, 0))
    ));
  }

  @Test
  public void shouldNotGetColumnByNameIfWrongCase() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueColumn(ColumnName.of("F0"));

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldNotGetMetaColumnFromValue() {
    assertThat(SOME_SCHEMA.findValueColumn(ROWTIME_NAME), is(Optional.empty()));
    assertThat(SOME_SCHEMA.findValueColumn(ROWPARTITION_NAME), is(Optional.empty()));
    assertThat(SOME_SCHEMA.findValueColumn(ROWOFFSET_NAME), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetKeyColumnFromValue() {
    assertThat(SOME_SCHEMA.findValueColumn(K0), is(Optional.empty()));
  }

  @Test
  public void shouldGetMetaColumnFromValueIfAdded() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withPseudoAndKeyColsInValue(false);

    // Then:
    assertThat(schema.findValueColumn(ROWTIME_NAME),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetRowPartitionAndOffsetColumnsFromValueIfAdded() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withPseudoAndKeyColsInValue(
        false, ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

    // Then:
    assertThat(schema.findValueColumn(ROWPARTITION_NAME),
        is(not(Optional.empty())));
    assertThat(schema.findValueColumn(ROWOFFSET_NAME),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetKeyColumnFromValueIfAdded() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withPseudoAndKeyColsInValue(false);

    // Then:
    assertThat(schema.findValueColumn(K0),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetKeyColumns() {
    assertThat(SOME_SCHEMA.findColumn(K0), is(Optional.of(
        Column.of(K0, BIGINT, Namespace.KEY, 0)
    )));
  }

  @Test
  public void shouldGetValueColumns() {
    assertThat(SOME_SCHEMA.findColumn(F0), is(Optional.of(
        Column.of(F0, STRING, Namespace.VALUE, 0)
    )));
  }

  @Test
  public void shouldGetHeaderColumns() {
    assertThat(SOME_SCHEMA.findColumn(H0), is(Optional.of(
        Column.of(H0, HEADERS_TYPE, Namespace.HEADERS, 0, Optional.empty())
    )));

    assertThat(SCHEMA_WITH_EXTRACTED_HEADERS.findColumn(H0), is(Optional.of(
        Column.of(H0, BYTES, Namespace.HEADERS, 0, Optional.of("key0"))
    )));

    assertThat(SCHEMA_WITH_EXTRACTED_HEADERS.findColumn(H1), is(Optional.of(
        Column.of(H1, BYTES, Namespace.HEADERS, 1, Optional.of("key1"))
    )));
  }

  @Test
  public void shouldPreferKeyOverValueHeaderAndMetaColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        // Implicit meta ROWTIME
        .valueColumn(ROWTIME_NAME, BIGINT)
        .keyColumn(ROWTIME_NAME, BIGINT)
        .headerColumn(ROWTIME_NAME, Optional.empty())
        .build();

    // Then:
    assertThat(
        schema.findColumn(ROWTIME_NAME).map(Column::namespace),
        is(Optional.of(Namespace.KEY))
    );
  }

  @Test
  public void shouldPreferValueOverHeaderAndMetaColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .headerColumn(ROWPARTITION_NAME, Optional.empty())
        .build();

    // Then:
    assertThat(
        schema.findColumn(ROWTIME_NAME).map(Column::namespace),
        is(Optional.of(Namespace.VALUE))
    );
    assertThat(
        schema.findColumn(ROWOFFSET_NAME).map(Column::namespace),
        is(Optional.of(Namespace.VALUE))
    );
    assertThat(
        schema.findColumn(ROWPARTITION_NAME).map(Column::namespace),
        is(Optional.of(Namespace.VALUE))
    );
  }

  @Test
  public void shouldExposeKeyColumns() {
    assertThat(SOME_SCHEMA.key(), contains(
        keyColumn(K0, BIGINT)
    ));
  }

  @Test
  public void shouldExposeValueColumns() {
    assertThat(SOME_SCHEMA.value(), contains(
        valueColumn(F0, STRING),
        valueColumn(F1, BIGINT)
    ));
  }

  @Test
  public void shouldExposeHeaderColumns() {
    assertThat(SOME_SCHEMA.headers(), contains(
        headersColumn(H0, HEADERS_TYPE, Optional.empty())
    ));
  }

  @Test
  public void shouldExposeExtractedHeaderColumns() {
    assertThat(SCHEMA_WITH_EXTRACTED_HEADERS.headers(), contains(
        headersColumn(H0, BYTES, Optional.of("key0")),
        headersColumn(H1, BYTES, Optional.of("key1"))
    ));
  }

  @Test
  public void shouldExposeAllColumnsWithoutImplicits() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .keyColumn(K0, BIGINT)
        .valueColumn(F1, BIGINT)
        .headerColumn(H0, Optional.empty())
        .build();

    assertThat(schema.columns(), contains(
        valueColumn(F0, STRING),
        keyColumn(K0, BIGINT),
        valueColumn(F1, BIGINT),
        headersColumn(H0, HEADERS_TYPE, Optional.empty())
    ));
  }

  @Test
  public void shouldConvertSchemaToString() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, BIGINT)
        .keyColumn(K1, DOUBLE)
        .valueColumn(F0, BOOLEAN)
        .valueColumn(F1, INTEGER)
        .valueColumn(ColumnName.of("f2"), BIGINT)
        .valueColumn(ColumnName.of("f4"), DOUBLE)
        .valueColumn(ColumnName.of("f5"), STRING)
        .valueColumn(ColumnName.of("f6"), SqlTypes.struct()
            .field("a", BIGINT)
            .build())
        .valueColumn(ColumnName.of("f7"), SqlTypes.array(STRING))
        .valueColumn(ColumnName.of("f8"), SqlTypes.map(SqlTypes.STRING, STRING))
        .headerColumn(H0, Optional.empty())
        .build();

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "`k0` BIGINT KEY, "
            + "`k1` DOUBLE KEY, "
            + "`f0` BOOLEAN, "
            + "`f1` INTEGER, "
            + "`f2` BIGINT, "
            + "`f4` DOUBLE, "
            + "`f5` STRING, "
            + "`f6` STRUCT<`a` BIGINT>, "
            + "`f7` ARRAY<STRING>, "
            + "`f8` MAP<STRING, STRING>, "
            + "`h0` ARRAY<STRUCT<`KEY` STRING, `VALUE` BYTES>> HEADERS"
    ));
  }

  @Test
  public void shouldConvertSchemaWithExtractedHeadersToString() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, BIGINT)
        .keyColumn(K1, DOUBLE)
        .valueColumn(F0, BOOLEAN)
        .valueColumn(F1, INTEGER)
        .valueColumn(ColumnName.of("f2"), BIGINT)
        .valueColumn(ColumnName.of("f4"), DOUBLE)
        .valueColumn(ColumnName.of("f5"), STRING)
        .valueColumn(ColumnName.of("f6"), SqlTypes.struct()
            .field("a", BIGINT)
            .build())
        .valueColumn(ColumnName.of("f7"), SqlTypes.array(STRING))
        .valueColumn(ColumnName.of("f8"), SqlTypes.map(SqlTypes.STRING, STRING))
        .headerColumn(H0, Optional.of("key"))
        .build();

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "`k0` BIGINT KEY, "
            + "`k1` DOUBLE KEY, "
            + "`f0` BOOLEAN, "
            + "`f1` INTEGER, "
            + "`f2` BIGINT, "
            + "`f4` DOUBLE, "
            + "`f5` STRING, "
            + "`f6` STRUCT<`a` BIGINT>, "
            + "`f7` ARRAY<STRING>, "
            + "`f8` MAP<STRING, STRING>, "
            + "`h0` BYTES HEADER('key')"
    ));
  }

  @Test
  public void shouldSupportKeyAndHeadersInterleavedWithValueColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, BOOLEAN)
        .keyColumn(K0, BIGINT)
        .valueColumn(V0, INTEGER)
        .headerColumn(H0, Optional.of("key0"))
        .keyColumn(K1, DOUBLE)
        .headerColumn(H1, Optional.of("key1"))
        .valueColumn(V1, BOOLEAN)
        .build();

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "`f0` BOOLEAN, "
            + "`k0` BIGINT KEY, "
            + "`v0` INTEGER, "
            + "`h0` BYTES HEADER('key0'), "
            + "`k1` DOUBLE KEY, "
            + "`h1` BYTES HEADER('key1'), "
            + "`v1` BOOLEAN"
    ));
  }

  @Test
  public void shouldConvertSchemaToStringWithReservedWords() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, DOUBLE)
        .valueColumn(F0, BOOLEAN)
        .valueColumn(F1, SqlTypes.struct()
            .field("f0", BIGINT)
            .field("f1", BIGINT)
            .build())
        .build();

    final FormatOptions formatOptions =
        FormatOptions.of(word -> word.equalsIgnoreCase("f0"));

    // When:
    final String s = schema.toString(formatOptions);

    // Then:
    assertThat(s, is(
        "k0 DOUBLE KEY, "
            + "`f0` BOOLEAN, "
            + "f1 STRUCT<`f0` BIGINT, f1 BIGINT>"
    ));
  }

  @Test
  public void shouldAddMetaHeadersAndKeyColumnsToValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .headerColumn(H0, Optional.empty())
        .build();

    // When:
    final LogicalSchema result = schema.withPseudoAndKeyColsInValue(false);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .headerColumn(H0, Optional.empty())
        .valueColumn(H0, HEADERS_TYPE)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .valueColumn(K0, INTEGER)
        .valueColumn(K1, STRING)
        .build()
    ));
  }

  @Test
  public void shouldAddRowPartitionAndOffsetColumnsToValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();

    // When:
    final LogicalSchema result = schema.withPseudoAndKeyColsInValue(false, ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .valueColumn(K0, INTEGER)
        .valueColumn(K1, STRING)
        .build()
    ));
  }

  @Test
  public void shouldAddWindowedMetaAndKeyColumnsToValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();

    // When:
    final LogicalSchema result = schema
        .withPseudoAndKeyColsInValue(true);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .valueColumn(K0, INTEGER)
        .valueColumn(K1, STRING)
        .valueColumn(WINDOWSTART_NAME, BIGINT)
        .valueColumn(WINDOWEND_NAME, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldAddWindowedRowOffsetAndPartitionColumnsToValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();

    // When:
    final LogicalSchema result = schema
        .withPseudoAndKeyColsInValue(true, ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .keyColumn(K1, STRING)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .valueColumn(K0, INTEGER)
        .valueColumn(K1, STRING)
        .valueColumn(WINDOWSTART_NAME, BIGINT)
        .valueColumn(WINDOWEND_NAME, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldAddMetaHeaderAndKeyColumnsOnlyOnce() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .keyColumn(K0, INTEGER)
        .valueColumn(F1, BIGINT)
        .headerColumn(H0, Optional.of("key0"))
        .build()
        .withPseudoAndKeyColsInValue(false);

    // When:
    final LogicalSchema result = schema.withPseudoAndKeyColsInValue(false);

    // Then:
    assertThat(result, is(schema));
  }

  @Test
  public void shouldRemoveOthersWhenAddingMetasAndKeyColumns() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(K0, DOUBLE)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, DOUBLE)
        .build();

    // When:
    final LogicalSchema result = ksqlSchema.withPseudoAndKeyColsInValue(false);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .valueColumn(K0, INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldRemoveOthersWhenAddingRowPartitionAndOffsetColumns() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(K0, DOUBLE)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, DOUBLE)
        .valueColumn(ROWPARTITION_NAME, DOUBLE)
        .valueColumn(ROWOFFSET_NAME, DOUBLE)
        .build();

    // When:
    final LogicalSchema result = ksqlSchema.withPseudoAndKeyColsInValue(
        false, ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .valueColumn(K0, INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaColumnsFromValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .headerColumn(H0, Optional.empty())
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
        .withPseudoAndKeyColsInValue(false);

    // When
    final LogicalSchema result = schema.withoutPseudoAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .headerColumn(H0, Optional.empty())
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveWindowedMetaColumnsFromValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
        .withPseudoAndKeyColsInValue(true);

    // When
    final LogicalSchema result = schema.withoutPseudoAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaColumnsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .build();

    // When
    final LogicalSchema result = schema.withoutPseudoAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveRowPartitionAndOffsetColumnsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .valueColumn(ROWTIME_NAME, BIGINT)
        .valueColumn(ROWPARTITION_NAME, INTEGER)
        .valueColumn(ROWOFFSET_NAME, BIGINT)
        .build();

    // When
    final LogicalSchema result = schema.withoutPseudoAndKeyColsInValue(
        ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveKeyColumnsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, STRING)
        .valueColumn(F0, BIGINT)
        .valueColumn(K0, STRING)
        .valueColumn(F1, BIGINT)
        .build();

    // When
    final LogicalSchema result = schema.withoutPseudoAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, STRING)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveHeadersColumnsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, STRING)
        .headerColumn(H0, Optional.empty())
        .valueColumn(F0, BIGINT)
        .valueColumn(H0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build();

    // When
    final LogicalSchema result = schema.withoutPseudoAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, STRING)
        .headerColumn(H0, Optional.empty())
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveAllButKeyCols() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
        .withPseudoAndKeyColsInValue(false);

    // When
    final LogicalSchema result = schema.withKeyColsOnly();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, INTEGER)
        .valueColumn(K0, INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldMatchHeaderColumnName() {
    assertThat(SOME_SCHEMA.isHeaderColumn(H0), is(true));
    assertThat(SOME_SCHEMA.isHeaderColumn(ROWPARTITION_NAME), is(false));
    assertThat(SOME_SCHEMA.isHeaderColumn(K0), is(false));
    assertThat(SOME_SCHEMA.isHeaderColumn(F0), is(false));
  }

  @Test
  public void shouldMatchMetaColumnName() {
    assertThat(SystemColumns.isPseudoColumn(ROWTIME_NAME), is(true));
    assertThat(SOME_SCHEMA.isKeyColumn(ROWTIME_NAME), is(false));
  }

  @Test
  public void shouldMatchRowPartitionAndOffsetColumnNames() {
    assertThat(SystemColumns.isPseudoColumn(ROWPARTITION_NAME), is(true));
    assertThat(SystemColumns.isPseudoColumn(ROWOFFSET_NAME), is(true));
    assertThat(SOME_SCHEMA.isKeyColumn(ROWPARTITION_NAME), is(false));
    assertThat(SOME_SCHEMA.isKeyColumn(ROWOFFSET_NAME), is(false));
  }

  @Test
  public void shouldMatchKeyColumnName() {
    assertThat(SystemColumns.isPseudoColumn(K0), is(false));
    assertThat(SOME_SCHEMA.isKeyColumn(K0), is(true));
  }

  @Test
  public void shouldNotMatchValueColumnsAsBeingMetaOrKeyColumns() {
    SOME_SCHEMA.value().forEach(column ->
    {
      assertThat(SystemColumns.isPseudoColumn(column.name()), is(false));
      assertThat(SOME_SCHEMA.isKeyColumn(column.name()), is(false));
    });
  }

  @Test
  public void shouldNotMatchRandomColumnNameAsBeingMetaOrKeyColumns() {
    assertThat(SystemColumns.isPseudoColumn(ColumnName.of("well_this_ain't_in_the_schema")), is(false));
    assertThat(SOME_SCHEMA.isKeyColumn(ColumnName.of("well_this_ain't_in_the_schema")), is(false));
  }

  @Test
  public void shouldThrowOnDuplicateKeyColumnName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .keyColumn(KEY, BIGINT);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.keyColumn(KEY, BIGINT)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Duplicate key columns found in schema: `key` BIGINT"));
  }

  @Test
  public void shouldThrowOnDuplicateValueColumnName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .valueColumn(VALUE, BIGINT);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.valueColumn(VALUE, BIGINT)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Duplicate value columns found in schema: `value` BIGINT"));
  }

  @Test
  public void shouldThrowOnDuplicateHeaderColumnName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .headerColumn(H0, Optional.of("key0"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.headerColumn(H0, Optional.of("key1"))
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Duplicate headers columns found in schema: `h0` BYTES"));
  }

  @Test
  public void shouldThrowOnMultipleHeadersColumns() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .headerColumn(H0, Optional.empty());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.headerColumn(F0, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Schema already contains a HEADERS column."));
  }

  @Test
  public void shouldThrowOnRepeatedHeaderKey() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .headerColumn(H0, Optional.of("key"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.headerColumn(F0, Optional.of("key"))
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Schema already contains a HEADER('key') column."));
  }

  @Test
  public void shouldSupportCopyingColumnsFromOtherSchemas() {
    // When:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumns(SOME_SCHEMA.value())
        .valueColumns(SOME_SCHEMA.key())
        .build();

    // Then:
    assertThat(schema.columns(), contains(
        keyColumn(F0, STRING),
        keyColumn(F1, BIGINT),
        valueColumn(K0, BIGINT)
    ));
  }

  @Test
  public void shouldMatchAnyValueSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .keyColumn(K0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build();

    // Then:
    assertThat(schema.valueContainsAny(ImmutableSet.of(F0)), is(true));
    assertThat(schema.valueContainsAny(ImmutableSet.of(V0, F0, F1, V1)), is(true));
  }

  @Test
  public void shouldOnlyMatchValueSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .keyColumn(K0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build();

    // Then:
    assertThat(schema.valueContainsAny(
        ImmutableSet.of(K0, V0, ROWTIME_NAME, ROWPARTITION_NAME, ROWOFFSET_NAME)), is(false));
  }

  @Test
  public void shouldDuplicateViaAsBuilder() {
    // Given:
    final Builder builder = SOME_SCHEMA.asBuilder();

    // When:
    final LogicalSchema clone = builder.build();

    // Then:
    assertThat(clone, is(SOME_SCHEMA));
  }

  @Test
  public void shouldAddColumnsViaAsBuilder() {
    // Given:
    final Builder builder = SOME_SCHEMA.asBuilder();

    // When:
    final LogicalSchema clone = builder
        .keyColumn(K1, INTEGER)
        .valueColumn(V1, STRING)
        .build();

    // Then:
    assertThat(clone, is(LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .keyColumn(K0, BIGINT)
        .valueColumn(F1, BIGINT)
        .headerColumn(H0, Optional.empty())
        .keyColumn(K1, INTEGER)
        .valueColumn(V1, STRING)
        .build()));
  }

  @Test
  public void shouldDetectDuplicateKeysViaAsBuilder() {
    // Given:
    final Builder builder = SOME_SCHEMA.asBuilder();

    // When:
    assertThrows(
        KsqlException.class,
        () -> builder.keyColumn(K0, STRING)
    );
  }

  @Test
  public void shouldDetectDuplicateValuesViaAsBuilder() {
    // Given:
    final Builder builder = SOME_SCHEMA.asBuilder();

    // When:
    assertThrows(
        KsqlException.class,
        () -> builder.valueColumn(F0, STRING)
    );
  }

  private static org.apache.kafka.connect.data.Field connectField(
      final String fieldName,
      final int index,
      final Schema schema
  ) {
    return new org.apache.kafka.connect.data.Field(fieldName, index, schema);
  }

  @Test
  public void shouldSchemaNoCompatibleWithDifferentSizes() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();
    final LogicalSchema otherSchema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .valueColumn(V1, BIGINT)
        .build();

    // Then:
    assertThat(schema.compatibleSchema(otherSchema), is(false));
  }

  @Test
  public void shouldSchemaNoCompatibleOnDifferentColumnName() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();
    final LogicalSchema otherSchema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(V1, BIGINT)
        .build();

    // Then:
    assertThat(schema.compatibleSchema(otherSchema), is(false));
  }

  @Test
  public void shouldSchemaNoCompatibleWhenCannotCastType() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();
    final LogicalSchema otherSchema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, INTEGER)
        .build();

    // Then:
    assertThat(schema.compatibleSchema(otherSchema), is(false));
  }

  @Test
  public void shouldSchemaCompatibleWithImplicitlyCastType() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, SqlDecimal.of(5, 2))
        .build();
    final LogicalSchema otherSchema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, SqlDecimal.of(6, 3))
        .build();

    // Then:
    assertThat(schema.compatibleSchema(otherSchema), is(true));
  }
}
