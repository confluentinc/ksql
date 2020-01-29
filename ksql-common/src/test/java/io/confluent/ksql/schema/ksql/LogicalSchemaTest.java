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

import static io.confluent.ksql.schema.ksql.ColumnMatchers.keyColumn;
import static io.confluent.ksql.schema.ksql.ColumnMatchers.metaColumn;
import static io.confluent.ksql.schema.ksql.ColumnMatchers.valueColumn;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BOOLEAN;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DOUBLE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.INTEGER;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static io.confluent.ksql.util.SchemaUtil.ROWKEY_NAME;
import static io.confluent.ksql.util.SchemaUtil.ROWTIME_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings({"UnstableApiUsage","unchecked"})
public class LogicalSchemaTest {

  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");
  private static final ColumnName KEY = ColumnName.of("key");
  private static final ColumnName V0 = ColumnName.of("v0");
  private static final ColumnName V1 = ColumnName.of("v1");
  private static final ColumnName F0 = ColumnName.of("f0");
  private static final ColumnName F1 = ColumnName.of("f1");
  private static final ColumnName VALUE = ColumnName.of("value");

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueColumn(F0, STRING)
      .keyColumn(K0, BIGINT)
      .valueColumn(F1, BIGINT)
      .build();

  // Constants used to represent column counts:
  private static final int ROWTIME = 1;
  private static final int ROWKEY = 1;
  private static final int WINDOW_BOUNDS = 2;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
                .valueColumn(F0, STRING)
                .valueColumn(F1, BIGINT)
                .build(),

            LogicalSchema.builder()
                .keyColumn(ROWKEY_NAME, STRING)
                .valueColumn(F0, STRING)
                .valueColumn(F1, BIGINT)
                .build()
        )
        .addEqualityGroup(
            aSchema,
            aSchema
                .withMetaAndKeyColsInValue(false)
                .withoutMetaAndKeyColsInValue(),

            aSchema
                .withMetaAndKeyColsInValue(true)
                .withoutMetaAndKeyColsInValue()
        )
        .addEqualityGroup(
            aSchema.withMetaAndKeyColsInValue(true)
        )
        .addEqualityGroup(
            aSchema.withMetaAndKeyColsInValue(false)
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
        Column.of(ROWTIME_NAME, BIGINT, Namespace.META, 0),
        Column.of(F0, STRING, Namespace.VALUE, 0),
        Column.of(K0, BIGINT, Namespace.KEY, 0),
        Column.of(F1, BIGINT, Namespace.VALUE, 1),
        Column.of(K1, BOOLEAN, Namespace.KEY, 1)
    ));
  }

  @Test
  public void shouldGetColumnByName() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueColumn(ColumnRef.of(F0));

    // Then:
    assertThat(result, is(Optional.of(
        Column.of(F0, STRING, Namespace.VALUE, 0))
    ));
  }

  @Test
  public void shouldNotGetColumnByNameIfWrongCase() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueColumn(ColumnRef.of(ColumnName.of("F0")));

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldNotGetMetaColumnFromValue() {
    assertThat(SOME_SCHEMA.findValueColumn(ColumnRef.of(ROWTIME_NAME)), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetKeyColumnFromValue() {
    assertThat(SOME_SCHEMA.findValueColumn(ColumnRef.of(K0)), is(Optional.empty()));
  }

  @Test
  public void shouldGetMetaColumnFromValueIfAdded() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withMetaAndKeyColsInValue(false);

    // Then:
    assertThat(schema.findValueColumn(ColumnRef.of(ROWTIME_NAME)),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetKeyColumnFromValueIfAdded() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withMetaAndKeyColsInValue(false);

    // Then:
    assertThat(schema.findValueColumn(ColumnRef.of(K0)),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetMetaFields() {
    assertThat(SOME_SCHEMA.findColumn(ColumnRef.of(ROWTIME_NAME)), is(Optional.of(
        Column.of(ROWTIME_NAME, BIGINT, Namespace.META, 0)
    )));
  }

  @Test
  public void shouldGetKeyColumns() {
    assertThat(SOME_SCHEMA.findColumn(ColumnRef.of(K0)), is(Optional.of(
        Column.of(K0, BIGINT, Namespace.KEY, 0)
    )));
  }

  @Test
  public void shouldGetValueColumns() {
    assertThat(SOME_SCHEMA.findColumn(ColumnRef.of(F0)), is(Optional.of(
        Column.of(F0, STRING, Namespace.VALUE, 0)
    )));
  }

  @Test
  public void shouldPreferKeyOverValueAndMetaColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        // Implicit meta ROWTIME
        .valueColumn(ROWTIME_NAME, BIGINT)
        .keyColumn(ROWTIME_NAME, BIGINT)
        .build();

    // Then:
    assertThat(
        schema.findColumn(ColumnRef.of(ROWTIME_NAME)).map(Column::namespace),
        is(Optional.of(Namespace.KEY))
    );
  }

  @Test
  public void shouldPreferValueOverMetaColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        // Implicit meta ROWTIME
        .valueColumn(ROWTIME_NAME, BIGINT)
        .build();

    // Then:
    assertThat(
        schema.findColumn(ColumnRef.of(ROWTIME_NAME)).map(Column::namespace),
        is(Optional.of(Namespace.VALUE))
    );
  }

  @Test
  public void shouldExposeMetaColumns() {
    assertThat(SOME_SCHEMA.metadata(), contains(
        metaColumn(ROWTIME_NAME, BIGINT)
    ));
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
  public void shouldExposeAllColumns() {
    assertThat(SOME_SCHEMA.columns(), contains(
        metaColumn(ROWTIME_NAME, BIGINT),
        valueColumn(F0, STRING),
        keyColumn(K0, BIGINT),
        valueColumn(F1, BIGINT)
    ));
  }

  @Test
  public void shouldExposeAllColumnsWithoutImplicits() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .valueColumn(F0, STRING)
        .keyColumn(K0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build();

    assertThat(schema.columns(), contains(
        valueColumn(F0, STRING),
        keyColumn(K0, BIGINT),
        valueColumn(F1, BIGINT)
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
        .valueColumn(ColumnName.of("f8"), SqlTypes.map(STRING))
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
            + "`f8` MAP<STRING, STRING>"
    ));
  }

  @Test
  public void shouldSupportKeyInterleavedWithValueColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, BOOLEAN)
        .keyColumn(K0, BIGINT)
        .valueColumn(V0, INTEGER)
        .keyColumn(K1, DOUBLE)
        .valueColumn(V1, BOOLEAN)
        .build();

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "`f0` BOOLEAN, "
            + "`k0` BIGINT KEY, "
            + "`v0` INTEGER, "
            + "`k1` DOUBLE KEY, "
            + "`v1` BOOLEAN"
    ));
  }

  @Test
  public void shouldConvertSchemaToStringWithReservedWords() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
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
        "ROWKEY STRING KEY, "
            + "`f0` BOOLEAN, "
            + "f1 STRUCT<`f0` BIGINT, f1 BIGINT>"
    ));
  }

  @Test
  public void shouldAddMetaAndKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyColsInValue(false);

    // Then:
    assertThat(result.value(), hasSize(schema.value().size() + ROWTIME + ROWKEY));
    assertThat(result.value().get(0).name(), is(SchemaUtil.ROWTIME_NAME));
    assertThat(result.value().get(0).type(), is(BIGINT));
    assertThat(result.value().get(1).name(), is(SchemaUtil.ROWKEY_NAME));
    assertThat(result.value().get(1).type(), is(STRING));
    assertThat(result.value().get(2).name(), is(F0));
  }

  @Test
  public void shouldAddWindowedMetaAndKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, DOUBLE)
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build();

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyColsInValue(true);

    // Then:
    assertThat(result.value(), hasSize(schema.value().size() + ROWTIME + WINDOW_BOUNDS + ROWKEY));
    assertThat(result.value().get(0).name(), is(SchemaUtil.ROWTIME_NAME));
    assertThat(result.value().get(0).type(), is(BIGINT));
    assertThat(result.value().get(1).name(), is(SchemaUtil.WINDOWSTART_NAME));
    assertThat(result.value().get(1).type(), is(BIGINT));
    assertThat(result.value().get(2).name(), is(SchemaUtil.WINDOWEND_NAME));
    assertThat(result.value().get(2).type(), is(BIGINT));
    assertThat(result.value().get(3).name(), is(SchemaUtil.ROWKEY_NAME));
    assertThat(result.value().get(3).type(), is(DOUBLE));
    assertThat(result.value().get(4).name(), is(F0));
  }

  @Test
  public void shouldAddMetaAndKeyColumnsOnlyOnce() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, STRING)
        .valueColumn(F1, BIGINT)
        .build()
        .withMetaAndKeyColsInValue(false);

    // When:
    final LogicalSchema result = schema.withMetaAndKeyColsInValue(false);

    // Then:
    assertThat(result, is(schema));
  }

  @Test
  public void shouldRemoveOthersWhenAddingMetasAndKeyColumns() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.builder()
        .valueColumn(F0, BIGINT)
        .valueColumn(SchemaUtil.ROWKEY_NAME, DOUBLE)
        .valueColumn(F1, BIGINT)
        .valueColumn(SchemaUtil.ROWTIME_NAME, DOUBLE)
        .build();

    // When:
    final LogicalSchema result = ksqlSchema.withMetaAndKeyColsInValue(false);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueColumn(SchemaUtil.ROWTIME_NAME, BIGINT)
        .valueColumn(SchemaUtil.ROWKEY_NAME, STRING)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaColumnsFromValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
        .withMetaAndKeyColsInValue(false);

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

 @Test
  public void shouldRemoveWindowedMetaColumnsFromValue() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
        .withMetaAndKeyColsInValue(true);

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaColumnsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(F0, BIGINT)
        .valueColumn(SchemaUtil.ROWKEY_NAME, STRING)
        .valueColumn(F1, BIGINT)
        .valueColumn(SchemaUtil.ROWTIME_NAME, BIGINT)
        .build();

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldMatchMetaColumnName() {
    assertThat(SOME_SCHEMA.isMetaColumn(ROWTIME_NAME), is(true));
    assertThat(SOME_SCHEMA.isKeyColumn(ROWTIME_NAME), is(false));
  }

  @Test
  public void shouldMatchKeyColumnName() {
    assertThat(SOME_SCHEMA.isMetaColumn(K0), is(false));
    assertThat(SOME_SCHEMA.isKeyColumn(K0), is(true));
  }

  @Test
  public void shouldNotMatchValueColumnsAsBeingMetaOrKeyColumns() {
    SOME_SCHEMA.value().forEach(column ->
    {
      assertThat(SOME_SCHEMA.isMetaColumn(column.name()), is(false));
      assertThat(SOME_SCHEMA.isKeyColumn(column.name()), is(false));
    });
  }

  @Test
  public void shouldNotMatchRandomColumnNameAsBeingMetaOrKeyColumns() {
    assertThat(SOME_SCHEMA.isMetaColumn(ColumnName.of("well_this_ain't_in_the_schema")), is(false));
    assertThat(SOME_SCHEMA.isKeyColumn(ColumnName.of("well_this_ain't_in_the_schema")), is(false));
  }

  @Test
  public void shouldThrowOnDuplicateKeyColumnName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .keyColumn(KEY, BIGINT);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate keys found in schema: `key` BIGINT");

    // When:
    builder.keyColumn(KEY, BIGINT);
  }

  @Test
  public void shouldThrowOnDuplicateValueColumnName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .valueColumn(VALUE, BIGINT);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate values found in schema: `value` BIGINT");

    // When:
    builder.valueColumn(VALUE, BIGINT);
  }

  @Test
  public void shouldGetKeyConnectSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(F0, DOUBLE)
        .valueColumn(F0, BIGINT)
        .build();

    // When:
    final ConnectSchema result = schema.keyConnectSchema();

    // Then:
    final List<org.apache.kafka.connect.data.Field> fields = result.fields();
    assertThat(fields, contains(
        connectField("f0", 0, Schema.OPTIONAL_FLOAT64_SCHEMA)
    ));
  }

  @Test
  public void shouldGetValueConnectSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(F0, STRING)
        .valueColumn(F0, BIGINT)
        .valueColumn(F1, STRING)
        .build();

    // When:
    final ConnectSchema result = schema.valueConnectSchema();

    // Then:
    final List<org.apache.kafka.connect.data.Field> fields = result.fields();
    assertThat(fields, contains(
        connectField("f0", 0, Schema.OPTIONAL_INT64_SCHEMA),
        connectField("f1", 1, Schema.OPTIONAL_STRING_SCHEMA)
    ));
  }

  @Test
  public void shouldBuildSchemaWithNoImplicitColumns() {
    // When:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .valueColumn(F0, BIGINT)
        .build();

    // Then:
    assertThat(schema.metadata(), is(empty()));
    assertThat(schema.key(), is(empty()));
    assertThat(schema.value(), contains(
        valueColumn(F0, BIGINT)
    ));
  }

  @Test
  public void shouldSupportCopyingColumnsFromOtherSchemas() {
    // When:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .keyColumns(SOME_SCHEMA.value())
        .valueColumns(SOME_SCHEMA.metadata())
        .valueColumns(SOME_SCHEMA.key())
        .build();

    // Then:
    assertThat(schema.columns(), contains(
        keyColumn(F0, STRING),
        keyColumn(F1, BIGINT),
        valueColumn(ROWTIME_NAME, BIGINT),
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
    assertThat(schema.valueContainsAny(ImmutableSet.of(K0, V0, ROWTIME_NAME)), is(false));
  }

  private static org.apache.kafka.connect.data.Field connectField(
      final String fieldName,
      final int index,
      final Schema schema
  ) {
    return new org.apache.kafka.connect.data.Field(fieldName, index, schema);
  }
}
