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

import static io.confluent.ksql.util.SchemaUtil.ROWKEY_NAME;
import static io.confluent.ksql.util.SchemaUtil.ROWTIME_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LogicalSchemaTest {

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .keyColumn("k0", SqlTypes.BIGINT)
      .valueColumn("f0", SqlTypes.STRING)
      .valueColumn("f1", SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema ALIASED_SCHEMA = SOME_SCHEMA.withAlias("bob");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEqualsProperly() {

    final LogicalSchema aSchema = LogicalSchema.builder()
        .keyColumn("key", SqlTypes.BIGINT)
        .valueColumn("value", SqlTypes.STRING)
        .build();

    new EqualsTester()
        .addEqualityGroup(
            LogicalSchema.builder()
                .keyColumn("k0", SqlTypes.BIGINT)
                .valueColumn("v0", SqlTypes.STRING)
                .build(),

            LogicalSchema.builder()
                .keyColumn("k0", SqlTypes.BIGINT)
                .valueColumn("v0", SqlTypes.STRING)
                .build()
        )
        .addEqualityGroup(
            LogicalSchema.builder()
                .valueColumn("f0", SqlTypes.STRING)
                .valueColumn("f1", SqlTypes.BIGINT)
                .build(),

            LogicalSchema.builder()
                .keyColumn(ROWKEY_NAME, SqlTypes.STRING)
                .valueColumn("f0", SqlTypes.STRING)
                .valueColumn("f1", SqlTypes.BIGINT)
                .build()
        )
        .addEqualityGroup(
            aSchema,

            aSchema
                .withMetaAndKeyColsInValue()
                .withoutMetaAndKeyColsInValue(),

            aSchema
                .withAlias("bob")
                .withoutAlias()
        )
        .addEqualityGroup(
            aSchema.withAlias("bob")
        )
        .addEqualityGroup(
            aSchema.withMetaAndKeyColsInValue()
        )
        .testEquals();
  }

  @Test
  public void shouldBuildSchemaWithAlias() {
    // When:
    final LogicalSchema result = SOME_SCHEMA.withAlias("bob");

    // Then:
    assertThat(result, is(ALIASED_SCHEMA));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
  @Test(expected = IllegalStateException.class)
  public void shouldThrowIfAlreadyAliased() {
    // Given:
    final LogicalSchema aliased = SOME_SCHEMA.withAlias("bob");

    // When:
    aliased.withAlias("bob");
  }

  @Test
  public void shouldOnlyAddAliasToTopLevelFields() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn("k0", SqlTypes.BIGINT)
        .valueColumn("f0", SqlTypes.STRING)
        .valueColumn("f1", SqlTypes.struct()
            .field("nested", SqlTypes.BIGINT)
            .build())
        .build();

    // When:
    final LogicalSchema result = schema.withAlias("bob");

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(Column.of("bob", "k0", SqlTypes.BIGINT))
        .valueColumn(Column.of("bob", "f0", SqlTypes.STRING))
        .valueColumn(Column.of("bob", "f1", SqlTypes.struct()
            .field("nested", SqlTypes.BIGINT)
            .build()))
        .build()));
  }

  @Test
  public void shouldBuildSchemaWithoutAlias() {
    // When:
    final LogicalSchema result = ALIASED_SCHEMA.withoutAlias();

    // Then:
    assertThat(result, is(SOME_SCHEMA));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
  @Test(expected = IllegalStateException.class)
  public void shouldThrowIfNotAliased() {
    // When:
    SOME_SCHEMA.withoutAlias();
  }

  @Test
  public void shouldOnlyRemoveAliasFromTopLevelFields() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn("k0", SqlTypes.INTEGER)
        .valueColumn("f0", SqlTypes.STRING)
        .valueColumn("f1", SqlTypes.struct()
            .field("bob.nested", SqlTypes.BIGINT)
            .build())
        .build().withAlias("bob");

    // When:
    final LogicalSchema result = schema.withoutAlias();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn("k0", SqlTypes.INTEGER)
        .valueColumn("f0", SqlTypes.STRING)
        .valueColumn("f1", SqlTypes.struct()
            .field("bob.nested", SqlTypes.BIGINT)
            .build())
        .build()));
  }

  @Test
  public void shouldGetFieldByName() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueColumn("f0");

    // Then:
    assertThat(result, is(Optional.of(Column.of("f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldGetFieldByAliasedName() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueColumn("SomeAlias.f0");

    // Then:
    assertThat(result, is(Optional.of(Column.of("f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldNotGetFieldByNameIfWrongCase() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueColumn("F0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldNotGetFieldByNameIfFieldIsAliasedAndNameIsNot() {
    // When:
    final Optional<Column> result = ALIASED_SCHEMA.findValueColumn("f0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldGetFieldByNameIfBothFieldAndNameAreAliased() {
    // When:
    final Optional<Column> result = ALIASED_SCHEMA.findValueColumn("bob.f0");

    // Then:
    assertThat(result, is(Optional.of(Column.of("bob", "f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldNotGetMetaFieldFromValue() {
    assertThat(SOME_SCHEMA.findValueColumn("ROWTIME"), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetKeyFieldFromValue() {
    assertThat(SOME_SCHEMA.findValueColumn("k0"), is(Optional.empty()));
  }

  @Test
  public void shouldGetMetaFieldFromValueIfAdded() {
    assertThat(SOME_SCHEMA.withMetaAndKeyColsInValue().findValueColumn("ROWTIME"),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetKeyFieldFromValueIfAdded() {
    assertThat(SOME_SCHEMA.withMetaAndKeyColsInValue().findValueColumn("k0"),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetMetaFields() {
    assertThat(SOME_SCHEMA.findColumn("ROWTIME"), is(Optional.of(
        Column.of("ROWTIME", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldGetKeyFields() {
    assertThat(SOME_SCHEMA.findColumn("k0"), is(Optional.of(
        Column.of("k0", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldGetValueFields() {
    assertThat(SOME_SCHEMA.findColumn("f0"), is(Optional.of(
        Column.of("f0", SqlTypes.STRING)
    )));
  }

  @Test
  public void shouldGetFieldIndex() {
    assertThat(SOME_SCHEMA.valueColumnIndex("f0"), is(OptionalInt.of(0)));
    assertThat(SOME_SCHEMA.valueColumnIndex("f1"), is(OptionalInt.of(1)));
  }

  @Test
  public void shouldReturnMinusOneForIndexIfFieldNotFound() {
    assertThat(SOME_SCHEMA.valueColumnIndex("wontfindme"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldNotFindFieldIfDifferentCase() {
    assertThat(SOME_SCHEMA.valueColumnIndex("F0"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldGetAliasedFieldIndex() {
    assertThat(ALIASED_SCHEMA.valueColumnIndex("bob.f1"), is(OptionalInt.of(1)));
  }

  @Test
  public void shouldNotFindUnaliasedFieldIndexInAliasedSchema() {
    assertThat(ALIASED_SCHEMA.valueColumnIndex("f1"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldNotFindAliasedFieldIndexInUnaliasedSchema() {
    assertThat(SOME_SCHEMA.valueColumnIndex("bob.f1"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldExposeMetaFields() {
    assertThat(SOME_SCHEMA.metadata(), is(ImmutableList.of(
        Column.of(ROWTIME_NAME, SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeAliasedMetaFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("fred");

    // When:
    final List<Column> fields = schema.metadata();

    // Then:
    assertThat(fields, is(ImmutableList.of(
        Column.of("fred", ROWTIME_NAME, SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeKeyFields() {
    assertThat(SOME_SCHEMA.key(), is(ImmutableList.of(
        Column.of("k0", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeAliasedKeyFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("fred");

    // When:
    final List<Column> fields = schema.key();

    // Then:
    assertThat(fields, is(ImmutableList.of(
        Column.of("fred", "k0", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeValueFields() {
    assertThat(SOME_SCHEMA.value(), contains(
        Column.of("f0", SqlTypes.STRING),
        Column.of("f1", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldExposeAliasedValueFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("bob");

    // When:
    final List<Column> fields = schema.value();

    // Then:
    assertThat(fields, contains(
        Column.of("bob", "f0", SqlTypes.STRING),
        Column.of("bob", "f1", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldExposeAllFields() {
    assertThat(SOME_SCHEMA.columns(), is(ImmutableList.of(
        Column.of(ROWTIME_NAME, SqlTypes.BIGINT),
        Column.of("k0", SqlTypes.BIGINT),
        Column.of("f0", SqlTypes.STRING),
        Column.of("f1", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeAliasedAllFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("bob");

    // When:
    final List<Column> fields = schema.columns();

    // Then:
    assertThat(fields, is(ImmutableList.of(
        Column.of("bob", ROWTIME_NAME, SqlTypes.BIGINT),
        Column.of("bob", "k0", SqlTypes.BIGINT),
        Column.of("bob", "f0", SqlTypes.STRING),
        Column.of("bob", "f1", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldConvertSchemaToString() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BOOLEAN)
        .valueColumn("f1", SqlTypes.INTEGER)
        .valueColumn("f2", SqlTypes.BIGINT)
        .valueColumn("f4", SqlTypes.DOUBLE)
        .valueColumn("f5", SqlTypes.STRING)
        .valueColumn("f6", SqlTypes.struct()
            .field("a", SqlTypes.BIGINT)
            .build())
        .valueColumn("f7", SqlTypes.array(SqlTypes.STRING))
        .valueColumn("f8", SqlTypes.map(SqlTypes.STRING))
        .keyColumn("k0", SqlTypes.BIGINT)
        .keyColumn("k1", SqlTypes.DOUBLE)
        .build();

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "["
            + "`k0` BIGINT KEY, "
            + "`k1` DOUBLE KEY, "
            + "`f0` BOOLEAN, "
            + "`f1` INTEGER, "
            + "`f2` BIGINT, "
            + "`f4` DOUBLE, "
            + "`f5` STRING, "
            + "`f6` STRUCT<`a` BIGINT>, "
            + "`f7` ARRAY<STRING>, "
            + "`f8` MAP<STRING, STRING>"
            + "]"));
  }

  @Test
  public void shouldConvertSchemaToStringWithReservedWords() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BOOLEAN)
        .valueColumn("f1", SqlTypes.struct()
            .field("f0", SqlTypes.BIGINT)
            .field("f1", SqlTypes.BIGINT)
            .build())
        .build();

    final FormatOptions formatOptions =
        FormatOptions.of(word -> word.equalsIgnoreCase("f0"));

    // When:
    final String s = schema.toString(formatOptions);

    // Then:
    assertThat(s, is(
        "["
            + "ROWKEY STRING KEY, "
            + "`f0` BOOLEAN, "
            + "f1 STRUCT<`f0` BIGINT, f1 BIGINT>"
            + "]"));
  }

  @Test
  public void shouldConvertAliasedSchemaToString() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BOOLEAN)
        .build()
        .withAlias("t");

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "["
            + "`t`.`ROWKEY` STRING KEY, "
            + "`t`.`f0` BOOLEAN"
            + "]"));
  }

  @Test
  public void shouldAddMetaAndKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.STRING)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build();

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyColsInValue();

    // Then:
    assertThat(result.value(), hasSize(schema.value().size() + 2));
    assertThat(result.value().get(0).name(), is(SchemaUtil.ROWTIME_NAME));
    assertThat(result.value().get(0).type(), is(SqlTypes.BIGINT));
    assertThat(result.value().get(1).name(), is(SchemaUtil.ROWKEY_NAME));
    assertThat(result.value().get(1).type(), is(SqlTypes.STRING));
  }

  @Test
  public void shouldAddMetaAndKeyColumnsWhenAliased() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.STRING)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build()
        .withAlias("bob");

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyColsInValue();

    // Then:
    assertThat(result.value(), hasSize(schema.value().size() + 2));
    assertThat(result.value().get(0).fullName(), is("bob." + SchemaUtil.ROWTIME_NAME));
    assertThat(result.value().get(0).type(), is(SqlTypes.BIGINT));
    assertThat(result.value().get(1).fullName(), is("bob." + SchemaUtil.ROWKEY_NAME));
    assertThat(result.value().get(1).type(), is(SqlTypes.STRING));
  }

  @Test
  public void shouldAddMetaAndKeyColumnsOnlyOnce() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.STRING)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build()
        .withMetaAndKeyColsInValue();

    // When:
    final LogicalSchema result = schema.withMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(schema));
  }

  @Test
  public void shouldRemoveOthersWhenAddingMetasAndKeyFields() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BIGINT)
        .valueColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.DOUBLE)
        .valueColumn("f1", SqlTypes.BIGINT)
        .valueColumn(SchemaUtil.ROWTIME_NAME, SqlTypes.DOUBLE)
        .build();

    // When:
    final LogicalSchema result = ksqlSchema.withMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueColumn(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn("f0", SqlTypes.BIGINT)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaFields() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BIGINT)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build()
        .withMetaAndKeyColsInValue();

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BIGINT)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaFieldsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BIGINT)
        .valueColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn("f1", SqlTypes.BIGINT)
        .valueColumn(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .build();

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BIGINT)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaFieldsEvenIfAliased() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("f0", SqlTypes.BIGINT)
        .valueColumn("f1", SqlTypes.BIGINT)
        .build()
        .withMetaAndKeyColsInValue()
        .withAlias("bob");

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyColsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(Column.of("bob", "ROWKEY", SqlTypes.STRING))
        .valueColumn(Column.of("bob", "f0", SqlTypes.BIGINT))
        .valueColumn(Column.of("bob", "f1", SqlTypes.BIGINT))
        .build()
    ));
  }

  @Test
  public void shouldMatchMetaFieldName() {
    assertThat(SOME_SCHEMA.isMetaColumn(ROWTIME_NAME), is(true));
    assertThat(SOME_SCHEMA.isKeyColumn(ROWTIME_NAME), is(false));
  }

  @Test
  public void shouldMatchKeyFieldName() {
    assertThat(SOME_SCHEMA.isMetaColumn("k0"), is(false));
    assertThat(SOME_SCHEMA.isKeyColumn("k0"), is(true));
  }

  @Test
  public void shouldNotMatchValueFieldsAsBeingMetaOrKeyFields() {
    SOME_SCHEMA.value().forEach(field ->
    {
      assertThat(SOME_SCHEMA.isMetaColumn(field.name()), is(false));
      assertThat(SOME_SCHEMA.isKeyColumn(field.name()), is(false));
    });
  }

  @Test
  public void shouldNotMatchRandomFieldNameAsBeingMetaOrKeyFields() {
    assertThat(SOME_SCHEMA.isMetaColumn("well_this_ain't_in_the_schema"), is(false));
    assertThat(SOME_SCHEMA.isKeyColumn("well_this_ain't_in_the_schema"), is(false));
  }

  @Test
  public void shouldThrowOnDuplicateKeyFieldName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .keyColumn("fieldName", SqlTypes.BIGINT);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate keys found in schema: `fieldName` BIGINT");

    // When:
    builder.keyColumn("fieldName", SqlTypes.BIGINT);
  }

  @Test
  public void shouldThrowOnDuplicateValueFieldName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .valueColumn("fieldName", SqlTypes.BIGINT);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate values found in schema: `fieldName` BIGINT");

    // When:
    builder.valueColumn("fieldName", SqlTypes.BIGINT);
  }

  @Test
  public void shouldAllowKeyFieldsWithSameNameButDifferentSource() {
    // Given:
    final Builder builder = LogicalSchema.builder();

    // When:
    builder
        .keyColumn("fieldName", SqlTypes.BIGINT)
        .keyColumn(Column.of("source", "fieldName", SqlTypes.BIGINT))
        .keyColumn(Column.of("diff", "fieldName", SqlTypes.BIGINT));

    // Then:
    assertThat(builder.build().key(), contains(
        Column.of("fieldName", SqlTypes.BIGINT),
        Column.of("source", "fieldName", SqlTypes.BIGINT),
        Column.of("diff", "fieldName", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldAllowValueFieldsWithSameNameButDifferentSource() {
    // Given:
    final Builder builder = LogicalSchema.builder();

    // When:
    builder
        .valueColumn("fieldName", SqlTypes.BIGINT)
        .valueColumn(Column.of("source", "fieldName", SqlTypes.BIGINT))
        .valueColumn(Column.of("diff", "fieldName", SqlTypes.BIGINT));

    // Then:
    assertThat(builder.build().value(), contains(
        Column.of("fieldName", SqlTypes.BIGINT),
        Column.of("source", "fieldName", SqlTypes.BIGINT),
        Column.of("diff", "fieldName", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldGetKeyConnectSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn("fieldName", SqlTypes.DOUBLE)
        .keyColumn(Column.of("source", "fieldName", SqlTypes.BOOLEAN))
        .keyColumn(Column.of("diff", "fieldName", SqlTypes.STRING))
        .valueColumn("fieldName", SqlTypes.BIGINT)
        .build();

    // When:
    final ConnectSchema result = schema.keyConnectSchema();

    // Then:
    final List<org.apache.kafka.connect.data.Field> fields = result.fields();
    assertThat(fields, contains(
        connectField("fieldName", 0, Schema.OPTIONAL_FLOAT64_SCHEMA),
        connectField("source.fieldName", 1, Schema.OPTIONAL_BOOLEAN_SCHEMA),
        connectField("diff.fieldName", 2, Schema.OPTIONAL_STRING_SCHEMA)
    ));
  }

  @Test
  public void shouldGetValueConnectSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn("fieldName", SqlTypes.STRING)
        .valueColumn("fieldName", SqlTypes.BIGINT)
        .valueColumn(Column.of("source", "fieldName", SqlTypes.INTEGER))
        .valueColumn(Column.of("diff", "fieldName", SqlTypes.STRING))
        .build();

    // When:
    final ConnectSchema result = schema.valueConnectSchema();

    // Then:
    final List<org.apache.kafka.connect.data.Field> fields = result.fields();
    assertThat(fields, contains(
        connectField("fieldName", 0, Schema.OPTIONAL_INT64_SCHEMA),
        connectField("source.fieldName", 1, Schema.OPTIONAL_INT32_SCHEMA),
        connectField("diff.fieldName", 2, Schema.OPTIONAL_STRING_SCHEMA)
    ));
  }

  private static org.apache.kafka.connect.data.Field connectField(
      final String fieldName,
      final int index,
      final Schema schema
  ) {
    return new org.apache.kafka.connect.data.Field(fieldName, index, schema);
  }
}
