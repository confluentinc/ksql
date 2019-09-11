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
      .keyField("k0", SqlTypes.BIGINT)
      .valueField("f0", SqlTypes.STRING)
      .valueField("f1", SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema ALIASED_SCHEMA = SOME_SCHEMA.withAlias("bob");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEqualsProperly() {

    final LogicalSchema aSchema = LogicalSchema.builder()
        .keyField("key", SqlTypes.BIGINT)
        .valueField("value", SqlTypes.STRING)
        .build();

    new EqualsTester()
        .addEqualityGroup(
            LogicalSchema.builder()
                .keyField("k0", SqlTypes.BIGINT)
                .valueField("v0", SqlTypes.STRING)
                .build(),

            LogicalSchema.builder()
                .keyField("k0", SqlTypes.BIGINT)
                .valueField("v0", SqlTypes.STRING)
                .build()
        )
        .addEqualityGroup(
            LogicalSchema.builder()
                .valueField("f0", SqlTypes.STRING)
                .valueField("f1", SqlTypes.BIGINT)
                .build(),

            LogicalSchema.builder()
                .keyField(ROWKEY_NAME, SqlTypes.STRING)
                .valueField("f0", SqlTypes.STRING)
                .valueField("f1", SqlTypes.BIGINT)
                .build()
        )
        .addEqualityGroup(
            aSchema,

            aSchema
                .withMetaAndKeyFieldsInValue()
                .withoutMetaAndKeyFieldsInValue(),

            aSchema
                .withAlias("bob")
                .withoutAlias()
        )
        .addEqualityGroup(
            aSchema.withAlias("bob")
        )
        .addEqualityGroup(
            aSchema.withMetaAndKeyFieldsInValue()
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
        .keyField("k0", SqlTypes.BIGINT)
        .valueField("f0", SqlTypes.STRING)
        .valueField("f1", SqlTypes.struct()
            .field("nested", SqlTypes.BIGINT)
            .build())
        .build();

    // When:
    final LogicalSchema result = schema.withAlias("bob");

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyField(Column.of("bob", "k0", SqlTypes.BIGINT))
        .valueField(Column.of("bob", "f0", SqlTypes.STRING))
        .valueField(Column.of("bob", "f1", SqlTypes.struct()
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
        .keyField("k0", SqlTypes.INTEGER)
        .valueField("f0", SqlTypes.STRING)
        .valueField("f1", SqlTypes.struct()
            .field("bob.nested", SqlTypes.BIGINT)
            .build())
        .build().withAlias("bob");

    // When:
    final LogicalSchema result = schema.withoutAlias();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyField("k0", SqlTypes.INTEGER)
        .valueField("f0", SqlTypes.STRING)
        .valueField("f1", SqlTypes.struct()
            .field("bob.nested", SqlTypes.BIGINT)
            .build())
        .build()));
  }

  @Test
  public void shouldGetFieldByName() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueField("f0");

    // Then:
    assertThat(result, is(Optional.of(Column.of("f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldGetFieldByAliasedName() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueField("SomeAlias.f0");

    // Then:
    assertThat(result, is(Optional.of(Column.of("f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldNotGetFieldByNameIfWrongCase() {
    // When:
    final Optional<Column> result = SOME_SCHEMA.findValueField("F0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldNotGetFieldByNameIfFieldIsAliasedAndNameIsNot() {
    // When:
    final Optional<Column> result = ALIASED_SCHEMA.findValueField("f0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldGetFieldByNameIfBothFieldAndNameAreAliased() {
    // When:
    final Optional<Column> result = ALIASED_SCHEMA.findValueField("bob.f0");

    // Then:
    assertThat(result, is(Optional.of(Column.of("bob", "f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldNotGetMetaFieldFromValue() {
    assertThat(SOME_SCHEMA.findValueField("ROWTIME"), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetKeyFieldFromValue() {
    assertThat(SOME_SCHEMA.findValueField("k0"), is(Optional.empty()));
  }

  @Test
  public void shouldGetMetaFieldFromValueIfAdded() {
    assertThat(SOME_SCHEMA.withMetaAndKeyFieldsInValue().findValueField("ROWTIME"),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetKeyFieldFromValueIfAdded() {
    assertThat(SOME_SCHEMA.withMetaAndKeyFieldsInValue().findValueField("k0"),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetMetaFields() {
    assertThat(SOME_SCHEMA.findField("ROWTIME"), is(Optional.of(
        Column.of("ROWTIME", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldGetKeyFields() {
    assertThat(SOME_SCHEMA.findField("k0"), is(Optional.of(
        Column.of("k0", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldGetValueFields() {
    assertThat(SOME_SCHEMA.findField("f0"), is(Optional.of(
        Column.of("f0", SqlTypes.STRING)
    )));
  }

  @Test
  public void shouldGetFieldIndex() {
    assertThat(SOME_SCHEMA.valueFieldIndex("f0"), is(OptionalInt.of(0)));
    assertThat(SOME_SCHEMA.valueFieldIndex("f1"), is(OptionalInt.of(1)));
  }

  @Test
  public void shouldReturnMinusOneForIndexIfFieldNotFound() {
    assertThat(SOME_SCHEMA.valueFieldIndex("wontfindme"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldNotFindFieldIfDifferentCase() {
    assertThat(SOME_SCHEMA.valueFieldIndex("F0"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldGetAliasedFieldIndex() {
    assertThat(ALIASED_SCHEMA.valueFieldIndex("bob.f1"), is(OptionalInt.of(1)));
  }

  @Test
  public void shouldNotFindUnaliasedFieldIndexInAliasedSchema() {
    assertThat(ALIASED_SCHEMA.valueFieldIndex("f1"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldNotFindAliasedFieldIndexInUnaliasedSchema() {
    assertThat(SOME_SCHEMA.valueFieldIndex("bob.f1"), is(OptionalInt.empty()));
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
    assertThat(SOME_SCHEMA.fields(), is(ImmutableList.of(
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
    final List<Column> fields = schema.fields();

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
        .valueField("f0", SqlTypes.BOOLEAN)
        .valueField("f1", SqlTypes.INTEGER)
        .valueField("f2", SqlTypes.BIGINT)
        .valueField("f4", SqlTypes.DOUBLE)
        .valueField("f5", SqlTypes.STRING)
        .valueField("f6", SqlTypes.struct()
            .field("a", SqlTypes.BIGINT)
            .build())
        .valueField("f7", SqlTypes.array(SqlTypes.STRING))
        .valueField("f8", SqlTypes.map(SqlTypes.STRING))
        .keyField("k0", SqlTypes.BIGINT)
        .keyField("k1", SqlTypes.DOUBLE)
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
        .valueField("f0", SqlTypes.BOOLEAN)
        .valueField("f1", SqlTypes.struct()
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
        .valueField("f0", SqlTypes.BOOLEAN)
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
        .valueField("f0", SqlTypes.STRING)
        .valueField("f1", SqlTypes.BIGINT)
        .build();

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyFieldsInValue();

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
        .valueField("f0", SqlTypes.STRING)
        .valueField("f1", SqlTypes.BIGINT)
        .build()
        .withAlias("bob");

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyFieldsInValue();

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
        .valueField("f0", SqlTypes.STRING)
        .valueField("f1", SqlTypes.BIGINT)
        .build()
        .withMetaAndKeyFieldsInValue();

    // When:
    final LogicalSchema result = schema.withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(schema));
  }

  @Test
  public void shouldRemoveOthersWhenAddingMetasAndKeyFields() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.builder()
        .valueField("f0", SqlTypes.BIGINT)
        .valueField(SchemaUtil.ROWKEY_NAME, SqlTypes.DOUBLE)
        .valueField("f1", SqlTypes.BIGINT)
        .valueField(SchemaUtil.ROWTIME_NAME, SqlTypes.DOUBLE)
        .build();

    // When:
    final LogicalSchema result = ksqlSchema.withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueField(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueField(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueField("f0", SqlTypes.BIGINT)
        .valueField("f1", SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaFields() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueField("f0", SqlTypes.BIGINT)
        .valueField("f1", SqlTypes.BIGINT)
        .build()
        .withMetaAndKeyFieldsInValue();

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueField("f0", SqlTypes.BIGINT)
        .valueField("f1", SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaFieldsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueField("f0", SqlTypes.BIGINT)
        .valueField(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueField("f1", SqlTypes.BIGINT)
        .valueField(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .build();

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .valueField("f0", SqlTypes.BIGINT)
        .valueField("f1", SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldRemoveMetaFieldsEvenIfAliased() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueField("f0", SqlTypes.BIGINT)
        .valueField("f1", SqlTypes.BIGINT)
        .build()
        .withMetaAndKeyFieldsInValue()
        .withAlias("bob");

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyField(Column.of("bob", "ROWKEY", SqlTypes.STRING))
        .valueField(Column.of("bob", "f0", SqlTypes.BIGINT))
        .valueField(Column.of("bob", "f1", SqlTypes.BIGINT))
        .build()
    ));
  }

  @Test
  public void shouldMatchMetaFieldName() {
    assertThat(SOME_SCHEMA.isMetaField(ROWTIME_NAME), is(true));
    assertThat(SOME_SCHEMA.isKeyField(ROWTIME_NAME), is(false));
  }

  @Test
  public void shouldMatchKeyFieldName() {
    assertThat(SOME_SCHEMA.isMetaField("k0"), is(false));
    assertThat(SOME_SCHEMA.isKeyField("k0"), is(true));
  }

  @Test
  public void shouldNotMatchValueFieldsAsBeingMetaOrKeyFields() {
    SOME_SCHEMA.value().forEach(field ->
    {
      assertThat(SOME_SCHEMA.isMetaField(field.name()), is(false));
      assertThat(SOME_SCHEMA.isKeyField(field.name()), is(false));
    });
  }

  @Test
  public void shouldNotMatchRandomFieldNameAsBeingMetaOrKeyFields() {
    assertThat(SOME_SCHEMA.isMetaField("well_this_ain't_in_the_schema"), is(false));
    assertThat(SOME_SCHEMA.isKeyField("well_this_ain't_in_the_schema"), is(false));
  }

  @Test
  public void shouldThrowOnDuplicateKeyFieldName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .keyField("fieldName", SqlTypes.BIGINT);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate keys found in schema: `fieldName` BIGINT");

    // When:
    builder.keyField("fieldName", SqlTypes.BIGINT);
  }

  @Test
  public void shouldThrowOnDuplicateValueFieldName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .valueField("fieldName", SqlTypes.BIGINT);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate values found in schema: `fieldName` BIGINT");

    // When:
    builder.valueField("fieldName", SqlTypes.BIGINT);
  }

  @Test
  public void shouldAllowKeyFieldsWithSameNameButDifferentSource() {
    // Given:
    final Builder builder = LogicalSchema.builder();

    // When:
    builder
        .keyField("fieldName", SqlTypes.BIGINT)
        .keyField(Column.of("source", "fieldName", SqlTypes.BIGINT))
        .keyField(Column.of("diff", "fieldName", SqlTypes.BIGINT));

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
        .valueField("fieldName", SqlTypes.BIGINT)
        .valueField(Column.of("source", "fieldName", SqlTypes.BIGINT))
        .valueField(Column.of("diff", "fieldName", SqlTypes.BIGINT));

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
        .keyField("fieldName", SqlTypes.DOUBLE)
        .keyField(Column.of("source", "fieldName", SqlTypes.BOOLEAN))
        .keyField(Column.of("diff", "fieldName", SqlTypes.STRING))
        .valueField("fieldName", SqlTypes.BIGINT)
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
        .keyField("fieldName", SqlTypes.STRING)
        .valueField("fieldName", SqlTypes.BIGINT)
        .valueField(Column.of("source", "fieldName", SqlTypes.INTEGER))
        .valueField(Column.of("diff", "fieldName", SqlTypes.STRING))
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
