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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;

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
import java.util.stream.Stream;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LogicalSchemaTest {

  private static final Schema IMMUTABLE_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;

  private static final Schema SOME_VALUE_SCHEMA = SchemaBuilder.struct()
      .field("f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .field("f1", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .build();

  private static final Schema OTHER_CONNECT_SCHEMA = SchemaBuilder.struct()
      .field("id", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final Schema SOME_KEY_SCHEMA = SchemaBuilder.struct()
      .field("k0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .build();

  private static final LogicalSchema SOME_SCHEMA =
      LogicalSchema.of(SOME_KEY_SCHEMA, SOME_VALUE_SCHEMA);

  private static final LogicalSchema ALIASED_SCHEMA = SOME_SCHEMA.withAlias("bob");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEqualsProperly() {
    final Schema implicitKeySchema = SchemaBuilder.struct()
        .field(ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    final Schema otherKeySchema = SchemaBuilder.struct()
        .field("k0", Schema.OPTIONAL_INT32_SCHEMA)
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
                .build(),

            LogicalSchema.of(
                SchemaBuilder.struct()
                    .field("k0", Schema.OPTIONAL_INT64_SCHEMA)
                    .build(),
                SchemaBuilder.struct()
                    .field("v0", Schema.OPTIONAL_STRING_SCHEMA)
                    .build()
            )
        )
        .addEqualityGroup(
            LogicalSchema.of(SOME_VALUE_SCHEMA),
            LogicalSchema.of(implicitKeySchema, SOME_VALUE_SCHEMA),
            LogicalSchema.of(SOME_VALUE_SCHEMA).withAlias("bob").withoutAlias(),
            LogicalSchema.of(SOME_VALUE_SCHEMA)
                .withMetaAndKeyFieldsInValue()
                .withoutMetaAndKeyFieldsInValue(),
            LogicalSchema.builder()
                .valueField("f0", SqlTypes.STRING)
                .valueField("f1", SqlTypes.BIGINT)
                .build()
        )
        .addEqualityGroup(
            LogicalSchema.of(SOME_VALUE_SCHEMA).withAlias("bob")
        )
        .addEqualityGroup(
            LogicalSchema.of(OTHER_CONNECT_SCHEMA)
        )
        .addEqualityGroup(
            LogicalSchema.of(otherKeySchema, SOME_VALUE_SCHEMA)
        )
        .testEquals();
  }

  @Test
  public void shouldThrowOnNoneSqlTypesInKey() {
    Stream.of(
        Schema.OPTIONAL_INT8_SCHEMA,
        Schema.OPTIONAL_INT16_SCHEMA,
        Schema.OPTIONAL_FLOAT32_SCHEMA
    ).forEach(schema -> {
      try {
        final Schema keySchema = SchemaBuilder.struct().field("test", schema).build();
        LogicalSchema.of(keySchema, SOME_VALUE_SCHEMA);
        fail();
      } catch (final KsqlException e) {
        assertThat(schema.toString(), e.getMessage(), containsString("Unexpected schema type"));
      }
    });
  }

  @Test
  public void shouldThrowOnNoneSqlTypesInValue() {
    Stream.of(
        Schema.OPTIONAL_INT8_SCHEMA,
        Schema.OPTIONAL_INT16_SCHEMA,
        Schema.OPTIONAL_FLOAT32_SCHEMA
    ).forEach(schema -> {
      try {
        final Schema valueSchema = SchemaBuilder.struct().field("test", schema).build();
        LogicalSchema.of(SOME_KEY_SCHEMA, valueSchema);
        fail();
      } catch (final KsqlException e) {
        assertThat(schema.toString(), e.getMessage(), containsString("Unexpected schema type"));
      }
    });
  }

  @Test
  public void shouldThrowIfKeyNotTopLevelStruct() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Top level schema must be STRUCT");

    // When:
    LogicalSchema.of(Schema.OPTIONAL_INT64_SCHEMA, SOME_VALUE_SCHEMA);
  }

  @Test
  public void shouldThrowIfValueNotTopLevelStruct() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Top level schema must be STRUCT");

    // When:
    LogicalSchema.of(SOME_KEY_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void shouldNotThrowIfTopLevelNotOptionalInKey() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    // When:
    LogicalSchema.of(schema, SOME_VALUE_SCHEMA);

    // Then: did not throw.
  }

  @Test
  public void shouldNotThrowIfTopLevelNotOptionalInValue() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    // When:
    LogicalSchema.of(SOME_KEY_SCHEMA, schema);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnNoneStringMapKeyInKey() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unsupported map key type: Schema{INT64}");

    // When:
    LogicalSchema.of(
        nested(SchemaBuilder
            .map(Schema.OPTIONAL_INT64_SCHEMA, IMMUTABLE_SCHEMA)
            .optional()
            .build()),
        SOME_VALUE_SCHEMA
    );
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
    final LogicalSchema schema = LogicalSchema.of(
        SchemaBuilder.struct()
            .field("k0", Schema.OPTIONAL_INT64_SCHEMA)
            .build(),
        SchemaBuilder.struct()
            .field("f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("f1", SchemaBuilder.struct()
                .field("nested", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build()
    );

    // When:
    final LogicalSchema result = schema.withAlias("bob");

    // Then:
    assertThat(result, is(LogicalSchema.of(
        SchemaBuilder.struct()
            .field("bob.k0", Schema.OPTIONAL_INT64_SCHEMA)
            .build(),
        SchemaBuilder.struct()
            .field("bob.f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("bob.f1", SchemaBuilder
                .struct()
                .field("nested", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build()
    )));
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
    final LogicalSchema schema = LogicalSchema.of(
        SchemaBuilder.struct()
            .field("k0", Schema.OPTIONAL_INT32_SCHEMA)
            .build(),
        SchemaBuilder.struct()
            .field("f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("f1", SchemaBuilder.struct()
                .field("bob.nested", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build()
    ).withAlias("bob");

    // When:
    final LogicalSchema result = schema.withoutAlias();

    // Then:
    assertThat(result, is(LogicalSchema.of(
        SchemaBuilder.struct()
            .field("k0", Schema.OPTIONAL_INT32_SCHEMA)
            .build(),
        SchemaBuilder.struct()
            .field("f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("f1", SchemaBuilder
                .struct()
                .field("bob.nested", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build())));
  }

  @Test
  public void shouldGetFieldByName() {
    // When:
    final Optional<Field> result = SOME_SCHEMA.findValueField("f0");

    // Then:
    assertThat(result, is(Optional.of(Field.of("f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldGetFieldByAliasedName() {
    // When:
    final Optional<Field> result = SOME_SCHEMA.findValueField("SomeAlias.f0");

    // Then:
    assertThat(result, is(Optional.of(Field.of("f0", SqlTypes.STRING))));
  }

  @Test
  public void shouldNotGetFieldByNameIfWrongCase() {
    // When:
    final Optional<Field> result = SOME_SCHEMA.findValueField("F0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldNotGetFieldByNameIfFieldIsAliasedAndNameIsNot() {
    // When:
    final Optional<Field> result = ALIASED_SCHEMA.findValueField("f0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldGetFieldByNameIfBothFieldAndNameAreAliased() {
    // When:
    final Optional<Field> result = ALIASED_SCHEMA.findValueField("bob.f0");

    // Then:
    assertThat(result, is(Optional.of(Field.of("bob", "f0", SqlTypes.STRING))));
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
        Field.of("ROWTIME", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldGetKeyFields() {
    assertThat(SOME_SCHEMA.findField("k0"), is(Optional.of(
        Field.of("k0", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldGetValueFields() {
    assertThat(SOME_SCHEMA.findField("f0"), is(Optional.of(
        Field.of("f0", SqlTypes.STRING)
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
    assertThat(SOME_SCHEMA.metaFields(), is(ImmutableList.of(
        Field.of(ROWTIME_NAME, SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeAliasedMetaFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("fred");

    // When:
    final List<Field> fields = schema.metaFields();

    // Then:
    assertThat(fields, is(ImmutableList.of(
        Field.of("fred", ROWTIME_NAME, SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeKeyFields() {
    assertThat(SOME_SCHEMA.keyFields(), is(ImmutableList.of(
        Field.of("k0", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeAliasedKeyFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("fred");

    // When:
    final List<Field> fields = schema.keyFields();

    // Then:
    assertThat(fields, is(ImmutableList.of(
        Field.of("fred", "k0", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeValueFields() {
    assertThat(SOME_SCHEMA.valueFields(), contains(
        Field.of("f0", SqlTypes.STRING),
        Field.of("f1", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldExposeAliasedValueFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("bob");

    // When:
    final List<Field> fields = schema.valueFields();

    // Then:
    assertThat(fields, contains(
        Field.of("bob", "f0", SqlTypes.STRING),
        Field.of("bob", "f1", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldExposeAllFields() {
    assertThat(SOME_SCHEMA.fields(), is(ImmutableList.of(
        Field.of(ROWTIME_NAME, SqlTypes.BIGINT),
        Field.of("k0", SqlTypes.BIGINT),
        Field.of("f0", SqlTypes.STRING),
        Field.of("f1", SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldExposeAliasedAllFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("bob");

    // When:
    final List<Field> fields = schema.fields();

    // Then:
    assertThat(fields, is(ImmutableList.of(
        Field.of("bob", ROWTIME_NAME, SqlTypes.BIGINT),
        Field.of("bob", "k0", SqlTypes.BIGINT),
        Field.of("bob", "f0", SqlTypes.STRING),
        Field.of("bob", "f1", SqlTypes.BIGINT)
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
    final LogicalSchema schema = LogicalSchema.of(
        SchemaBuilder.struct()
            .field("f0", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .field("f1", SchemaBuilder
                .struct()
                .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
                .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build()
    );

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
    final LogicalSchema schema = LogicalSchema.of(
        SchemaBuilder.struct()
            .field("f0", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .build()
    ).withAlias("t");

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "["
            + "`t.ROWKEY` STRING KEY, "
            + "`t.f0` BOOLEAN"
            + "]"));
  }

  @Test
  public void shouldAddMetaAndKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SOME_VALUE_SCHEMA);

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result.valueFields(), hasSize(SOME_VALUE_SCHEMA.fields().size() + 2));
    assertThat(result.valueFields().get(0).name(), is(SchemaUtil.ROWTIME_NAME));
    assertThat(result.valueFields().get(0).type(), is(SqlTypes.BIGINT));
    assertThat(result.valueFields().get(1).name(), is(SchemaUtil.ROWKEY_NAME));
    assertThat(result.valueFields().get(1).type(), is(SqlTypes.STRING));
  }

  @Test
  public void shouldAddMetaAndKeyColumnsWhenAliased() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SOME_VALUE_SCHEMA)
        .withAlias("bob");

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result.valueFields(), hasSize(SOME_VALUE_SCHEMA.fields().size() + 2));
    assertThat(result.valueFields().get(0).fullName(), is("bob." + SchemaUtil.ROWTIME_NAME));
    assertThat(result.valueFields().get(0).type(), is(SqlTypes.BIGINT));
    assertThat(result.valueFields().get(1).fullName(), is("bob." + SchemaUtil.ROWKEY_NAME));
    assertThat(result.valueFields().get(1).type(), is(SqlTypes.STRING));
  }

  @Test
  public void shouldAddMetaAndKeyColumnsOnlyOnce() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.of(SOME_VALUE_SCHEMA)
        .withMetaAndKeyFieldsInValue();

    // When:
    final LogicalSchema result = ksqlSchema.withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(ksqlSchema));
  }

  @Test
  public void shouldRemoveOthersWhenAddingMetasAndKeyFields() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build()
    );

    // When:
    final LogicalSchema result = ksqlSchema.withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.of(SchemaBuilder.struct()
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    )));
  }

  @Test
  public void shouldRemoveMetaFields() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    ).withMetaAndKeyFieldsInValue();

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    )));
  }

  @Test
  public void shouldRemoveMetaFieldsWhereEverTheyAre() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    )));
  }

  @Test
  public void shouldRemoveMetaFieldsEvenIfAliased() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build())
        .withMetaAndKeyFieldsInValue()
        .withAlias("bob");

    // When
    final LogicalSchema result = schema.withoutMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result, is(LogicalSchema.of(
        SchemaBuilder.struct()
            .field("bob.ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
            .build(),
        SchemaBuilder.struct()
            .field("bob.f0", Schema.OPTIONAL_INT64_SCHEMA)
            .field("bob.f1", Schema.OPTIONAL_INT64_SCHEMA)
            .build()
    )));
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
    SOME_SCHEMA.valueFields().forEach(field ->
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
    expectedException.expect(IllegalArgumentException.class);

    // When:
    builder.keyField("fieldName", SqlTypes.BIGINT);
  }

  @Test
  public void shouldThrowOnDuplicateValueFieldName() {
    // Given:
    final Builder builder = LogicalSchema.builder()
        .valueField("fieldName", SqlTypes.BIGINT);

    // Then:
    expectedException.expect(IllegalArgumentException.class);

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
        .keyField(Field.of("source", "fieldName", SqlTypes.BIGINT))
        .keyField(Field.of("diff", "fieldName", SqlTypes.BIGINT));

    // Then:
    assertThat(builder.build().keyFields(), contains(
        Field.of("fieldName", SqlTypes.BIGINT),
        Field.of("source", "fieldName", SqlTypes.BIGINT),
        Field.of("diff", "fieldName", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldAllowValueFieldsWithSameNameButDifferentSource() {
    // Given:
    final Builder builder = LogicalSchema.builder();

    // When:
    builder
        .valueField("fieldName", SqlTypes.BIGINT)
        .valueField(Field.of("source", "fieldName", SqlTypes.BIGINT))
        .valueField(Field.of("diff", "fieldName", SqlTypes.BIGINT));

    // Then:
    assertThat(builder.build().valueFields(), contains(
        Field.of("fieldName", SqlTypes.BIGINT),
        Field.of("source", "fieldName", SqlTypes.BIGINT),
        Field.of("diff", "fieldName", SqlTypes.BIGINT)
    ));
  }

  @Test
  public void shouldGetKeySchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyField("fieldName", SqlTypes.DOUBLE)
        .keyField(Field.of("source", "fieldName", SqlTypes.BOOLEAN))
        .keyField(Field.of("diff", "fieldName", SqlTypes.STRING))
        .valueField("fieldName", SqlTypes.BIGINT)
        .build();

    // When:
    final ConnectSchema result = schema.keySchema();

    // Then:
    final List<org.apache.kafka.connect.data.Field> fields = result.fields();
    assertThat(fields, contains(
        connectField("fieldName", 0, Schema.OPTIONAL_FLOAT64_SCHEMA),
        connectField("source.fieldName", 1, Schema.OPTIONAL_BOOLEAN_SCHEMA),
        connectField("diff.fieldName", 2, Schema.OPTIONAL_STRING_SCHEMA)
    ));
  }

  @Test
  public void shouldGetValueSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyField("fieldName", SqlTypes.STRING)
        .valueField("fieldName", SqlTypes.BIGINT)
        .valueField(Field.of("source", "fieldName", SqlTypes.INTEGER))
        .valueField(Field.of("diff", "fieldName", SqlTypes.STRING))
        .build();

    // When:
    final ConnectSchema result = schema.valueSchema();

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

  private static Schema nested(final Schema schema) {
    // Nest the schema under test within another layer of schema to ensure checks are deep:
    return SchemaBuilder.struct()
        .field("f0", schema)
        .build();
  }
}
