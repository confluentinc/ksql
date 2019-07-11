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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LogicalSchemaTest {

  private static final Schema IMMUTABLE_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;
  private static final SchemaBuilder MUTABLE_SCHEMA = SchemaBuilder.struct()
      .optional()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA);

  private static final Schema SOME_CONNECT_SCHEMA = SchemaBuilder.struct()
      .field("f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .field("f1", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .build();

  private static final Schema OTHER_CONNECT_SCHEMA = SchemaBuilder.struct()
      .field("id", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final Schema ALIASED_CONNECT_SCHEMA = SchemaBuilder.struct()
      .field("bob.f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .field("bob.f1", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .build();

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.of(SOME_CONNECT_SCHEMA);
  private static final LogicalSchema ALIASED_SCHEMA = SOME_SCHEMA.withAlias("bob");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            LogicalSchema.of(SOME_CONNECT_SCHEMA),
            LogicalSchema.of(SOME_CONNECT_SCHEMA),
            LogicalSchema.of(SOME_CONNECT_SCHEMA).withAlias("bob").withoutAlias()
        )
        .addEqualityGroup(
            LogicalSchema.of(SOME_CONNECT_SCHEMA).withAlias("bob")
        )
        .addEqualityGroup(
            LogicalSchema.of(OTHER_CONNECT_SCHEMA)
        )
        .testEquals();
  }

  @Test
  public void shouldThrowOnNoneSqlTypes() {
    Stream.of(
        Schema.OPTIONAL_INT8_SCHEMA,
        Schema.OPTIONAL_INT16_SCHEMA,
        Schema.OPTIONAL_FLOAT32_SCHEMA
    ).forEach(schema -> {
      try {
        LogicalSchema.of(SchemaBuilder.struct().field("test", schema).build());
        fail();
      } catch (final IllegalArgumentException e) {
        assertThat(schema.toString(), e.getMessage(), containsString("Unsupported schema type"));
      }
    });
  }

  @Test
  public void shouldThrowIfNotTopLevelStruct() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Top level schema must be STRUCT");

    // When:
    LogicalSchema.of(Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void shouldThrowOnMutableStructFields() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Mutable schema found");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.struct()
            .field("fieldWithMutableSchema", MUTABLE_SCHEMA)
            .optional()
            .build()
    ));
  }

  @Test
  public void shouldThrowOnMutableMapKeys() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Mutable schema found");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.map(new SchemaBuilder(Type.STRING).optional(), IMMUTABLE_SCHEMA)
            .optional()
            .build()
    ));
  }

  @Test
  public void shouldThrowOnMutableMapValues() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Mutable schema found");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.map(IMMUTABLE_SCHEMA, MUTABLE_SCHEMA)
            .optional()
            .build()
    ));
  }

  @Test
  public void shouldThrowOnMutableArrayElements() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Mutable schema found");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.array(MUTABLE_SCHEMA)
            .optional()
            .build()
    ));
  }

  @Test
  public void shouldThrowOnNoneOptionalMapKeys() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Non-optional field found");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.map(IMMUTABLE_SCHEMA, IMMUTABLE_SCHEMA)
            .build()
    ));
  }

  @Test
  public void shouldThrowOnNoneOptionalMapValues() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Non-optional field found");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.map(IMMUTABLE_SCHEMA, IMMUTABLE_SCHEMA).build()
    ));
  }

  @Test
  public void shouldThrowOnNoneOptionalElements() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Non-optional field found");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.array(IMMUTABLE_SCHEMA).build()
    ));
  }

  @Test
  public void shouldNotThrowIfTopLevelNotOptional() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    // When:
    LogicalSchema.of(schema);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnNoneStringMapLey() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("MAP only supports STRING keys");

    // When:
    LogicalSchema.of(nested(
        SchemaBuilder.map(Schema.OPTIONAL_INT64_SCHEMA, IMMUTABLE_SCHEMA)
            .optional()
            .build()
    ));
  }

  @Test
  public void shouldThrowOnNonDecimalBytes() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Expected schema of type DECIMAL but got a schema of type BYTES and name foobar");

    // When:
    LogicalSchema.of(nested(SchemaBuilder.bytes().name("foobar").optional().build()));
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
    assertThat(result, is(LogicalSchema.of(SchemaBuilder.struct()
        .field("bob.f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("bob.f1", SchemaBuilder
            .struct()
            .field("nested", Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build())
        .build())));
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
    assertThat(result, is(LogicalSchema.of(SchemaBuilder.struct()
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
    assertThat(result, is(Optional.of(SOME_CONNECT_SCHEMA.field("f0"))));
  }

  @Test
  public void shouldGetFieldByAliasedName() {
    // When:
    final Optional<Field> result = SOME_SCHEMA.findValueField("SomeAlias.f0");

    // Then:
    assertThat(result, is(Optional.of(SOME_CONNECT_SCHEMA.field("f0"))));
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
    assertThat(result, is(Optional.of(ALIASED_CONNECT_SCHEMA.field("bob.f0"))));
  }

  @Test
  public void shouldNotGetMetaFieldFromValue() {
    assertThat(SOME_SCHEMA.findValueField("ROWTIME"), is(Optional.empty()));
  }

  @Test
  public void shouldNotGetKeyFieldFromValue() {
    assertThat(SOME_SCHEMA.findValueField("ROWKEY"), is(Optional.empty()));
  }

  @Test
  public void shouldGetMetaFieldFromValueIfAdded() {
    assertThat(SOME_SCHEMA.withMetaAndKeyFieldsInValue().findValueField("ROWTIME"),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetKeyFieldFromValueIfAdded() {
    assertThat(SOME_SCHEMA.withMetaAndKeyFieldsInValue().findValueField("ROWKEY"),
        is(not(Optional.empty())));
  }

  @Test
  public void shouldGetMetaFields() {
    assertThat(SOME_SCHEMA.findField("ROWTIME"), is(Optional.of(
        new Field("ROWTIME", 0, Schema.OPTIONAL_INT64_SCHEMA)
    )));
  }

  @Test
  public void shouldGetFields() {
    assertThat(SOME_SCHEMA.findField("ROWKEY"), is(Optional.of(
        new Field("ROWKEY", 0, Schema.OPTIONAL_STRING_SCHEMA)
    )));
  }

  @Test
  public void shouldGetValueFields() {
    assertThat(SOME_SCHEMA.findField("f0"), is(Optional.of(
        new Field("f0", 0, Schema.OPTIONAL_STRING_SCHEMA)
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
        new Field(ROWTIME_NAME, 0, Schema.OPTIONAL_INT64_SCHEMA)
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
        new Field("fred." + ROWTIME_NAME, 0, Schema.OPTIONAL_INT64_SCHEMA)
    )));
  }

  @Test
  public void shouldExposeKeyFields() {
    assertThat(SOME_SCHEMA.keyFields(), is(ImmutableList.of(
        new Field(ROWKEY_NAME, 0, Schema.OPTIONAL_STRING_SCHEMA)
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
        new Field("fred." + ROWKEY_NAME, 0, Schema.OPTIONAL_STRING_SCHEMA)
    )));
  }

  @Test
  public void shouldExposeValueFields() {
    assertThat(SOME_SCHEMA.valueFields(), is(SOME_CONNECT_SCHEMA.fields()));
  }

  @Test
  public void shouldExposeAliasedValueFields() {
    // Given:
    final LogicalSchema schema = SOME_SCHEMA.withAlias("bob");

    // When:
    final List<Field> fields = schema.valueFields();

    // Then:
    assertThat(fields, is(ALIASED_CONNECT_SCHEMA.fields()));
  }

  @Test
  public void shouldExposeAllFields() {
    assertThat(SOME_SCHEMA.fields(), is(ImmutableList.of(
        new Field(ROWTIME_NAME, 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field(ROWKEY_NAME, 0, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("f0", 0, SchemaBuilder.OPTIONAL_STRING_SCHEMA),
        new Field("f1", 1, SchemaBuilder.OPTIONAL_INT64_SCHEMA)
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
        new Field("bob." + ROWTIME_NAME, 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("bob." + ROWKEY_NAME, 0, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("bob.f0", 0, SchemaBuilder.OPTIONAL_STRING_SCHEMA),
        new Field("bob.f1", 1, SchemaBuilder.OPTIONAL_INT64_SCHEMA)
    )));
  }

  @Test
  public void shouldConvertSchemaToString() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(
        SchemaBuilder.struct()
            .field("f0", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .field("f1", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
            .field("f2", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("f4", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
            .field("f5", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("f6", SchemaBuilder
                .struct()
                .field("a", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .field("f7", SchemaBuilder
                .array(
                    SchemaBuilder.OPTIONAL_STRING_SCHEMA
                )
                .optional()
                .build())
            .field("f8", SchemaBuilder
                .map(
                    SchemaBuilder.OPTIONAL_STRING_SCHEMA,
                    SchemaBuilder.OPTIONAL_STRING_SCHEMA
                )
                .optional()
                .build())
            .build()
    );

    // When:
    final String s = schema.toString();

    // Then:
    assertThat(s, is(
        "["
            + "`f0` BOOLEAN, "
            + "`f1` INT, "
            + "`f2` BIGINT, "
            + "`f4` DOUBLE, "
            + "`f5` VARCHAR, "
            + "`f6` STRUCT<`a` BIGINT>, "
            + "`f7` ARRAY<VARCHAR>, "
            + "`f8` MAP<VARCHAR, VARCHAR>"
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
            + "`t.f0` BOOLEAN"
            + "]"));
  }

  @Test
  public void shouldAddMetaAndKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SOME_CONNECT_SCHEMA);

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result.valueFields(), hasSize(SOME_CONNECT_SCHEMA.fields().size() + 2));
    assertThat(result.valueFields().get(0).name(), is(SchemaUtil.ROWTIME_NAME));
    assertThat(result.valueFields().get(0).index(), is(0));
    assertThat(result.valueFields().get(0).schema(), is(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(result.valueFields().get(1).name(), is(SchemaUtil.ROWKEY_NAME));
    assertThat(result.valueFields().get(1).index(), is(1));
    assertThat(result.valueFields().get(1).schema(), is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldAddMetaAndKeyColumnsWhenAliased() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SOME_CONNECT_SCHEMA)
        .withAlias("bob");

    // When:
    final LogicalSchema result = schema
        .withMetaAndKeyFieldsInValue();

    // Then:
    assertThat(result.valueFields(), hasSize(SOME_CONNECT_SCHEMA.fields().size() + 2));
    assertThat(result.valueFields().get(0).name(), is("bob." + SchemaUtil.ROWTIME_NAME));
    assertThat(result.valueFields().get(0).index(), is(0));
    assertThat(result.valueFields().get(0).schema(), is(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(result.valueFields().get(1).name(), is("bob." + SchemaUtil.ROWKEY_NAME));
    assertThat(result.valueFields().get(1).index(), is(1));
    assertThat(result.valueFields().get(1).schema(), is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldAddMetaAndKeyColumnsOnlyOnce() {
    // Given:
    final LogicalSchema ksqlSchema = LogicalSchema.of(SOME_CONNECT_SCHEMA)
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
    assertThat(result, is(LogicalSchema.of(SchemaBuilder.struct()
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
    assertThat(SOME_SCHEMA.isMetaField(ROWKEY_NAME), is(false));
    assertThat(SOME_SCHEMA.isKeyField(ROWKEY_NAME), is(true));
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

  private static Schema nested(final Schema schema) {
    // Nest the schema under test within another layer of schema to ensure checks are deep:
    return SchemaBuilder.struct()
        .field("f0", schema)
        .build();
  }
}
