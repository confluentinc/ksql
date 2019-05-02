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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KsqlSchemaTest {

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

  private static final KsqlSchema SOME_SCHEMA = KsqlSchema.of(SOME_CONNECT_SCHEMA);
  private static final KsqlSchema ALIASED_SCHEMA = KsqlSchema.of(ALIASED_CONNECT_SCHEMA);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            KsqlSchema.of(SOME_CONNECT_SCHEMA), KsqlSchema.of(SOME_CONNECT_SCHEMA)
        )
        .addEqualityGroup(
            KsqlSchema.of(ALIASED_CONNECT_SCHEMA)
        )
        .addEqualityGroup(
            KsqlSchema.of(OTHER_CONNECT_SCHEMA)
        )
        .testEquals();
  }

  @Test
  public void shouldThrowOnNoneSqlTypes() {
    Stream.of(
        Schema.OPTIONAL_INT8_SCHEMA,
        Schema.OPTIONAL_INT16_SCHEMA,
        Schema.OPTIONAL_FLOAT32_SCHEMA,
        Schema.OPTIONAL_BYTES_SCHEMA

    ).forEach(schema -> {
      try {
        KsqlSchema.of(SchemaBuilder.struct().field("test", schema).build());
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
    KsqlSchema.of(Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void shouldThrowOnMutableStructFields() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Mutable schema found");

    // When:
    KsqlSchema.of(nested(
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
    KsqlSchema.of(nested(
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
    KsqlSchema.of(nested(
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
    KsqlSchema.of(nested(
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
    KsqlSchema.of(nested(
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
    KsqlSchema.of(nested(
        SchemaBuilder.map(IMMUTABLE_SCHEMA, IMMUTABLE_SCHEMA).build()
    ));
  }

  @Test
  public void shouldThrowOnNoneOptionalElements() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Non-optional field found");

    // When:
    KsqlSchema.of(nested(
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
    KsqlSchema.of(schema);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnNoneStringMapLey() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("MAP only supports STRING keys");

    // When:
    KsqlSchema.of(nested(
        SchemaBuilder.map(Schema.OPTIONAL_INT64_SCHEMA, IMMUTABLE_SCHEMA)
            .optional()
            .build()
    ));
  }

  @Test
  public void shouldBuildSchemaWithAlias() {
    // When:
    final KsqlSchema result = SOME_SCHEMA.withAlias("bob");

    // Then:
    assertThat(result, is(ALIASED_SCHEMA));
  }

  @Test
  public void shouldCopySchemaIfSchemaAlreadyAliased() {
    // When:
    final KsqlSchema result = ALIASED_SCHEMA.withAlias("bob");

    // Then:
    assertThat(result, is(ALIASED_SCHEMA));
  }

  @Test
  public void shouldOnlyAddAliasToTopLevelFields() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(
        SchemaBuilder.struct()
            .field("f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("f1", SchemaBuilder.struct()
                .field("nested", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build()
    );

    // When:
    final KsqlSchema result = schema.withAlias("bob");

    // Then:
    assertThat(result, is(KsqlSchema.of(SchemaBuilder.struct()
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
    final KsqlSchema result = ALIASED_SCHEMA.withoutAlias();

    // Then:
    assertThat(result, is(SOME_SCHEMA));
  }

  @Test
  public void shouldCopySchemaIfSchemaAlreadyUnaliased() {
    // When:
    final KsqlSchema result = SOME_SCHEMA.withoutAlias();

    // Then:
    assertThat(result, is(SOME_SCHEMA));
  }

  @Test
  public void shouldOnlyRemoveAliasFromTopLevelFields() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(
        SchemaBuilder.struct()
            .field("bob.f0", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("bob.f1", SchemaBuilder.struct()
                .field("bob.nested", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build()
    );

    // When:
    final KsqlSchema result = schema.withoutAlias();

    // Then:
    assertThat(result, is(KsqlSchema.of(SchemaBuilder.struct()
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
    final Optional<Field> result = SOME_SCHEMA.findField("f0");

    // Then:
    assertThat(result, is(Optional.of(SOME_CONNECT_SCHEMA.field("f0"))));
  }

  @Test
  public void shouldGetFieldByAliasedName() {
    // When:
    final Optional<Field> result = SOME_SCHEMA.findField("SomeAlias.f0");

    // Then:
    assertThat(result, is(Optional.of(SOME_CONNECT_SCHEMA.field("f0"))));
  }

  @Test
  public void shouldNotGetFieldByNameIfWrongCase() {
    // When:
    final Optional<Field> result = SOME_SCHEMA.findField("F0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldNotGetFieldByNameIfFieldIsAliasedAndNameIsNot() {
    // When:
    final Optional<Field> result = ALIASED_SCHEMA.findField("f0");

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldGetFieldByNameIfBothFieldAndNameAreAliased() {
    // When:
    final Optional<Field> result = ALIASED_SCHEMA.findField("bob.f0");

    // Then:
    assertThat(result, is(Optional.of(ALIASED_CONNECT_SCHEMA.field("bob.f0"))));
  }

  @Test
  public void shouldGetFieldIndex() {
    assertThat(SOME_SCHEMA.fieldIndex("f0"), is(OptionalInt.of(0)));
    assertThat(SOME_SCHEMA.fieldIndex("f1"), is(OptionalInt.of(1)));
  }

  @Test
  public void shouldReturnMinusOneForIndexIfFieldNotFound() {
    assertThat(SOME_SCHEMA.fieldIndex("wontfindme"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldNotFindFieldIfDifferentCase() {
    assertThat(SOME_SCHEMA.fieldIndex("F0"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldGetAliasedFieldIndex() {
    assertThat(ALIASED_SCHEMA.fieldIndex("bob.f1"), is(OptionalInt.of(1)));
  }

  @Test
  public void shouldNotFindUnaliasedFieldIndexInAliasedSchema() {
    assertThat(ALIASED_SCHEMA.fieldIndex("f1"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldNotFindAliasedFieldIndexInUnaliasedSchema() {
    assertThat(SOME_SCHEMA.fieldIndex("bob.f1"), is(OptionalInt.empty()));
  }

  @Test
  public void shouldExposeFields() {
    assertThat(SOME_SCHEMA.fields(), is(SOME_CONNECT_SCHEMA.fields()));
  }

  @Test
  public void shouldConvertSchemaToString() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(
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
            + "f0 : BOOLEAN, "
            + "f1 : INT, "
            + "f2 : BIGINT, "
            + "f4 : DOUBLE, "
            + "f5 : VARCHAR, "
            + "f6 : STRUCT<a BIGINT>, "
            + "f7 : ARRAY<VARCHAR>, "
            + "f8 : MAP<VARCHAR,VARCHAR>"
            + "]"));
  }

  @Test
  public void shouldAddImplicitColumns() {
    // When:
    final KsqlSchema result = KsqlSchema.of(SOME_CONNECT_SCHEMA).withImplicitFields();

    // Then:
    assertThat(result.fields(), hasSize(SOME_CONNECT_SCHEMA.fields().size() + 2));
    assertThat(result.fields().get(0).name(), is(SchemaUtil.ROWTIME_NAME));
    assertThat(result.fields().get(0).index(), is(0));
    assertThat(result.fields().get(0).schema(), is(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(result.fields().get(1).name(), is(SchemaUtil.ROWKEY_NAME));
    assertThat(result.fields().get(1).index(), is(1));
    assertThat(result.fields().get(1).schema(), is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldAddImplicitColumnsOnlyOnce() {
    // Given:
    final KsqlSchema ksqlSchema = KsqlSchema.of(SOME_CONNECT_SCHEMA)
        .withImplicitFields();

    // When:
    final KsqlSchema result = ksqlSchema.withImplicitFields();

    // Then:
    assertThat(result, is(ksqlSchema));
  }

  @Test
  public void shouldRemoveOtherImplicitsWhenAddingImplicit() {
    // Given:
    final KsqlSchema ksqlSchema = KsqlSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build()
    );

    // When:
    final KsqlSchema result = ksqlSchema.withImplicitFields();

    // Then:
    assertThat(result, is(KsqlSchema.of(SchemaBuilder.struct()
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    )));
  }

  @Test
  public void shouldRemoveImplicitFields() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder.struct()
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When
    final KsqlSchema result = schema.withoutImplicitFields();

    // Then:
    assertThat(result, is(KsqlSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    )));
  }

  @Test
  public void shouldRemoveImplicitFieldsWhereEverTheyAre() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When
    final KsqlSchema result = schema.withoutImplicitFields();

    // Then:
    assertThat(result, is(KsqlSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    )));
  }

  @Test
  public void shouldRemoveImplicitFieldsEvenIfAliased() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder.struct()
        .field("bob.f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("bob." + SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("bob.f1", Schema.OPTIONAL_INT64_SCHEMA)
        .field("bob." + SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When
    final KsqlSchema result = schema.withoutImplicitFields();

    // Then:
    assertThat(result, is(KsqlSchema.of(SchemaBuilder.struct()
        .field("bob.f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("bob.f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    )));
  }

  @Test
  public void shouldGetImplicitColumnIndexes() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When
    final Set<Integer> result = schema.implicitColumnIndexes();

    // Then:
    assertThat(result, containsInAnyOrder(1, 3));
  }

  @Test
  public void shouldGetNoImplicitColumnIndexesIfImplicitColumnsAsAliased() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder.struct()
        .field("bob.f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("bob." + SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .field("bob.f1", Schema.OPTIONAL_INT64_SCHEMA)
        .field("bob." + SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When
    final Set<Integer> result = schema.implicitColumnIndexes();

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldGetNoImplicitColumnIndexesIfNoImplicitColumns() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When
    final Set<Integer> result = schema.implicitColumnIndexes();

    // Then:
    assertThat(result, is(empty()));
  }

  private static Schema nested(final Schema schema) {
    // Nest the schema under test within another layer of schema to ensure checks are deep:
    return SchemaBuilder.struct()
        .field("f0", schema)
        .build();
  }
}