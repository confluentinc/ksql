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

package io.confluent.ksql.metastore.model;

import static io.confluent.ksql.schema.ksql.ColumnMatchers.valueColumn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KeyFieldTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("field0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field1"), SqlTypes.BIGINT)
      .build();

  private static final Column OTHER_SCHEMA_COL = SCHEMA.value().get(1);

  private static final String SOME_ALIAS = "fred";

  private static final KeyField ALIASED_KEY_FIELD = KeyField.of(
      ColumnRef.of(SourceName.of(SOME_ALIAS), OTHER_SCHEMA_COL.name())
  );

  private static final KeyField UNALIASED_KEY_FIELD = KeyField.of(OTHER_SCHEMA_COL.ref());

  private static final ColumnRef VALID_COL_REF = SCHEMA.value().get(0).ref();
  private static final SqlType VALID_COL_TYPE = SCHEMA.value().get(0).type();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    final ColumnRef keyField = ColumnRef.withoutSource(ColumnName.of("key"));

    new EqualsTester()
        .addEqualityGroup(KeyField.of(keyField), KeyField.of(keyField))
        .addEqualityGroup(KeyField.of(Optional.empty()))
        .addEqualityGroup(KeyField.of(ColumnRef.withoutSource(ColumnName.of("different"))))
        .testEquals();
  }

  @Test
  public void shouldHandleNonEmpty() {
    // Given:
    final ColumnRef columnRef = ColumnRef.withoutSource(ColumnName.of("something"));

    // When:
    final KeyField keyField = KeyField.of(columnRef);

    // Then:
    assertThat(keyField.ref(), is(Optional.of(columnRef)));
  }

  @Test
  public void shouldHandleEmpty() {
    // When:
    final KeyField keyField = KeyField.of(Optional.empty());

    // Then:
    assertThat(keyField.ref(), is(Optional.empty()));
    assertThat(keyField, is(KeyField.none()));
  }

  @Test
  public void shouldNotThrowOnValidateIfKeyInSchema() {
    // Given:
    final KeyField keyField = KeyField.of(VALID_COL_REF);

    // When:
    keyField.validateKeyExistsIn(SCHEMA);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnValidateIfKeyNotInSchema() {
    // Given:
    final KeyField keyField = KeyField.of(ColumnRef.withoutSource(ColumnName.of("????")));

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid key field, not found in schema: ????");

    // When:
    keyField.validateKeyExistsIn(SCHEMA);
  }

  @Test
  public void shouldNotThrowOnValidateIfNoKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty());

    // When:
    keyField.validateKeyExistsIn(SCHEMA);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnResolveIfSchemaDoesNotContainKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(ColumnRef.withoutSource(ColumnName.of("not found")));

    // Then:
    expectedException.expect(IllegalArgumentException.class);

    // When:
    keyField.resolve(SCHEMA);
  }

  @Test
  public void shouldThrowIfKeyColumnTypeDoesNotMatchWithKeyFieldType() {
    // Given:
    final KeyField keyField = KeyField.of(SCHEMA.value().get(1).ref());

    // Then:
    expectedException.expect(IllegalArgumentException.class);

    // When:
    keyField.validateKeyExistsIn(SCHEMA);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldResolveKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(VALID_COL_REF);

    // When:
    final Optional<Column> resolved = keyField.resolve(SCHEMA);

    // Then:
    assertThat(resolved, is(not(Optional.empty())));
    assertThat(resolved.get(), is(valueColumn(VALID_COL_REF.source(), VALID_COL_REF.name(), VALID_COL_TYPE)));
  }

  @Test
  public void shouldResolveEmptyKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty());

    // When:
    final Optional<Column> resolved = keyField.resolve(SCHEMA);

    // Then:
    assertThat(resolved, is(Optional.empty()));
  }

  @Test
  public void shouldBuildWithAlias() {
    // When:
    final KeyField result = UNALIASED_KEY_FIELD.withAlias(SourceName.of("fred"));

    // Then:
    assertThat(result, is(ALIASED_KEY_FIELD));
  }

  @Test
  public void shouldBuildWithAliasIfAlreadyAliased() {
    // When:
    final KeyField result = ALIASED_KEY_FIELD.withAlias(SourceName.of("fred"));

    // Then:
    assertThat(result, is(ALIASED_KEY_FIELD));
  }
}