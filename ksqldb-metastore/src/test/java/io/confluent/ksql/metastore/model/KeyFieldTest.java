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

import static io.confluent.ksql.metastore.model.KeyField.of;
import static io.confluent.ksql.schema.ksql.ColumnMatchers.valueColumn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Test;

public class KeyFieldTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("field0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field1"), SqlTypes.BIGINT)
      .build();

  private static final ColumnName VALID_COL_REF = SCHEMA.value().get(0).ref();
  private static final SqlType VALID_COL_TYPE = SCHEMA.value().get(0).type();

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    final ColumnName keyField = ColumnName.of("key");

    new EqualsTester()
        .addEqualityGroup(KeyField.of(keyField), KeyField.of(keyField))
        .addEqualityGroup(KeyField.of(Optional.empty()))
        .addEqualityGroup(KeyField.of(ColumnName.of("different")))
        .testEquals();
  }

  @Test
  public void shouldHandleNonEmpty() {
    // Given:
    final ColumnName columnName = ColumnName.of("something");

    // When:
    final KeyField keyField = KeyField.of(columnName);

    // Then:
    assertThat(keyField.ref(), is(Optional.of(columnName)));
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
    final KeyField keyField = KeyField.of(ColumnName.of("????"));

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> keyField.validateKeyExistsIn(SCHEMA)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid key field, not found in schema: ????"));
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
    final KeyField keyField = of(ColumnName.of("not found"));

    // When:
    assertThrows(
        IllegalArgumentException.class,
        () -> keyField.resolve(SCHEMA)
    );
  }

  @Test
  public void shouldThrowIfKeyColumnTypeDoesNotMatchWithKeyFieldType() {
    // Given:
    final KeyField keyField = of(SCHEMA.value().get(1).ref());

    // When:
    assertThrows(
        IllegalArgumentException.class,
        () -> keyField.validateKeyExistsIn(SCHEMA)
    );
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
    assertThat(resolved.get(), is(valueColumn(VALID_COL_REF, VALID_COL_TYPE)));
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
}