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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.metastore.model.KeyField.LegacyField;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KeyFieldTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueField("field0", SqlTypes.STRING)
      .valueField("field1", SqlTypes.BIGINT)
      .build();

  private static final KsqlConfig LEGACY_CONFIG = new KsqlConfig(
      ImmutableMap.of(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true)
  );

  private static final KsqlConfig LATEST_CONFIG = new KsqlConfig(
      ImmutableMap.of()
  );

  private static final LegacyField RANDOM_LEGACY_FIELD =
      LegacyField.of("won't find me anywhere", SqlTypes.STRING);

  private static final LegacyField LEGACY_FIELD = LegacyField
      .of(SCHEMA.valueFields().get(0).fullName(), SCHEMA.valueFields().get(0).type());

  private static final Field OTHER_SCHEMA_FIELD = SCHEMA.valueFields().get(1);

  private static final String SOME_ALIAS = "fred";

  private static final KeyField ALIASED_KEY_FIELD = KeyField.of(
      SOME_ALIAS + "." + OTHER_SCHEMA_FIELD.name(),
      LegacyField.of(SOME_ALIAS + "." + LEGACY_FIELD.name(), LEGACY_FIELD.type())
  );

  private static final KeyField UNALIASED_KEY_FIELD = KeyField
      .of(OTHER_SCHEMA_FIELD.name(), LEGACY_FIELD);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    final Optional<String> keyField = Optional.of("key");
    final Optional<LegacyField> legacy = Optional.of(LEGACY_FIELD);

    new EqualsTester()
        .addEqualityGroup(KeyField.of(keyField, legacy), KeyField.of(keyField, legacy))
        .addEqualityGroup(KeyField.of(Optional.empty(), legacy))
        .addEqualityGroup(KeyField.of(keyField, Optional.empty()))
        .testEquals();
  }

  @Test
  public void shouldHandleLegacyEmpty() {
    // When:
    final KeyField keyField = KeyField.of(Optional.of("something"), Optional.empty());

    // Then:
    assertThat(keyField.name(), is(Optional.of("something")));
    assertThat(keyField.legacy(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleNewEmpty() {
    // When:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.of(RANDOM_LEGACY_FIELD));

    // Then:
    assertThat(keyField.name(), is(Optional.empty()));
    assertThat(keyField.legacy(), is(Optional.of(RANDOM_LEGACY_FIELD)));
  }

  @Test
  public void shouldHandleBothEmpty() {
    // When:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.empty());

    // Then:
    assertThat(keyField.name(), is(Optional.empty()));
    assertThat(keyField.legacy(), is(Optional.empty()));
  }

  @Test
  public void shouldNotThrowOnValidateIfKeyInSchema() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of("field0"), Optional.empty());

    // When:
    keyField.validateKeyExistsIn(SCHEMA);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnValidateIfKeyNotInSchema() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of("????"), Optional.empty());

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid key field, not found in schema: ????");

    // When:
    keyField.validateKeyExistsIn(SCHEMA);
  }

  @Test
  public void shouldNotThrowOnValidateIfKeyNotInSchema() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.of(RANDOM_LEGACY_FIELD));

    // When:
    keyField.validateKeyExistsIn(SCHEMA);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnResolveIfSchemaDoesNotContainNewKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of("not found"), Optional.empty());

    // Then:
    expectedException.expect(IllegalArgumentException.class);

    // When:
    keyField.resolve(SCHEMA, LATEST_CONFIG);
  }

  @Test
  public void shouldNotThrowOnResolveIfSchemaDoesNotContainsLegacyKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.of(RANDOM_LEGACY_FIELD));

    // When:
    final Optional<Field> resolved = keyField.resolve(SCHEMA, LEGACY_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.of(Field.of(RANDOM_LEGACY_FIELD.name(), RANDOM_LEGACY_FIELD.type()))));
  }

  @Test
  public void shouldResolveToNewKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of(LEGACY_FIELD.name()), Optional.empty());

    // When:
    final Optional<Field> resolved = keyField.resolve(SCHEMA, LATEST_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.of(Field.of(LEGACY_FIELD.name(), LEGACY_FIELD.type()))));
  }

  @Test
  public void shouldResolveToLegacyKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.of(LEGACY_FIELD));

    // When:
    final Optional<Field> resolved = keyField.resolve(SCHEMA, LEGACY_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.of(Field.of(LEGACY_FIELD.name(), LEGACY_FIELD.type()))));
  }

  @Test
  public void shouldResolveToEmptyNewKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.of(LEGACY_FIELD));

    // When:
    final Optional<Field> resolved = keyField.resolve(SCHEMA, LATEST_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.empty()));
  }

  @Test
  public void shouldResolveToEmptyLegacyKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of("something"), Optional.empty());

    // When:
    final Optional<Field> resolved = keyField.resolve(SCHEMA, LEGACY_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.empty()));
  }

  @Test
  public void shouldResolveNameToNewKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of(LEGACY_FIELD.name()), Optional.empty());

    // When:
    final Optional<String> resolved = keyField.resolveName(LATEST_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.of(LEGACY_FIELD.name())));
  }

  @Test
  public void shouldResolveNameToEmptyNewKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.of(LEGACY_FIELD));

    // When:
    final Optional<?> resolved = keyField.resolveName(LATEST_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.empty()));
  }

  @Test
  public void shouldResolveNameToLegacyKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.empty(), Optional.of(RANDOM_LEGACY_FIELD));

    // When:
    final Optional<String> resolved = keyField.resolveName(LEGACY_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.of(RANDOM_LEGACY_FIELD.name())));
  }

  @Test
  public void shouldResolveNameToEmptyLegacyKeyField() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of(LEGACY_FIELD.name()), Optional.empty());

    // When:
    final Optional<String> resolved = keyField.resolveName(LEGACY_CONFIG);

    // Then:
    assertThat(resolved, is(Optional.empty()));
  }

  @Test
  public void shouldBuildNewWithNewName() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of("something"), Optional.empty());

    // When:
    final KeyField result = keyField.withName("new-name");

    // Then:
    assertThat(keyField.name(), is(Optional.of("something")));
    assertThat(result, is(KeyField.of(Optional.of("new-name"), Optional.empty())));
  }

  @Test
  public void shouldBuildNewWithLegacy() {
    // Given:
    final KeyField keyField = KeyField.of(Optional.of("something"), Optional.empty());

    // When:
    final KeyField result = keyField.withLegacy(Optional.of(LEGACY_FIELD));

    // Then:
    assertThat(keyField.legacy(), is(Optional.empty()));
    assertThat(result, is(KeyField.of("something", LEGACY_FIELD)));
  }

  @Test
  public void shouldBuildWithAlias() {
    // When:
    final KeyField result = UNALIASED_KEY_FIELD.withAlias("fred");

    // Then:
    assertThat(result, is(ALIASED_KEY_FIELD));
  }

  @Test
  public void shouldBuildWithAliasIfAlreadyAliased() {
    // When:
    final KeyField result = ALIASED_KEY_FIELD.withAlias("fred");

    // Then:
    assertThat(result, is(ALIASED_KEY_FIELD));
  }
}