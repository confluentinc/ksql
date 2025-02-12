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

package io.confluent.ksql.analyzer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceSchemasTest {

  private static final SourceName ALIAS_1 = SourceName.of("S1");
  private static final SourceName ALIAS_2 = SourceName.of("S2");

  private static final ColumnName K0 = ColumnName.of("K0");
  private static final ColumnName K1 = ColumnName.of("K1");

  private static final ColumnName COMMON_VALUE_FIELD_NAME = ColumnName.of("V0");
  private static final ColumnName V1 = ColumnName.of("V1");
  private static final ColumnName V2 = ColumnName.of("V2");

  private static final LogicalSchema SCHEMA_1 = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .keyColumn(K0, SqlTypes.INTEGER)
      .valueColumn(COMMON_VALUE_FIELD_NAME, SqlTypes.STRING)
      .valueColumn(V1, SqlTypes.STRING)
      .build();

  private static final LogicalSchema SCHEMA_2 = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .keyColumn(K1, SqlTypes.STRING)
      .valueColumn(COMMON_VALUE_FIELD_NAME, SqlTypes.STRING)
      .valueColumn(V2, SqlTypes.STRING)
      .build();

  private SourceSchemas sourceSchemas;

  @Before
  public void setUp() {
    sourceSchemas = new SourceSchemas(ImmutableMap.of(ALIAS_1, SCHEMA_1, ALIAS_2, SCHEMA_2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNoSchemas() {
    new SourceSchemas(ImmutableMap.of());
  }

  @Test
  public void shouldNotBeJoinIfSingleSchema() {
    // When:
    sourceSchemas = new SourceSchemas(ImmutableMap.of(ALIAS_1, SCHEMA_1));

    // Then:
    assertThat(sourceSchemas.isJoin(), is(false));
  }

  @Test
  public void shouldBeJoinIfMultipleSchemas() {
    // When:
    sourceSchemas = new SourceSchemas(ImmutableMap.of(ALIAS_1, SCHEMA_1, ALIAS_2, SCHEMA_2));

    // Then:
    assertThat(sourceSchemas.isJoin(), is(true));
  }

  @Test
  public void shouldFindNoField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.empty(), ColumnName.of("unknown")), is(empty()));
  }

  @Test
  public void shouldFindNoQualifiedField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.of(ALIAS_1), V2), is(empty()));
  }

  @Test
  public void shouldFindUnqualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.empty(), V1), contains(ALIAS_1));
  }

  @Test
  public void shouldFindQualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.of(ALIAS_2), V2), contains(ALIAS_2));
  }

  @Test
  public void shouldFindUnqualifiedCommonField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.empty(), COMMON_VALUE_FIELD_NAME),
        containsInAnyOrder(ALIAS_1, ALIAS_2));
  }

  @Test
  public void shouldFindQualifiedFieldOnlyInThatSource() {
    assertThat(sourceSchemas.sourcesWithField(Optional.of(ALIAS_1), COMMON_VALUE_FIELD_NAME),
        contains(ALIAS_1));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfMetaField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.empty(), SystemColumns.ROWTIME_NAME), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliaasedMetaField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.of(ALIAS_2), SystemColumns.ROWTIME_NAME), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfKeyField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.empty(), K0), is(true));
    assertThat(sourceSchemas.matchesNonValueField(Optional.empty(), K1), is(true));
  }

  @Test
  public void shouldNotMatchNonKeyFieldOnWrongSource() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.of(ALIAS_2), K0), is(false));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliasedKeyField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.of(ALIAS_2), K1), is(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnknownSourceWhenMatchingNonValueFields() {
    sourceSchemas.matchesNonValueField(Optional.of(SourceName.of("unknown")), K0);
  }

  @Test
  public void shouldNotMatchOtherFields() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.of(ALIAS_2), V2), is(false));
  }

  @Test
  public void shouldNotMatchUnknownFields() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.empty(), ColumnName.of("unknown")), is(false));
  }
}
