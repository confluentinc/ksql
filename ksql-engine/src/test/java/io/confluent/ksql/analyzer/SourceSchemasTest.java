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
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class SourceSchemasTest {

  private static final SourceName ALIAS_1 = SourceName.of("S1");
  private static final SourceName ALIAS_2 = SourceName.of("S2");
  private static final ColumnName COMMON_FIELD_NAME = ColumnName.of("F0");

  private static final LogicalSchema SCHEMA_1 = LogicalSchema.builder()
      .valueColumn(COMMON_FIELD_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema SCHEMA_2 = LogicalSchema.builder()
      .valueColumn(COMMON_FIELD_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("F2"), SqlTypes.STRING)
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
    assertThat(sourceSchemas.sourcesWithField(Optional.empty(), ColumnRef.of(ColumnName.of("unknown"))), is(empty()));
  }

  @Test
  public void shouldFindNoQualifiedField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.of(ALIAS_1), ColumnRef.of(ColumnName.of("F2"))), is(empty()));
  }

  @Test
  public void shouldFindUnqualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.empty(), ColumnRef.of(ColumnName.of("F1"))), contains(ALIAS_1));
  }

  @Test
  public void shouldFindQualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.of(ALIAS_2), ColumnRef.of(ColumnName.of("F2"))), contains(ALIAS_2));
  }

  @Test
  public void shouldFindUnqualifiedCommonField() {
    assertThat(sourceSchemas.sourcesWithField(Optional.empty(), ColumnRef.of(COMMON_FIELD_NAME)),
        containsInAnyOrder(ALIAS_1, ALIAS_2));
  }

  @Test
  public void shouldFindQualifiedFieldOnlyInThatSource() {
    assertThat(sourceSchemas.sourcesWithField(Optional.of(ALIAS_1), ColumnRef.of(COMMON_FIELD_NAME)),
        contains(ALIAS_1));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfMetaField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.empty(), ColumnRef.of(SchemaUtil.ROWTIME_NAME)), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliaasedMetaField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.of(ALIAS_2), ColumnRef.of(SchemaUtil.ROWTIME_NAME)), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfKeyField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.empty(), ColumnRef.of(SchemaUtil.ROWKEY_NAME)), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliasedKeyField() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.of(ALIAS_2), ColumnRef.of(SchemaUtil.ROWKEY_NAME)), is(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnknownSourceWhenMatchingNonValueFields() {
    sourceSchemas.matchesNonValueField(Optional.of(SourceName.of("unknown")), ColumnRef.of(SchemaUtil.ROWKEY_NAME));
  }

  @Test
  public void shouldNotMatchOtherFields() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.of(ALIAS_2), ColumnRef.of(ColumnName.of("F2"))), is(false));
  }

  @Test
  public void shouldNotMatchUnknownFields() {
    assertThat(sourceSchemas.matchesNonValueField(Optional.empty(), ColumnRef.of(ColumnName.of("unknown"))), is(false));
  }
}
