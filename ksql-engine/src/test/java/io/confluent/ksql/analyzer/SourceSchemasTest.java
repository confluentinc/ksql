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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class SourceSchemasTest {

  private static final String ALIAS_1 = "S1";
  private static final String ALIAS_2 = "S2";
  private static final String COMMON_FIELD_NAME = "F0";

  private static final LogicalSchema SCHEMA_1 = LogicalSchema.of(SchemaBuilder.struct()
      .field(COMMON_FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .field("F1", Schema.OPTIONAL_STRING_SCHEMA)
      .build()
  );

  private static final LogicalSchema SCHEMA_2 = LogicalSchema.of(SchemaBuilder.struct()
      .field(COMMON_FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .field("F2", Schema.OPTIONAL_STRING_SCHEMA)
      .build()
  );

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
    assertThat(sourceSchemas.sourcesWithField("unknown"), is(empty()));
  }

  @Test
  public void shouldFindNoQualifiedField() {
    assertThat(sourceSchemas.sourcesWithField(ALIAS_1 + ".F2"), is(empty()));
  }

  @Test
  public void shouldFindUnqualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField("F1"), contains(ALIAS_1));
  }

  @Test
  public void shouldFindQualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField(ALIAS_2 + ".F2"), contains(ALIAS_2));
  }

  @Test
  public void shouldFindUnqualifiedCommonField() {
    assertThat(sourceSchemas.sourcesWithField(COMMON_FIELD_NAME),
        containsInAnyOrder(ALIAS_1, ALIAS_2));
  }

  @Test
  public void shouldFindQualifiedFieldOnlyInThatSource() {
    assertThat(sourceSchemas.sourcesWithField(ALIAS_1 + "." + COMMON_FIELD_NAME),
        contains(ALIAS_1));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfMetaField() {
    assertThat(sourceSchemas.matchesNonValueField("ROWTIME"), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliaasedMetaField() {
    assertThat(sourceSchemas.matchesNonValueField(ALIAS_2 + ".ROWTIME"), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfKeyField() {
    assertThat(sourceSchemas.matchesNonValueField("ROWKEY"), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliasedKeyField() {
    assertThat(sourceSchemas.matchesNonValueField(ALIAS_2 + ".ROWKEY"), is(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUknonwnSourceWhenMatchingNonValueFields() {
    sourceSchemas.matchesNonValueField("unknown.ROWKEY");
  }

  @Test
  public void shouldNotMatchOtherFields() {
    assertThat(sourceSchemas.matchesNonValueField(ALIAS_2 + ".F2"), is(false));
  }

  @Test
  public void shouldNotMatchUnknownFields() {
    assertThat(sourceSchemas.matchesNonValueField("unknown"), is(false));
  }
}
