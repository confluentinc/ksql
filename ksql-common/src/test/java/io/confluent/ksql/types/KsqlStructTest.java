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

package io.confluent.ksql.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import io.confluent.ksql.schema.ksql.DataException;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.FieldName;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KsqlStructTest {

  private static final SqlStruct SCHEMA = SqlTypes.struct()
      .field("f0", SqlTypes.BIGINT)
      .field(Field.of(FieldName.of("s1", "v1"), SqlTypes.BOOLEAN))
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldHandleExplicitNulls() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set(FieldName.of("f0"), Optional.empty())
        .set(FieldName.of("s1", "v1"), Optional.empty())
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.empty(), Optional.empty()));
  }

  @Test
  public void shouldHandleImplicitNulls() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.empty(), Optional.empty()));
  }

  @Test
  public void shouldThrowFieldNotKnown() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unknown field: `??`");

    // When:
    KsqlStruct.builder(SCHEMA)
        .set("??", Optional.empty());
  }

  @Test
  public void shouldThrowIfValueWrongType() {
    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("Expected BIGINT, got STRING");

    // When:
    KsqlStruct.builder(SCHEMA)
        .set("f0", Optional.of("field is BIGINT, so won't like this"));
  }

  @Test
  public void shouldBuildStruct() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set(FieldName.of("f0"), Optional.of(10L))
        .set(FieldName.of("s1", "v1"), Optional.of(true))
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.of(10L), Optional.of(true)));
  }
}