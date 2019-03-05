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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.tree.Struct.Field;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class StructTest {

  private static final List<Field> SOME_OTHER_FIELDS = ImmutableList.of(
      new Field("f0", PrimitiveType.of(SqlType.BOOLEAN))
  );

  private static final List<Field> SOME_FIELDS = ImmutableList.of(
      new Field("f0", PrimitiveType.of(SqlType.BOOLEAN)),
      new Field("f1", PrimitiveType.of(SqlType.INTEGER)),
      new Field("f2", PrimitiveType.of(SqlType.BIGINT)),
      new Field("f3", PrimitiveType.of(SqlType.DOUBLE)),
      new Field("f4", PrimitiveType.of(SqlType.STRING)),
      new Field("f5", Array.of(PrimitiveType.of(SqlType.BOOLEAN))),
      new Field("f6", Map.of(PrimitiveType.of(SqlType.INTEGER))),
      new Field("f7", Struct.builder().addFields(SOME_OTHER_FIELDS).build())
  );

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            Struct.builder().addFields(SOME_FIELDS).build(),
            Struct.builder().addFields(SOME_FIELDS).build()
        )
        .addEqualityGroup(
            Struct.builder().addFields(SOME_OTHER_FIELDS).build()
        )
        .addEqualityGroup(Map.of(PrimitiveType.of(SqlType.BOOLEAN)))
        .testEquals();
  }

  @Test
  public void shouldReturnSqlType() {
    assertThat(Struct.builder().addFields(SOME_FIELDS).build().getSqlType(), is(SqlType.STRUCT));
  }

  @Test
  public void shouldReturnFields() {
    assertThat(Struct.builder().addFields(SOME_FIELDS).build().getFields(), is(SOME_FIELDS));
  }

  @Test
  public void shouldThrowIfNoFields() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("STRUCT type must define fields");

    // When:
    Struct.builder().build();
  }

  @Test
  public void shouldThrowOnDuplicateFieldName() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Duplicate field names found in STRUCT: 'F0 BOOLEAN' and 'F0 INTEGER'");

    // When:
    Struct.builder()
        .addField("F0", PrimitiveType.of(SqlType.BOOLEAN))
        .addField("F0", PrimitiveType.of(SqlType.INTEGER));
  }

  @Test
  public void shouldNotThrowIfTwoFieldsHaveSameNameButDifferentCase() {
    // When:
    Struct.builder()
        .addField("f0", PrimitiveType.of(SqlType.BOOLEAN))
        .addField("F0", PrimitiveType.of(SqlType.INTEGER))
        .build();

    // Then: did not throw.
  }
}