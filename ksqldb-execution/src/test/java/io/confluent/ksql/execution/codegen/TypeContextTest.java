/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.codegen;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;


import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class TypeContextTest {

  @Test
  public void shouldThrowOnLambdaMismatch() {
    // Given
    TypeContext context = new TypeContext();
    context.addLambdaInputType(SqlTypes.STRING);
    context.addLambdaInputType(SqlTypes.STRING);

    // When
    final Exception e =
        assertThrows(IllegalArgumentException.class, () -> context.mapLambdaInputTypes(ImmutableList.of("x", "y", "z")));

    // Then
    assertThat(e.getMessage(),
        is("Was expecting 2 arguments but found 3, [x, y, z]. Check your lambda statement."));
  }

  @Test
  public void shouldMapLambdaTypes() {
    // Given
    TypeContext context = new TypeContext();
    context.addLambdaInputType(SqlTypes.STRING);
    context.addLambdaInputType(SqlTypes.BIGINT);

    // When
    context.mapLambdaInputTypes(ImmutableList.of("x", "y"));

    // Then
    assertThat(context.getLambdaType("x"), is(SqlTypes.STRING));
    assertThat(context.getLambdaType("y"), is(SqlTypes.BIGINT));
  }
}
