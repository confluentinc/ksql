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

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
public class TypeContextTest {

  @Test
  public void shouldThrowOnMappingMismatch() {
    // Given
    final TypeContext context = new TypeContext(ImmutableMap.of("x", SqlTypes.STRING, "y", SqlTypes.STRING));

    // When
    final Exception e =
        assertThrows(IllegalStateException.class, () ->
            context.copyWithLambdaVariableTypeMapping(
                ImmutableMap.of("x", SqlTypes.BIGINT, "y", SqlTypes.STRING))
        );

    // Then
    assertThat(e.getMessage(),
        is("Could not resolve type for lambda variable x, cannot be both STRING and BIGINT"));
  }

  @Test
  public void shouldCopyContextWithLambdaMapping() {
    // Given
    final Map<String, SqlType> arguments = new HashMap<>(ImmutableMap.of("x", SqlTypes.STRING, "y", SqlTypes.BIGINT));
    final TypeContext context = new TypeContext(arguments);

    // When
    final TypeContext copyContext = context.copyWithLambdaVariableTypeMapping(arguments);

    // Then
    assertThat(copyContext.equals(context), is(true));
    assertThat(context.getLambdaType("x"), is(copyContext.getLambdaType("x")));
    assertThat(context.getLambdaType("y"), is(copyContext.getLambdaType("y")));
  }
}
