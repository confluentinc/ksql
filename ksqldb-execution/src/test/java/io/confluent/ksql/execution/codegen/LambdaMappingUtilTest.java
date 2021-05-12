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
public class LambdaMappingUtilTest {

  @Test
  public void shouldThrowOnMappingMismatch() {
    // Given
    final Map<String, SqlType> oldMapping = ImmutableMap.of("x", SqlTypes.BIGINT, "y", SqlTypes.STRING);
    final Map<String, SqlType> newMapping = ImmutableMap.of("x", SqlTypes.STRING, "y", SqlTypes.STRING);
    
    // When
    final Exception e =
        assertThrows(IllegalStateException.class, () ->
            LambdaMappingUtil.resolveOldAndNewLambdaMapping(newMapping, oldMapping)
        );

    // Then
    assertThat(e.getMessage(),
        is("Could not map type STRING to lambda variable x, x was already mapped to BIGINT"));
  }

  @Test
  public void shouldCopyContextWithLambdaMapping() {
    // Given
    final Map<String, SqlType> oldMapping = new HashMap<>(ImmutableMap.of("x", SqlTypes.STRING, "y", SqlTypes.BIGINT));
    final Map<String, SqlType> newMapping = new HashMap<>(ImmutableMap.of("x", SqlTypes.STRING, "z", SqlTypes.BOOLEAN));

    // When
    final Map<String, SqlType> updatedMapping = LambdaMappingUtil.resolveOldAndNewLambdaMapping(oldMapping, newMapping);

    // Then
    assertThat(updatedMapping.get("x"), is(SqlTypes.STRING));
    assertThat(updatedMapping.get("y"), is(SqlTypes.BIGINT));
    assertThat(updatedMapping.get("z"), is(SqlTypes.BOOLEAN));
  }
}