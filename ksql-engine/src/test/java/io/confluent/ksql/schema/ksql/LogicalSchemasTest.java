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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Type.KsqlType;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.Test;

public class LogicalSchemasTest {

  private static final Set<KsqlType> COMPLEX_SQL_TYPES = ImmutableSet.of(
      // Note: do not add new complex types to this set without adding suitable tests in this file.
      KsqlType.ARRAY, KsqlType.MAP, KsqlType.STRUCT
  );

  private static final Predicate<KsqlType> IS_COMPLEX_TYPE = COMPLEX_SQL_TYPES::contains;
  private static final Predicate<KsqlType> IS_SIMPLE_TYPE = IS_COMPLEX_TYPE.negate();

  private static final Set<PrimitiveType> PRIMITIVE_TYPES = ImmutableSet.copyOf(
      Arrays.stream(KsqlType.values())
          .filter(IS_SIMPLE_TYPE)
          .map(PrimitiveType::new)
          .collect(Collectors.toSet()));

  @Test
  public void shouldGetLogicalForEveryPrimitiveSqlType() {
    PRIMITIVE_TYPES.forEach(type -> {
      assertThat(LogicalSchemas.fromPrimitiveSqlType(type), is(notNullValue()));
    });
  }
}