/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.junit.Test;

public class ComparisonUtilTest {

  private static final List<SqlType> typesTable = ImmutableList.of(
      SqlTypes.BOOLEAN,
      SqlTypes.INTEGER,
      SqlTypes.BIGINT,
      SqlTypes.DOUBLE,
      SqlDecimal.of(4, 2),
      SqlTypes.STRING,
      SqlTypes.array(SqlTypes.STRING),
      SqlTypes.map(SqlTypes.BIGINT, SqlTypes.STRING),
      SqlTypes.struct().field("foo", SqlTypes.BIGINT).build(),
      SqlTypes.TIMESTAMP,
      SqlTypes.TIME,
      SqlTypes.DATE,
      SqlTypes.BYTES
  );

  private static final List<List<Boolean>> expectedResults = ImmutableList.of(
      ImmutableList.of(true, false, false, false, false, false, false, false, false, false, false, false, false), // Boolean
      ImmutableList.of(false, true, true, true, true, false, false, false, false, false, false, false, false), // Int
      ImmutableList.of(false, true, true, true, true, false, false, false, false, false, false, false, false), // BigInt
      ImmutableList.of(false, true, true, true, true, false, false, false, false, false, false, false, false), // Double
      ImmutableList.of(false, true, true, true, true, false, false, false, false, false, false, false, false),  // Decimal
      ImmutableList.of(false, false, false, false, false, true, false, false, false, true, true, true, false),  // String
      ImmutableList.of(false, false, false, false, false, false, true, false, false, false, false, false, false), // Array
      ImmutableList.of(false, false, false, false, false, false, false, true, false, false, false, false, false), // Map
      ImmutableList.of(false, false, false, false, false, false, false, false, true, false, false, false, false), // Struct
      ImmutableList.of(false, false, false, false, false, true, false, false, false, true, false, true, false), // Timestamp
      ImmutableList.of(false, false, false, false, false, true, false, false, false, false, true, false, false), // Time
      ImmutableList.of(false, false, false, false, false, true, false, false, false, true, false, true, false), // Date
      ImmutableList.of(false, false, false, false, false, false, false, false, false, false, false, false, true) // Bytes
  );

  @Test
  public void shouldAssertTrueForValidComparisons() {
    // When:
    int i = 0;
    int j = 0;
    for (final SqlType leftType: typesTable) {
      for (final SqlType rightType: typesTable) {
        if (expectedResults.get(i).get(j)) {
          assertThat(ComparisonUtil.isValidComparison(leftType, ComparisonExpression.Type.EQUAL,
              rightType), is(true));
        }

        j++;
      }
      i++;
      j = 0;
    }
  }

  @Test
  public void shouldThrowForInvalidComparisons() {
    // When:
    int i = 0;
    int j = 0;
    for (final SqlType leftType : typesTable) {
      for (final SqlType rightType : typesTable) {
        assertThat(ComparisonUtil.isValidComparison(leftType, ComparisonExpression.Type.EQUAL,
            rightType), is(expectedResults.get(i).get(j)));
        j++;
      }
      i++;
      j = 0;
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void shouldNotCompareLeftNullSchema() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> ComparisonUtil.isValidComparison(null, ComparisonExpression.Type.EQUAL, SqlTypes.STRING)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Comparison with NULL not supported: NULL = STRING"));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void shouldNotCompareLeftRightSchema() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> ComparisonUtil.isValidComparison(SqlTypes.STRING, ComparisonExpression.Type.EQUAL, null)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Comparison with NULL not supported: STRING = NULL"));
  }
}