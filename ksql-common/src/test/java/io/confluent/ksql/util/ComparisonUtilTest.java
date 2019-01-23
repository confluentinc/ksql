/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

public class ComparisonUtilTest {

  @Test
  public void shouldReturnCorrectFunctionForComparisonCompatibility() {
    final List<Type> typesTable = ImmutableList.of(
        Schema.Type.BOOLEAN, Schema.Type.INT32, Schema.Type.INT64, Schema.Type.FLOAT64, Schema.Type.STRING, Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.STRUCT
    );
    final List<List<Boolean>> expectedResults = ImmutableList.of(
        ImmutableList.of(true, false, false, false, false, false, false, false), // Boolean
        ImmutableList.of(false, true, true, true, false, false, false, false), // Int
        ImmutableList.of(false, true, true, true, false, false, false, false), // BigInt
        ImmutableList.of(false, true, true, true, false, false, false, false), // Double
        ImmutableList.of(false, false, false, false, true, false, false, false),  // String
        ImmutableList.of(false, false, false, false, false, false, false, false), // Array
        ImmutableList.of(false, false, false, false, false, false, false, false), // Map
        ImmutableList.of(false, false, false, false, false, false, false, false) // Struct
    );
    int i = 0;
    int j = 0;
    for (final Schema.Type leftType: typesTable) {
      for (final Schema.Type rightType: typesTable) {
        assertThat(ComparisonUtil.areCompatibleTypesForComparison(leftType, rightType), equalTo(expectedResults.get(i).get(j)));
        j++;
      }
      i++;
      j = 0;
    }
  }
}