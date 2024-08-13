/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.topk;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;

public class TopKTestUtil {

  public static <T> void checkAggregate(final List<T> expectedSortCols,
                                        final List<Boolean> expectedCol0s,
                                        final List<String> expectedCol1s,
                                        final List<Struct> actual) {
    checkStructField(expectedSortCols, actual, "sort_col");
    checkStructField(expectedCol0s, actual, "col0");
    checkStructField(expectedCol1s, actual, "col1");
  }

  private static void checkStructField(final List<?> expected,
                                       final List<Struct> actual, final String fieldName) {
    final List<Object> actualCols = actual.stream()
            .map((struct) -> struct.get(fieldName))
            .collect(Collectors.toList());
    assertThat("Invalid results.", actualCols, equalTo(expected));
  }

}
