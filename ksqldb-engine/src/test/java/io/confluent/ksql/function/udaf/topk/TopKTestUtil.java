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
    final List<Object> sortCols = actual.stream()
            .map((struct) -> struct.get("sort_col"))
            .collect(Collectors.toList());
    assertThat("Invalid results.", sortCols, equalTo(expectedSortCols));

    final List<Object> colOnes = actual.stream()
            .map((struct) -> struct.get("col0"))
            .collect(Collectors.toList());
    assertThat("Invalid results.", colOnes, equalTo(expectedCol0s));

    final List<Object> colTwos = actual.stream()
            .map((struct) -> struct.get("col1"))
            .collect(Collectors.toList());
    assertThat("Invalid results.", colTwos, equalTo(expectedCol1s));
  }

}
