/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udaf.topkdistinct;

import io.confluent.ksql.function.AggregateFunctionArguments;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;

public class TopKDistinctTestUtils {
  @SuppressWarnings("unchecked")
  public static <T extends Comparable<? super T>> TopkDistinctKudaf<T> getTopKDistinctKudaf(
      final int topk, final Schema schema) {
    return (TopkDistinctKudaf<T>) new TopkDistinctAggFunctionFactory()
        .getProperAggregateFunction(
            Collections.singletonList(schema))
        .getInstance(
            new AggregateFunctionArguments(0, Arrays.asList("foo", Integer.toString(topk))));
  }
}
