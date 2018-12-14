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

package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class CountAggFunctionFactory extends AggregateFunctionFactory {
  private static final String FUNCTION_NAME = "COUNT";

  public CountAggFunctionFactory() {
    super(FUNCTION_NAME, Collections.singletonList(new CountKudaf(FUNCTION_NAME, -1)));
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(final List<Schema> argTypeList) {
    return getAggregateFunctionList().get(0);
  }
}
