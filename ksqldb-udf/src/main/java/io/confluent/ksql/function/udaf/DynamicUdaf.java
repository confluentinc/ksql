/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;

public interface DynamicUdaf<I, A, O> extends Udaf<I, A, O> {

  /**
   * Sets the types of the input arguments for this Udaf
   * This type is used to build the necessary internal structures for the aggregate data.
   * E.g., we may need this to handle nulls completely and correctly.
   * @param argTypeList Types for the input
   */
  void setInputArgumentTypes(List<SqlArgument> argTypeList);

  /**
   *
   * @return SqlType for the immediate aggregations
   */
  SqlType getAggregateType();

  /**
   *
   * @return SqlType for the return type
   */
  SqlType getReturnType();
}
