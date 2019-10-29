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

package io.confluent.ksql.function;

/**
 * This interface is used to invoke UDFs
 */
public interface FunctionInvoker {

  /**
   * Call onto an UDF instance with the expected args. This is providing
   * a wrapper such that we can invoke all UDFs in a generic way.
   *
   * @param udf     the udf that is being called
   * @param udfArgs any arguments that need to be passed to the udf
   * @return  the result of evaluating the udf
   */
  Object eval(Object udf, Object... udfArgs);
}
