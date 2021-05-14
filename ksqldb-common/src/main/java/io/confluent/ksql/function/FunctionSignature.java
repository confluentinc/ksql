/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.name.FunctionName;
import java.util.List;

/**
 * Describes something that has a function signature and can be indexed in a {@link UdfIndex}.
 */
public interface FunctionSignature {

  /**
   * @return the function name
   */
  FunctionName name();

  /**
   * @return the unresolved return type - this could be something like {@code STRUCT}, which
   *         without any fields even if the resolved return type may have fields
   */
  ParamType declaredReturnType();

  /**
   * @return the schemas for the parameters
   */
  List<ParamType> parameters();

  /**
   * @return the {@link ParameterInfo} for the parameters
   */
  List<ParameterInfo> parameterInfo();

  /**
   * @return whether or not to consider the final argument in
   *         {@link #parameters()} as variadic
   */
  default boolean isVariadic() {
    return false;
  }


}
