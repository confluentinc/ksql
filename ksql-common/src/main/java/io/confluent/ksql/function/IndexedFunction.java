/*
 * Copyright 2019 Confluent Inc.
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

import java.util.List;
import org.apache.kafka.connect.data.Schema;

/**
 * An interface that designates functions that can be indexed using the
 * {@link UdfIndex}.
 */
public interface IndexedFunction {

  /**
   * @return the function name
   */
  String getFunctionName();

  /**
   * @return the schemas for the parameters
   */
  List<Schema> getArguments();

  /**
   * @return whether or not to consider the final argument in
   *         {@link #getArguments()} as variadic
   */
  boolean isVariadic();


}
