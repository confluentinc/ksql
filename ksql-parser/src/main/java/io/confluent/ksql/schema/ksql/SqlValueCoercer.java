/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Optional;

/**
 * Coerces values to {@link SqlBaseType SQL types}.
 */
public interface SqlValueCoercer {

  /**
   * Coerce the supplied {@code value} to the supplied {@code sqlType}.
   *
   * <p>Complex SQL types are not supported, (yet).
   *
   * @param value the value to try to coerce.
   * @param targetSchema the target SQL type.
   * @param <T> target Java type
   * @return the coerced value if the value could be coerced, {@link Optional#empty()} otherwise.
   */
  <T> Optional<T> coerce(Object value, SqlType targetSchema);
}
