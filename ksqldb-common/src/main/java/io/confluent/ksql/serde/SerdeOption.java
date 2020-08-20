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

package io.confluent.ksql.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableSet;
import java.util.Set;

public enum SerdeOption {

  /**
   * If the value schema contains only a single field, persist it as an anonymous value.
   *
   * <p>If not set, any single field value schema will be persisted within an outer object, e.g.
   * JSON Object or Avro Record.
   */
  UNWRAP_SINGLE_VALUES;

  @JsonCreator
  public static SerdeOption fromString(final String value) {
    return SerdeOption.valueOf(value);
  }

  public static Set<SerdeOption> none() {
    return ImmutableSet.of();
  }

  public static Set<SerdeOption> of(final SerdeOption... options) {
    return ImmutableSet.copyOf(options);
  }
}
