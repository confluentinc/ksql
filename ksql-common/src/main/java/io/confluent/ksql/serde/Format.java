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

import io.confluent.ksql.util.KsqlException;

public enum Format {

  JSON(true),
  AVRO(true),
  DELIMITED(false),
  KAFKA(false);

  private final boolean supportsUnwrapping;

  Format(final boolean supportsUnwrapping) {
    this.supportsUnwrapping = supportsUnwrapping;
  }

  public boolean supportsUnwrapping() {
    return supportsUnwrapping;
  }

  public static Format of(final String value) {
    try {
      return valueOf(value.toUpperCase());
    } catch (final IllegalArgumentException e) {
      throw new KsqlException("Unknown format: " + value);
    }
  }
}
