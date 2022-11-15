/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde.unwrapped;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

final class Unwrapped {

  private Unwrapped() {
  }

  static Field getOnlyField(final Schema schema) {
    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("Expected STRUCT, " + "got: " + schema);
    }

    if (schema.fields().size() != 1) {
      throw new IllegalArgumentException("Expected single column, got: " + schema);
    }

    return schema.fields().get(0);
  }
}
