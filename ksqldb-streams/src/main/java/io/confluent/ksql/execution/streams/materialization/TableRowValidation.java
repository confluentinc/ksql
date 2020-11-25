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

package io.confluent.ksql.execution.streams.materialization;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;

final class TableRowValidation {

  private TableRowValidation() {
  }

  interface Validator {

    void validate(
        LogicalSchema schema,
        GenericKey key,
        GenericRow value
    );
  }

  static void validate(
      final LogicalSchema schema,
      final GenericKey key,
      final GenericRow value
  ) {
    final int expectedKeyCount = schema.key().size();
    final int actualKeyCount = key.values().size();
    if (actualKeyCount != expectedKeyCount) {
      throw new IllegalArgumentException("key column count mismatch."
          + " expected: " + expectedKeyCount
          + ", got: " + actualKeyCount
      );
    }

    final int expectedValueCount = schema.value().size();
    final int actualValueCount = value.size();
    if (expectedValueCount != actualValueCount) {
      throw new IllegalArgumentException("value column count mismatch."
          + " expected: " + expectedValueCount
          + ", got: " + actualValueCount
      );
    }
  }
}
