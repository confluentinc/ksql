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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Struct;

final class TableRowValidation {

  private TableRowValidation() {
  }

  interface Validator {

    void validate(
        LogicalSchema schema,
        Struct key,
        GenericRow value
    );
  }

  static void validate(
      final LogicalSchema schema,
      final Struct key,
      final GenericRow value
  ) {
    if (schema.metadata().size() != 1) {
      throw new IllegalArgumentException(
          "expected only " + SchemaUtil.ROWTIME_NAME + " meta columns, got: " + schema.metadata());
    }

    if (!schema.metadata().get(0).name().equals(SchemaUtil.ROWTIME_NAME)) {
      throw new IllegalArgumentException(
          "expected " + SchemaUtil.ROWTIME_NAME + ", got: " + schema.metadata().get(0));
    }

    final int expectedKeyCount = schema.key().size();
    final int actualKeyCount = key.schema().fields().size();
    if (actualKeyCount != expectedKeyCount) {
      throw new IllegalArgumentException("key column count mismatch."
          + " expected: " + expectedKeyCount
          + ", got: " + actualKeyCount
      );
    }

    final int expectedValueCount = schema.value().size();
    final int actualValueCount = value.getColumns().size();
    if (expectedValueCount != actualValueCount) {
      throw new IllegalArgumentException("value column count mismatch."
          + " expected: " + expectedValueCount
          + ", got: " + actualValueCount
      );
    }
  }
}
