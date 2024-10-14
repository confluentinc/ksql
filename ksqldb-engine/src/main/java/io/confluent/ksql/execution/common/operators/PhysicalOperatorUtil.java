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

package io.confluent.ksql.execution.common.operators;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.common.QueryRow;
import java.util.List;

final class PhysicalOperatorUtil {

  private PhysicalOperatorUtil() {

  }

  static GenericRow getIntermediateRow(final QueryRow row, final boolean additionalColumnsNeeded) {

    if (!additionalColumnsNeeded) {
      return row.value();
    }

    final GenericKey key = row.key();
    final GenericRow value = row.value();

    final List<?> keyFields = key.values();

    value.ensureAdditionalCapacity(
        1 // ROWTIME
            + keyFields.size()
            + row.window().map(w -> 2).orElse(0)
    );

    value.append(row.rowTime());
    value.appendAll(keyFields);

    row.window().ifPresent(window -> {
      value.append(window.start().toEpochMilli());
      value.append(window.end().toEpochMilli());
    });

    return value;
  }

}
