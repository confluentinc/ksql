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

package io.confluent.ksql.test.utils;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;

public final class TestParsingUtil {

  private TestParsingUtil() {
  }

  public static ColumnRef parseColumnRef(final String columnString) {
    final String[] split = columnString.split("\\.", 2);
    if (split.length == 1) {
      return ColumnRef.withoutSource(ColumnName.of(split[0]));
    }

    return ColumnRef.of(
        SourceName.of(split[0]),
        ColumnName.of(split[1])
    );
  }

}
