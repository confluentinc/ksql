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

package io.confluent.ksql.parser.json;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;

final class ColumnRefTestCase {
  static final ColumnRef COLUMN_REF = ColumnRef.of(SourceName.of("SOURCE"), ColumnName.of("COL"));
  static final String COLUMN_REF_TXT = "\"SOURCE.COL\"";

  static final ColumnRef COLUMN_REF_NO_SOURCE = ColumnRef.withoutSource(ColumnName.of("COL"));
  static final String COLUMN_REF_NO_SOURCE_TXT = "\"COL\"";

  static final ColumnRef COLUMN_REF_NEEDS_QUOTES =
      ColumnRef.of(SourceName.of("STREAM"), ColumnName.of("foo"));
  static final String COLUMN_REF_NEEDS_QUOTES_TXT = "\"`STREAM`.`foo`\"";
}
