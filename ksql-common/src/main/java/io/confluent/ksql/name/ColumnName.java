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

package io.confluent.ksql.name;

import com.google.errorprone.annotations.Immutable;

/**
 * The name of a column within a source.
 */
@Immutable
public final class ColumnName extends Name<ColumnName> {

  private static final String AGGREGATE_COLUMN_PREFIX = "KSQL_AGG_VARIABLE_";
  private static final String UDTF_COLUMN_PREFIX = "KSQL_UDTF_VARIABLE_";

  public static ColumnName aggregateColumn(final int idx) {
    return of(AGGREGATE_COLUMN_PREFIX + idx);
  }

  public static ColumnName udtfColumn(final int idx) {
    return of(UDTF_COLUMN_PREFIX + idx);
  }

  public static ColumnName of(final String name) {
    return new ColumnName(name);
  }

  private ColumnName(final String name) {
    super(name);
  }

  public boolean isAggregate() {
    return name.startsWith(AGGREGATE_COLUMN_PREFIX);
  }

}
