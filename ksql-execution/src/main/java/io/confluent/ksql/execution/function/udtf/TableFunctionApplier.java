/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.function.udtf;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlTableFunction;
import java.util.List;

/**
 * Applies a table function on a row to get a list of values
 */
@Immutable
public class TableFunctionApplier {
  private final KsqlTableFunction tableFunction;
  private final int argColumnIndex;

  public TableFunctionApplier(final KsqlTableFunction tableFunction, final int argColumnIndex) {
    this.tableFunction = tableFunction;
    this.argColumnIndex = argColumnIndex;
  }

  @SuppressWarnings("unchecked")
  List<Object> apply(final GenericRow row) {
    final List<Object> unexplodedValue = row.getColumnValue(argColumnIndex);
    return tableFunction.flatMap(unexplodedValue);
  }
}
