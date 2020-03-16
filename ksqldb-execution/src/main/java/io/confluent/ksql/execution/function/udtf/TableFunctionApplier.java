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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.function.KsqlTableFunction;
import java.util.List;

/**
 * Applies a table function on a row to get a list of values
 */
@Immutable
public class TableFunctionApplier {

  private final KsqlTableFunction tableFunction;
  private final ImmutableList<ExpressionMetadata> expressionMetadataList;

  public TableFunctionApplier(
      final KsqlTableFunction tableFunction, final List<ExpressionMetadata> expressionMetadataList
  ) {
    this.tableFunction = requireNonNull(tableFunction);
    this.expressionMetadataList = ImmutableList.copyOf(requireNonNull(expressionMetadataList));
  }

  List<?> apply(final GenericRow row) {
    final Object[] args = new Object[expressionMetadataList.size()];
    int i = 0;
    for (final ExpressionMetadata expressionMetadata : expressionMetadataList) {
      args[i++] = expressionMetadata.evaluate(row);
    }
    return tableFunction.apply(args);
  }
}
