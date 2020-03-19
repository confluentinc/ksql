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
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import java.util.List;

/**
 * Applies a table function on a row to get a list of values
 */
@Immutable
public class TableFunctionApplier {

  private final KsqlTableFunction tableFunction;
  private final ImmutableList<ExpressionMetadata> parameterExtractors;
  private final String errorMsg;

  public TableFunctionApplier(
      final KsqlTableFunction tableFunction,
      final List<ExpressionMetadata> parameterExtractors
  ) {
    this.tableFunction = requireNonNull(tableFunction);
    this.parameterExtractors = ImmutableList.copyOf(requireNonNull(parameterExtractors));
    this.errorMsg = "Table function " + tableFunction.name().text() + " threw an exception";
  }

  List<?> apply(
      final GenericRow row,
      final ProcessingLogger processingLogger
  ) {
    final Object[] args = new Object[parameterExtractors.size()];

    for (int i = 0; i < parameterExtractors.size(); i++) {
      args[i] = evalParam(row, processingLogger, i);
    }

    try {
      return tableFunction.apply(args);
    } catch (final Exception e) {
      processingLogger.error(RecordProcessingError.recordProcessingError(errorMsg, e, row));
      return null;
    }
  }

  private Object evalParam(
      final GenericRow row,
      final ProcessingLogger processingLogger,
      final int idx
  ) {
    return parameterExtractors.get(idx).evaluate(
        row,
        null,
        processingLogger,
        () -> "Failed to evaluate table function parameter " + idx
    );
  }
}
