/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.structured;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.util.ExpressionMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.kstream.ValueMapper;

class SelectValueMapper implements ValueMapper<GenericRow, GenericRow> {

  private final ImmutableList<SelectInfo> selects;
  private final ProcessingLogger processingLogger;

  SelectValueMapper(
      final List<SelectInfo> selects,
      final ProcessingLogger processingLogger
  ) {
    this.selects = ImmutableList.copyOf(requireNonNull(selects, "selects"));
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
  }

  List<SelectInfo> getSelects() {
    return selects;
  }

  @Override
  public GenericRow apply(final GenericRow row) {
    if (row == null) {
      return null;
    }

    final List<Object> newColumns = new ArrayList<>();

    for (int i = 0; i < selects.size(); i++) {
      newColumns.add(processColumn(i, row));
    }

    return new GenericRow(newColumns);
  }

  private Object processColumn(final int column, final GenericRow row) {
    final SelectInfo select = selects.get(column);

    try {
      return select.evaluator.evaluate(row);
    } catch (final Exception e) {
      final String errorMsg = String.format(
          "Error computing expression %s for column %s with index %d: %s",
          select.evaluator.getExpression(),
          select.fieldName,
          column,
          e.getMessage()
      );

      processingLogger.error(
          EngineProcessingLogMessageFactory.recordProcessingError(
              errorMsg,
              e,
              row
          )
      );
      return null;
    }
  }

  static final class SelectInfo {

    final String fieldName;
    final ExpressionMetadata evaluator;

    static SelectInfo of(final String fieldName, final ExpressionMetadata evaluator) {
      return new SelectInfo(fieldName, evaluator);
    }

    private SelectInfo(final String fieldName, final ExpressionMetadata evaluator) {
      this.fieldName = requireNonNull(fieldName, "fieldName");
      this.evaluator = requireNonNull(evaluator, "evaluator");
    }

    String getFieldName() {
      return fieldName;
    }

    SqlType getExpressionType() {
      return evaluator.getExpressionType();
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SelectInfo that = (SelectInfo) o;
      return Objects.equals(fieldName, that.fieldName)
          && Objects.equals(evaluator, that.evaluator);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldName, evaluator);
    }
  }
}
