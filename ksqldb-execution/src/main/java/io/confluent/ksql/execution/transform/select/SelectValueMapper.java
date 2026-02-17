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

package io.confluent.ksql.execution.transform.select;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class SelectValueMapper<K> {

  private final ImmutableList<SelectInfo> selects;

  SelectValueMapper(final List<SelectInfo> selects) {
    this.selects = ImmutableList.copyOf(requireNonNull(selects, "selects"));
  }

  List<SelectInfo> getSelects() {
    return selects;
  }

  public KsqlTransformer<K, GenericRow> getTransformer(
      final ProcessingLogger processingLogger
  ) {
    return new SelectMapper<>(selects, processingLogger);
  }

  public static final class SelectInfo {

    final ColumnName fieldName;
    final ExpressionEvaluator evaluator;

    static SelectInfo of(final ColumnName fieldName, final ExpressionEvaluator evaluator) {
      return new SelectInfo(fieldName, evaluator);
    }

    private SelectInfo(final ColumnName fieldName, final ExpressionEvaluator evaluator) {
      this.fieldName = requireNonNull(fieldName, "fieldName");
      this.evaluator = requireNonNull(evaluator, "evaluator");
    }

    public ColumnName getFieldName() {
      return fieldName;
    }

    ExpressionEvaluator getEvaluator() {
      return evaluator;
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

  private static final class SelectMapper<K> implements KsqlTransformer<K, GenericRow> {

    private final ImmutableList<SelectInfo> selects;
    private final ProcessingLogger processingLogger;

    private SelectMapper(
        final ImmutableList<SelectInfo> selects,
        final ProcessingLogger processingLogger
    ) {
      this.selects = requireNonNull(selects, "selects");
      this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    }

    @Override
    public GenericRow transform(
        final K readOnlyKey,
        final GenericRow value
    ) {
      if (value == null) {
        return null;
      }

      final GenericRow row = new GenericRow(selects.size());

      for (int i = 0; i < selects.size(); i++) {
        row.append(processColumn(i, value));
      }

      return row;
    }

    private Object processColumn(final int column, final GenericRow row) {
      final SelectInfo select = selects.get(column);

      final Supplier<String> errorMsgSupplier = () ->
          "Error computing expression " + select.evaluator.getExpression()
              + " for column " + select.fieldName.toString(FormatOptions.noEscape())
              + " with index " + column;

      return select.evaluator.evaluate(row, null, processingLogger, errorMsgSupplier);
    }
  }
}
