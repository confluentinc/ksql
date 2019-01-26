/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.structured;

import io.confluent.common.logging.StructuredLogger;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.util.ExpressionMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SelectValueMapper implements ValueMapper<GenericRow, GenericRow> {
  private final List<String> selectFieldNames;
  private final List<ExpressionMetadata> expressionEvaluators;
  private final StructuredLogger processingLogger;

  SelectValueMapper(
      final List<String> selectFieldNames,
      final List<ExpressionMetadata> expressionEvaluators,
      final StructuredLogger processingLogger
  ) {
    this.selectFieldNames = Objects.requireNonNull(selectFieldNames);
    this.expressionEvaluators = Objects.requireNonNull(expressionEvaluators);
    this.processingLogger = Objects.requireNonNull(processingLogger);

    if (selectFieldNames.size() != expressionEvaluators.size()) {
      throw new IllegalArgumentException("must have field names for all expressions");
    }
  }

  @Override
  public GenericRow apply(final GenericRow row) {
    if (row == null) {
      return null;
    }

    final List<Object> newColumns = new ArrayList<>();
    for (int i = 0; i < selectFieldNames.size(); i++) {
      newColumns.add(processColumn(i, row));
    }
    return new GenericRow(newColumns);
  }

  private Object processColumn(final int column, final GenericRow row) {
    try {
      return expressionEvaluators
          .get(column)
          .evaluate(row);
    } catch (final Exception e) {
      final String errorMsg = String.format(
          "Error computing expression %s for column %s with index %d: %s",
          expressionEvaluators.get(column).getExpression(),
          selectFieldNames.get(column),
          column,
          e.getMessage());
      processingLogger.error(
          EngineProcessingLogMessageFactory.recordProcessingError(errorMsg, row));
      return null;
    }
  }
}
