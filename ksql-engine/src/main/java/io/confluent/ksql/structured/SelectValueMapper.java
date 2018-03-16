/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.structured;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.Pair;

class SelectValueMapper implements ValueMapper<GenericRow, GenericRow> {
  private static Logger log = LoggerFactory.getLogger(SelectValueMapper.class);

  private final GenericRowValueTypeEnforcer typeEnforcer;
  private final List<Pair<String, Expression>> expressionPairList;
  private final List<ExpressionMetadata> expressionEvaluators;

  SelectValueMapper(
      final GenericRowValueTypeEnforcer typeEnforcer,
      final List<Pair<String, Expression>> expressionPairList,
      final List<ExpressionMetadata> expressionEvaluators
  ) {
    this.typeEnforcer = typeEnforcer;
    this.expressionPairList = expressionPairList;
    this.expressionEvaluators = expressionEvaluators;
  }

  @Override
  public GenericRow apply(final GenericRow row) {
    if (row == null) {
      return row;
    }
    final List<Object> newColumns = new ArrayList<>();
    for (int i = 0; i < expressionPairList.size(); i++) {
      try {
        final int[] parameterIndexes = expressionEvaluators.get(i).getIndexes();
        final Kudf[] kudfs = expressionEvaluators.get(i).getUdfs();
        final Object[] parameterObjects = new Object[parameterIndexes.length];
        for (int j = 0; j < parameterIndexes.length; j++) {
          if (parameterIndexes[j] < 0) {
            parameterObjects[j] = kudfs[j];
          } else {
            parameterObjects[j] =
                typeEnforcer.enforceFieldType(parameterIndexes[j],
                    row.getColumns()
                        .get(parameterIndexes[j]));
          }
        }
        newColumns.add(
            expressionEvaluators.get(i).getExpressionEvaluator().evaluate(parameterObjects)
        );
      } catch (Exception e) {
        log.error(
            "Error calculating column with index {} : {}",
            i,
            expressionPairList.get(i).getLeft(),
            e
        );
        newColumns.add(null);
      }
    }
    return new GenericRow(newColumns);
  }
}
