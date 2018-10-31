/*
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

package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.udf.Kudf;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public class ExpressionMetadata {

  private final IExpressionEvaluator expressionEvaluator;
  private final List<Integer> indexes;
  private final List<Kudf> udfs;
  private final Schema expressionType;
  private final GenericRowValueTypeEnforcer typeEnforcer;
  private final Object[] parameters;

  public ExpressionMetadata(
      final IExpressionEvaluator expressionEvaluator,
      final List<Integer> indexes,
      final List<Kudf> udfs,
      final Schema expressionType,
      final Schema schema) {
    this.expressionEvaluator = Objects.requireNonNull(expressionEvaluator, "expressionEvaluator");
    this.indexes = Collections.unmodifiableList(Objects.requireNonNull(indexes, "indexes"));
    this.udfs = Collections.unmodifiableList(Objects.requireNonNull(udfs, "udfs"));
    this.expressionType = Objects.requireNonNull(expressionType, "expressionType");
    this.typeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.parameters = new Object[indexes.size()];
  }

  public List<Integer> getIndexes() {
    return indexes;
  }

  public List<Kudf> getUdfs() {
    return udfs;
  }

  public Schema getExpressionType() {
    return expressionType;
  }

  public Object evaluate(final GenericRow row) {
    try {
      return expressionEvaluator.evaluate(getParameters(row));
    } catch (InvocationTargetException e) {
      throw new KsqlException(e.getMessage(), e);
    }
  }

  private Object[] getParameters(final GenericRow row) {
    for (int idx = 0; idx < indexes.size(); idx++) {
      final int paramIndex = indexes.get(idx);
      if (paramIndex < 0) {
        parameters[idx] = udfs.get(idx);
      } else {
        parameters[idx] = typeEnforcer
            .enforceFieldType(paramIndex, row.getColumns().get(paramIndex));
      }
    }
    return parameters;
  }
}
