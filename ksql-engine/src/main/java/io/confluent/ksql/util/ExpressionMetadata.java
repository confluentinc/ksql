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

package io.confluent.ksql.util;

import io.confluent.ksql.function.udf.Kudf;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public class ExpressionMetadata {

  private final IExpressionEvaluator expressionEvaluator;
  private final int[] indexes;
  private final Kudf[] udfs;
  private final Schema expressionType;

  public ExpressionMetadata(IExpressionEvaluator expressionEvaluator, int[] indexes, Kudf[] udfs,
                            Schema expressionType) {
    this.expressionEvaluator = expressionEvaluator;
    this.indexes = indexes;
    this.udfs = udfs;
    this.expressionType = expressionType;
  }

  public IExpressionEvaluator getExpressionEvaluator() {
    return expressionEvaluator;
  }

  public int[] getIndexes() {
    final int [] result = new int[indexes.length];
    System.arraycopy(indexes, 0, result, 0, indexes.length);
    return result;
  }

  public Kudf[] getUdfs() {
    final Kudf[] result = new Kudf[udfs.length];
    System.arraycopy(udfs, 0, result, 0, udfs.length);
    return result;
  }

  public Schema getExpressionType() {
    return expressionType;
  }
}
