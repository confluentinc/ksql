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

import io.confluent.ksql.function.udf.Kudf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public class ExpressionMetadata {

  private final IExpressionEvaluator expressionEvaluator;
  private final List<Integer> indexes;
  private final List<Kudf> udfs;
  private final Schema expressionType;

  public ExpressionMetadata(
      final IExpressionEvaluator expressionEvaluator,
      final List<Integer> indexes,
      final List<Kudf> udfs,
      final Schema expressionType) {
    this.expressionEvaluator = expressionEvaluator;
    this.indexes = Collections.unmodifiableList(new ArrayList<>(indexes));;
    this.udfs = Collections.unmodifiableList(new ArrayList<>(udfs));
    this.expressionType = expressionType;
  }

  public IExpressionEvaluator getExpressionEvaluator() {
    return expressionEvaluator;
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
}
