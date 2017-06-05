/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

import io.confluent.ksql.function.udf.Kudf;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public class ExpressionMetadata {

  final IExpressionEvaluator expressionEvaluator;
  final int[] indexes;
  final Kudf[] udfs;
  final Schema expressionType;

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
    return indexes;
  }

  public Kudf[] getUdfs() {
    return udfs;
  }

  public Schema getExpressionType() {
    return expressionType;
  }
}
