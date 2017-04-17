/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import io.confluent.kql.function.udf.KUDF;

public class ExpressionMetadata {

  final IExpressionEvaluator expressionEvaluator;
  final int[] indexes;
  final KUDF[] udfs;
  final Schema expressionType;

  public ExpressionMetadata(IExpressionEvaluator expressionEvaluator, int[] indexes, KUDF[] udfs,
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

  public KUDF[] getUdfs() {
    return udfs;
  }

  public Schema getExpressionType() {
    return expressionType;
  }
}
