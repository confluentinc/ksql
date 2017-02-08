/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.codehaus.commons.compiler.IExpressionEvaluator;

import io.confluent.kql.function.udf.KUDF;

public class ExpressionMetadata {

  final IExpressionEvaluator expressionEvaluator;
  final int[] indexes;
  final KUDF[] udfs;

  public ExpressionMetadata(IExpressionEvaluator expressionEvaluator, int[] indexes, KUDF[] udfs) {
    this.expressionEvaluator = expressionEvaluator;
    this.indexes = indexes;
    this.udfs = udfs;
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
}
