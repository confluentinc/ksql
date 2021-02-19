package io.confluent.ksql.execution.interpreter;

import io.confluent.ksql.GenericRow;

public class TermEvaluationContext {

  private final GenericRow row;

  public static TermEvaluationContext of(final GenericRow row) {
    return new TermEvaluationContext(row);
  }

  private TermEvaluationContext(final GenericRow row) {
    this.row = row;
  }

  public GenericRow getRow() {
    return row;
  }
}
