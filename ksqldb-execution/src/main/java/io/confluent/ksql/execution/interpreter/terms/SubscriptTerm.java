package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;

public class SubscriptTerm implements Term {

  private final Term baseObjectTerm;
  private final Term subscriptTerm;
  private final SubscriptFunction subscriptFunction;
  private final SqlType sqlType;

  public SubscriptTerm(final Term baseObjectTerm, final Term subscriptTerm,
      final SubscriptFunction subscriptFunction, final SqlType sqlType) {
    this.baseObjectTerm = baseObjectTerm;
    this.subscriptTerm = subscriptTerm;
    this.subscriptFunction = subscriptFunction;
    this.sqlType = sqlType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return subscriptFunction.access(baseObjectTerm.getValue(context),
        subscriptTerm.getValue(context));
  }

  @Override
  public SqlType getSqlType() {
    return sqlType;
  }

  public interface SubscriptFunction {
    Object access(Object o, Object subscript);
  }
}
