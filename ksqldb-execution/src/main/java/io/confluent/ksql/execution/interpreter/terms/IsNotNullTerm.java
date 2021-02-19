package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.BooleanTerm;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;

public class IsNotNullTerm implements BooleanTerm {

  private final Term term;

  public IsNotNullTerm(final Term term) {
    this.term = term;
  }

  @Override
  public Boolean getValue(final TermEvaluationContext context) {
    return getBoolean(context);
  }

  @Override
  public Boolean getBoolean(final TermEvaluationContext context) {
    return term.getValue(context) != null;
  }

  @Override
  public SqlType getSqlType() {
    return SqlTypes.BOOLEAN;
  }
}
