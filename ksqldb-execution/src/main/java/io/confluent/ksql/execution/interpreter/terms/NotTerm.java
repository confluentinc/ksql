package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.BooleanTerm;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;

public class NotTerm implements BooleanTerm {

  private final BooleanTerm term;

  public NotTerm(final BooleanTerm term) {
    this.term = term;
  }

  @Override
  public Boolean getBoolean(final TermEvaluationContext context) {
    return !term.getBoolean(context);
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return getBoolean(context);
  }

  @Override
  public SqlType getSqlType() {
    return SqlTypes.BOOLEAN;
  }
}
