package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;

public class CastTerm implements Term {

  private final Term term;
  private final SqlType sqlType;
  private final CastFunction castFunction;

  public CastTerm(
      final Term term,
      final SqlType sqlType,
      final CastFunction castFunction
  ) {
    this.term = term;
    this.sqlType = sqlType;
    this.castFunction = castFunction;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final Object value = term.getValue(context);
    if (value == null) {
      return null;
    }
    return castFunction.cast(value);
  }

  @Override
  public SqlType getSqlType() {
    return sqlType;
  }

  public interface CastFunction {
    Object cast(Object object);
  }
}
