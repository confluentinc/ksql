package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;

public interface Term {

  Object getValue(TermEvaluationContext termEvaluationContext);

  SqlType getSqlType();

}
