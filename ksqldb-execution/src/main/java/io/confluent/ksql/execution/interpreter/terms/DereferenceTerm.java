package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import org.apache.kafka.connect.data.Struct;

public class DereferenceTerm implements Term {

  private final Term struct;
  private final String fieldName;
  private final SqlType resultType;

  public DereferenceTerm(final Term struct, final String fieldName, final SqlType resultType) {
    this.struct = struct;
    this.fieldName = fieldName;
    this.resultType = resultType;
  }

  public Object getValue(final TermEvaluationContext context) {
    return ((Struct) struct.getValue(context)).get(fieldName);
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }
}
