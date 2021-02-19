package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.BooleanTerm;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;

public class ColumnReferenceTerm implements Term {

  private final int rowIndex;
  private final SqlType sqlType;

  public ColumnReferenceTerm(
      final int rowIndex,
      final SqlType sqlType
  ) {
    this.rowIndex = rowIndex;
    this.sqlType = sqlType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return context.getRow().get(rowIndex);
  }

  @Override
  public SqlType getSqlType() {
    return sqlType;
  }

  public static ColumnReferenceTerm create(
      final int rowIndex,
      final SqlType sqlType
  ) {
    if (sqlType.baseType() == SqlBaseType.BOOLEAN) {
      return new BooleanColumnReferenceTerm(rowIndex, sqlType);
    }
    return new ColumnReferenceTerm(rowIndex, sqlType);
  }

  public static class BooleanColumnReferenceTerm extends ColumnReferenceTerm implements BooleanTerm {

    public BooleanColumnReferenceTerm(final int rowIndex, final SqlType sqlType) {
      super(rowIndex, sqlType);
    }

    @Override
    public Boolean getBoolean(final TermEvaluationContext context) {
      return (Boolean) getValue(context);
    }
  }
}
