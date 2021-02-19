package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.codegen.helpers.ArrayBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;

public class CreateArrayTerm implements Term {

  private final List<Term> arrayTerms;
  private final SqlType resultType;

  public CreateArrayTerm(final List<Term> arrayTerms, final SqlType resultType) {
    this.arrayTerms = arrayTerms;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final ArrayBuilder build = new ArrayBuilder(arrayTerms.size());
    for (Term term : arrayTerms) {
      build.add(term.getValue(context));
    }
    return build.build();
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }
}
