package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.codegen.helpers.MapBuilder;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Map;

public class CreateMapTerm implements Term {

  private final Map<Term, Term> mapTerms;
  private final SqlType resultType;

  public CreateMapTerm(final Map<Term, Term> mapTerms, final SqlType resultType) {
    this.mapTerms = mapTerms;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final MapBuilder builder = new MapBuilder(mapTerms.size());
    for (Map.Entry<Term, Term> entry : mapTerms.entrySet()) {
      builder.put(entry.getKey().getValue(context), entry.getValue().getValue(context));
    }
    return builder.build();
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }
}
