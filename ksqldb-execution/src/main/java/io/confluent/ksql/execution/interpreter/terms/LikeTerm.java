package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.codegen.helpers.LikeEvaluator;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;

public class LikeTerm implements Term {

  private final Term patternString;
  private final Term valueString;
  private final Optional<Character> escapeChar;

  public LikeTerm(
      final Term patternString,
      final Term valueString,
      final Optional<Character> escapeChar
  ) {
    this.patternString = patternString;
    this.valueString = valueString;
    this.escapeChar = escapeChar;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return escapeChar.map(
        character -> LikeEvaluator.matches(
            (String) valueString.getValue(context), (String) patternString.getValue(context),
            character))
        .orElseGet(() -> LikeEvaluator.matches(
            (String) valueString.getValue(context), (String) patternString.getValue(context)));
  }

  @Override
  public SqlType getSqlType() {
    return SqlTypes.STRING;
  }
}
