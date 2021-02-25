/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
