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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction.LazyWhenClause;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.Pair;
import java.util.List;
import java.util.Optional;

public class SearchedCaseTerm implements Term {

  private final List<Pair<Term, Term>> operandResultTerms;
  private final Optional<Term> defaultValue;
  private final SqlType resultType;

  public SearchedCaseTerm(
      final List<Pair<Term, Term>> operandResultTerms,
      final Optional<Term> defaultValue,
      final SqlType resultType) {
    this.operandResultTerms = operandResultTerms;
    this.defaultValue = defaultValue;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final List<LazyWhenClause<Object>> lazyWhenClause = operandResultTerms
        .stream()
        .map(pair -> SearchedCaseFunction.whenClause(
            () -> (Boolean) pair.getLeft().getValue(context),
            () -> pair.getRight().getValue(context)))
        .collect(ImmutableList.toImmutableList());
    return SearchedCaseFunction.searchedCaseFunction(lazyWhenClause,
        () -> defaultValue.map(term -> term.getValue(context)).orElse(null));
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }
}
