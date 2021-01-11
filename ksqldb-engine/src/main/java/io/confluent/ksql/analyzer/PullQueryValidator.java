/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.analyzer;

import static io.confluent.ksql.links.DocumentationLinks.PUSH_PULL_QUERY_DOC_LINK;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class PullQueryValidator implements QueryValidator {

  public static final String PULL_QUERY_SYNTAX_HELP = " "
      + "See " + PUSH_PULL_QUERY_DOC_LINK + " for more info."
      + System.lineSeparator()
      + "Add EMIT CHANGES if you intended to issue a push query.";

  private static final List<Rule> RULES = ImmutableList.of(
      Rule.of(
          analysis -> !analysis.getInto().isPresent(),
          "Pull queries don't support output to sinks."
      ),
      Rule.of(
          analysis -> !analysis.isJoin(),
          "Pull queries don't support JOIN clauses."
      ),
      Rule.of(
          analysis -> !analysis.getWindowExpression().isPresent(),
          "Pull queries don't support WINDOW clauses."
      ),
      Rule.of(
          analysis -> !analysis.getGroupBy().isPresent(),
          "Pull queries don't support GROUP BY clauses."
      ),
      Rule.of(
          analysis -> !analysis.getPartitionBy().isPresent(),
          "Pull queries don't support PARTITION BY clauses."
      ),
      Rule.of(
          analysis -> !analysis.getHavingExpression().isPresent(),
          "Pull queries don't support HAVING clauses."
      ),
      Rule.of(
          analysis -> !analysis.getLimitClause().isPresent(),
          "Pull queries don't support LIMIT clauses."
      ),
      Rule.of(
          analysis -> !analysis.getRefinementInfo().isPresent(),
          "Pull queries don't support EMIT clauses."
      )
  );

  @Override
  public void validate(final Analysis analysis) {
    try {
      RULES.forEach(rule -> rule.check(analysis));
    } catch (final KsqlException e) {
      throw new KsqlException(e.getMessage() + PULL_QUERY_SYNTAX_HELP, e);
    }
  }

  private static final class Rule {

    private final Predicate<Analysis> condition;
    private final String failureMsg;

    private static Rule of(final Predicate<Analysis> condition, final String failureMsg) {
      return new Rule(condition, failureMsg);
    }

    private Rule(final Predicate<Analysis> condition, final String failureMsg) {
      this.condition = Objects.requireNonNull(condition, "condition");
      this.failureMsg = Objects.requireNonNull(failureMsg, "failureMsg");
    }

    public void check(final Analysis analysis) {
      if (!condition.test(analysis)) {
        throw new KsqlException(failureMsg);
      }
    }
  }
}
