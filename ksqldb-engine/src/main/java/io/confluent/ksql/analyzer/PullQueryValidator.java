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
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.name.Name;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PullQueryValidator implements QueryValidator {

  public static final String PULL_QUERY_SYNTAX_HELP = " "
      + "See " + PUSH_PULL_QUERY_DOC_LINK + " for more info."
      + System.lineSeparator()
      + "Add EMIT CHANGES if you intended to issue a push query.";

  private static final String PULL_QUERY_LIMIT_CLAUSE_ERROR_IF_DISABLED
      = "LIMIT clause in pull queries is currently disabled. "
      + "You can enable them by setting ksql.query.pull.limit.clause.enabled=true.";

  private static final String PULL_QUERY_LIMIT_CLAUSE_ERROR_IF_NEGATIVE
      = "Pull queries don't support negative integers in the LIMIT clause.";

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
          PullQueryValidator::validateLimitClause
      ),
      Rule.of(
          analysis -> !analysis.getRefinementInfo().isPresent(),
          "Pull queries don't support EMIT clauses."
      ),
      Rule.of(
          PullQueryValidator::disallowedColumnNameInSelectClause
      ),
      Rule.of(
          PullQueryValidator::disallowedColumnNameInWhereClause
      )
  );

  @Override
  public void validate(final Analysis analysis) {
    try {
      RULES.forEach(rule -> rule.check(analysis));
    } catch (final KsqlException e) {
      throw new KsqlException(e.getMessage() + PULL_QUERY_SYNTAX_HELP, e);
    }
    QueryValidatorUtil.validateNoUserColumnsWithSameNameAsPseudoColumns(analysis);
  }

  private static Optional<String> validateLimitClause(final Analysis analysis) {
    if (!analysis.getPullLimitClauseEnabled() && analysis.getLimitClause().isPresent()) {
      return Optional.of(PULL_QUERY_LIMIT_CLAUSE_ERROR_IF_DISABLED);
    } else if (analysis.getLimitClause().isPresent()
            && analysis.getLimitClause().getAsInt() < 0) {
      return Optional.of(PULL_QUERY_LIMIT_CLAUSE_ERROR_IF_NEGATIVE);
    } else {
      return Optional.empty();
    }
  }

  private static Optional<String> disallowedColumnNameInSelectClause(final Analysis analysis) {

    final String disallowedColumns =  analysis.getSelectItems()
        .stream()
        .filter(col -> col instanceof SingleColumn) //filter out select *
        .map(SingleColumn.class::cast)
        .map(SingleColumn::getExpression)
        .map(ColumnExtractor::extractColumns)
        .flatMap(Collection::stream)
        .map(ColumnReferenceExp::getColumnName)
        .filter(SystemColumns::isDisallowedInPullOrScalablePushQueries)
        .map(Name::toString)
        .collect(Collectors.joining(", "));

    if (disallowedColumns.length() != 0) {
      final String message = "Pull queries don't support the following columns in SELECT clauses: "
          + disallowedColumns + "\n";
      return Optional.of(message);
    }

    return Optional.empty();
  }

  private static Optional<String> disallowedColumnNameInWhereClause(final Analysis analysis) {
    final Optional<Expression> expression = analysis.getWhereExpression();

    if (!expression.isPresent()) {
      return Optional.empty();
    }

    final String disallowedColumns = ColumnExtractor.extractColumns(expression.get())
        .stream()
        .map(ColumnReferenceExp::getColumnName)
        .filter(SystemColumns::isDisallowedInPullOrScalablePushQueries)
        .map(Name::toString)
        .collect(Collectors.joining(", "));

    if (disallowedColumns.length() != 0) {
      final String message = "Pull queries don't support the following columns in WHERE clauses: "
          + disallowedColumns + "\n";
      return Optional.of(message);
    }

    return Optional.empty();
  }

  private static final class Rule {

    private final Function<Analysis, Optional<String>> potentialErrorMessageGenerator;

    private static Rule of(final Predicate<Analysis> condition, final String failureMsg) {
      final Function<Analysis, Optional<String>> potentialErrorMessageGenerator = (analysis) ->
          !condition.test(analysis) ? Optional.of(failureMsg) : Optional.empty();

      return new Rule(potentialErrorMessageGenerator);
    }

    private static Rule of(
        final Function<Analysis, Optional<String>> potentialErrorMessageGenerator) {
      return new Rule(potentialErrorMessageGenerator);
    }

    private Rule(final Function<Analysis, Optional<String>> potentialErrorMessageGenerator) {
      this.potentialErrorMessageGenerator =
          Objects.requireNonNull(
              potentialErrorMessageGenerator,
              "potentialErrorMessageGenerator"
          );
    }

    public void check(final Analysis analysis) {
      final Optional<String> exceptionMessage = potentialErrorMessageGenerator.apply(analysis);

      if (exceptionMessage.isPresent()) {
        throw new KsqlException(exceptionMessage.get());
      }
    }
  }
}
