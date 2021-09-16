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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
      ),
      Rule.of(
          analysis -> !disallowedColumnNameInSelectClause(analysis),
          "Pull queries don't support ROWPARTITION or ROWOFFSET in SELECT clauses."
      ),
      Rule.of(
          analysis -> !disallowedColumnNameInWhereClause(analysis),
          "Pull queries don't support ROWPARTITION or ROWOFFSET in WHERE clauses."
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

  private static boolean disallowedColumnNameInSelectClause(final Analysis analysis) {

//    List<Expression> list = analysis.getSelectItems().stream().map(SingleColumn.class::cast).map(SingleColumn::getExpression).collect(Collectors.toList());
//    for (Expression s : list) {
//      Set<? extends ColumnReferenceExp> set = ColumnExtractor.extractColumns(s);
//      System.out.println("hi");
//    }
//    return false;

//    return analysis.getSelectItems()
//        .stream()
//        .map(SingleColumn.class::cast)
//        .map(SingleColumn::getExpression)
//        .map(ColumnExtractor::extractColumns)
//        .flatMap(Collection::stream)
//        .map(ColumnReferenceExp::getColumnName)
//        .anyMatch(name -> disallowedInPullQueries(name, analysis.getKsqlConfig()));
  }

  private static boolean disallowedColumnNameInWhereClause(final Analysis analysis) {
    final Optional<Expression> expression = analysis.getWhereExpression();

    if (!expression.isPresent()) {
      return false;
    }

//    Set<ColumnReferenceExp> set = ColumnExtractor.extractColumns(expression.get());

    return ColumnExtractor.extractColumns(expression.get())
        .stream()
        .map(ColumnReferenceExp::getColumnName)
        .anyMatch(name -> disallowedInPullQueries(name, analysis.getKsqlConfig()));
  }

  private static boolean disallowedInPullQueries(
      final ColumnName columnName,
      final KsqlConfig ksqlConfig
  ) {
    final int pseudoColumnVersion = SystemColumns.getPseudoColumnVersionFromConfig(ksqlConfig);
    return SystemColumns.disallowedForPullQueries(columnName, pseudoColumnVersion);
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
