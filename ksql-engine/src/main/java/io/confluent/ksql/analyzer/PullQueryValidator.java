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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.ResultMaterialization;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class PullQueryValidator implements QueryValidator {

  private static final String PUSH_PULL_QUERY_DOC_LINK = "https://cnfl.io/queries";

  public static final String NEW_QUERY_SYNTAX_SHORT_HELP = ""
      + "Refer to " + PUSH_PULL_QUERY_DOC_LINK + " for info on query types. "
      + "If you intended to issue a push query, resubmit with the EMIT CHANGES clause";

  public static final String NEW_QUERY_SYNTAX_ADDITIONAL_HELP = ""
      + "Query syntax in KSQL has changed. There are now two broad categories of queries:"
      + System.lineSeparator()
      + "- Pull queries: query the current state of the system, return a result, and terminate. "
      + System.lineSeparator()
      + "- Push queries: query the state of the system in motion and continue to output "
      + "results until they meet a LIMIT condition or are terminated by the user."
      + System.lineSeparator()
      + System.lineSeparator()
      + "'EMIT CHANGES' is used to to indicate a query is a push query. "
      + "To convert a pull query into a push query, which was the default behavior in older "
      + "versions of KSQL, add `EMIT CHANGES` to the end of the statement before any LIMIT clause."
      + System.lineSeparator()
      + System.lineSeparator()
      + "For example, the following are pull queries:"
      + System.lineSeparator()
      + "\t'SELECT * FROM X WHERE ROWKEY=Y;' (non-windowed table)"
      + System.lineSeparator()
      + "\t'SELECT * FROM X WHERE ROWKEY=Y AND WINDOWSTART>=Z;' (windowed table)"
      + System.lineSeparator()
      + System.lineSeparator()
      + "The following is a push query:"
      + System.lineSeparator()
      + "\t'SELECT * FROM X EMIT CHANGES;'"
      + System.lineSeparator()
      + System.lineSeparator()
      + "Note: Persistent queries, e.g. `CREATE TABLE AS ...`, have an implicit "
      + "`EMIT CHANGES`, but we recommend adding `EMIT CHANGES` to these statements.";

  private static final String NEW_QUERY_SYNTAX_LONG_HELP = ""
      + NEW_QUERY_SYNTAX_SHORT_HELP
      + System.lineSeparator()
      + System.lineSeparator()
      + NEW_QUERY_SYNTAX_ADDITIONAL_HELP;

  private static final List<Rule> RULES = ImmutableList.of(
      Rule.of(
          analysis -> analysis.getResultMaterialization() == ResultMaterialization.FINAL,
          "Pull queries don't support `EMIT CHANGES`."
      ),
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
          analysis -> analysis.getGroupByExpressions().isEmpty(),
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
          analysis -> analysis.getSelectColumnRefs().stream()
                  .map(ColumnRef::name)
                  .noneMatch(n -> n.equals(SchemaUtil.ROWTIME_NAME)),
          "Pull queries don't support ROWTIME in select columns."
      )
  );

  @Override
  public void validate(final Analysis analysis) {
    try {
      RULES.forEach(rule -> rule.check(analysis));
    } catch (final KsqlException e) {
      throw new KsqlException(e.getMessage() + " " + NEW_QUERY_SYNTAX_LONG_HELP, e);
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
