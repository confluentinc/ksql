/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.testutils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlParserTestUtil;
import java.util.List;
import java.util.Optional;

public final class AnalysisTestUtil {

  private AnalysisTestUtil() {
  }

  public static OutputNode buildLogicalPlan(
      final KsqlConfig ksqlConfig,
      final String queryStr,
      final MetaStore metaStore
  ) {
    final boolean pullLimitClauseEnabled =
            ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED);

    final Analyzer analyzer = new Analyzer(queryStr, metaStore, pullLimitClauseEnabled);

    final LogicalPlanner logicalPlanner =
        new LogicalPlanner(ksqlConfig, analyzer.analysis, metaStore);

    return logicalPlanner.buildPersistentLogicalPlan();
  }

  private static class Analyzer {

    private final Analysis analysis;

    private Analyzer(
        final String queryStr,
        final MetaStore metaStore,
        final boolean pullLimitClauseEnabled
    ) {
      final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(
          metaStore, "", pullLimitClauseEnabled);
      final Statement statement =
          parseStatement(queryStr, metaStore);
      final Query query = statement instanceof QueryContainer
          ? ((QueryContainer) statement).getQuery()
          : (Query) statement;

      final Optional<Sink> sink = statement instanceof QueryContainer
          ? Optional.of(((QueryContainer)statement).getSink())
          : Optional.empty();

      this.analysis = queryAnalyzer.analyze(query, sink);
    }

    private static Statement parseStatement(
        final String queryStr,
        final MetaStore metaStore
    ) {
      final List<PreparedStatement<?>> statements =
          KsqlParserTestUtil.buildAst(queryStr, metaStore);
      assertThat(statements, hasSize(1));
      return statements.get(0).getStatement();
    }
  }
}
