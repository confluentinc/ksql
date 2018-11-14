/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.testutils;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.PlanNode;
import java.util.List;

public final class AnalysisTestUtil {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private AnalysisTestUtil() {
  }

  public static Query parseQuery(final String queryStr, final MetaStore metaStore) {
    final List<PreparedStatement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    return (Query) statements.get(0).getStatement();
  }

  public static Analysis analyzeQuery(final String queryStr, final MetaStore metaStore) {
    final Query query = parseQuery(queryStr, metaStore);
    return analyzeQuery(queryStr, query, metaStore);
  }

  public static PlanNode buildLogicalPlan(final String queryStr, final MetaStore metaStore) {
    final Query query = parseQuery(queryStr, metaStore);
    final Analysis analysis = analyzeQuery(queryStr, query, metaStore);

    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(metaStore, metaStore, "");
    final AggregateAnalysis aggregateAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    return new LogicalPlanner(analysis, aggregateAnalysis, metaStore).buildPlan();
  }

  private static Analysis analyzeQuery(
      final String queryStr,
      final Query query,
      final MetaStore metaStore
  ) {
    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(metaStore, metaStore, "");
    return queryAnalyzer.analyze(queryStr, query);
  }
}
