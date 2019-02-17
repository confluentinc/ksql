/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.testutils;

import io.confluent.ksql.analyzer.AggregateAnalysisResult;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.PlanNode;
import java.util.List;

public final class AnalysisTestUtil {

  private AnalysisTestUtil() {
  }

  public static Analysis analyzeQuery(final String queryStr, final MetaStore metaStore) {
    return new Analyzer(queryStr, metaStore).analysis;
  }

  public static PlanNode buildLogicalPlan(final String queryStr, final MetaStore metaStore) {
    final Analyzer analyzer = new Analyzer(queryStr, metaStore);

    final LogicalPlanner logicalPlanner = new LogicalPlanner(
        analyzer.analysis,
        analyzer.aggregateAnalys(),
        metaStore);

    return logicalPlanner.buildPlan();
  }

  private static class Analyzer {
    private final Query query;
    private final Analysis analysis;
    private final QueryAnalyzer queryAnalyzer;

    private Analyzer(final String queryStr, final MetaStore metaStore) {
      this.queryAnalyzer = new QueryAnalyzer(metaStore, "");
      this.query = parseQuery(queryStr, metaStore);
      this.analysis = queryAnalyzer.analyze(queryStr, query);
    }

    private static Query parseQuery(final String queryStr, final MetaStore metaStore) {
      final List<PreparedStatement<?>> statements = KsqlParserTestUtil.buildAst(queryStr, metaStore);
      return (Query) statements.get(0).getStatement();
    }

    AggregateAnalysisResult aggregateAnalys() {
      return queryAnalyzer.analyzeAggregate(query, analysis);
    }
  }
}
