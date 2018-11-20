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

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import java.util.List;

public final class AnalysisTestUtil {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private AnalysisTestUtil() {
  }

  public static List<PreparedStatement<?>> getPreparedStatements(
      final String queryStr,
      final MetaStore metaStore
  ) {
    return KSQL_PARSER.buildAst(queryStr, metaStore);
  }

  public static Analysis analyzeQuery(final String queryStr, final MetaStore metaStore) {
    final List<PreparedStatement<?>> statements =
        getPreparedStatements(queryStr, metaStore);

    final Analysis analysis = new Analysis();
    final Analyzer analyzer = new Analyzer(queryStr, analysis, metaStore, "");
    analyzer.process(statements.get(0).getStatement(), new AnalysisContext(null));
    return analysis;
  }
}
