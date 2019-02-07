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

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.KsqlParserTestUtil;

public final class AnalysisTestUtil {

  private AnalysisTestUtil() {
  }

  public static Analysis analyzeQuery(final String queryStr, final MetaStore metaStore) {
    final PreparedStatement<?> statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore);

    final Analysis analysis = new Analysis();
    final Analyzer analyzer = new Analyzer(queryStr, analysis, metaStore, "");
    analyzer.process(statement.getStatement(), new AnalysisContext(null));
    return analysis;
  }
}
