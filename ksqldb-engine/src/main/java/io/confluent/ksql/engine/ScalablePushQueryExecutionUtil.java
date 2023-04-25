/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.engine;

import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Set;

public final class ScalablePushQueryExecutionUtil {

  private ScalablePushQueryExecutionUtil() {

  }

  static PersistentQueryMetadata findQuery(
      final EngineContext engineContext, final ImmutableAnalysis analysis) {

    final DataSource source = analysis.getFrom().getDataSource();
    final SourceName sourceName = source.getName();

    final Set<QueryId> queries = engineContext.getQueryRegistry().getQueriesWithSink(sourceName);

    if (queries.isEmpty()) {
      throw new IllegalStateException(
          "Scalable push queries require a query that has a sink. "
              + "Source Name: " + sourceName);
    }

    if (queries.size() > 1) {
      throw new IllegalStateException(
          "Scalable push queries only work on sources that have a single writer query. "
              + "Source Name: " + sourceName + " Queries: " + queries);
    }

    final QueryId queryId = Iterables.getOnlyElement(queries);

    return engineContext
        .getQueryRegistry()
        .getPersistentQuery(queryId)
        .orElseThrow(() -> new KsqlException("Persistent query has been stopped: " + queryId));
  }
}
