package io.confluent.ksql.engine;

import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Set;

public class ScalablePushQueryExecutionUtil {

  static PersistentQueryMetadata findQuery(
      final EngineContext engineContext, final ImmutableAnalysis analysis) {

    final DataSource source = analysis.getFrom().getDataSource();
    final SourceName sourceName = source.getName();

    final Set<QueryId> queries = engineContext.getQueryRegistry().getQueriesWithSink(sourceName);

//    if (source.getDataSourceType() != DataSourceType.KTABLE) {
//      throw new KsqlException("Pull queries are not supported on streams."
//          + PullQueryValidator.PULL_QUERY_SYNTAX_HELP);
//    }

    if (queries.isEmpty()) {
      throw new IllegalStateException(
          "Scalable push queries require a query that has a sink. "
              + "Source Name: " + sourceName);
    }

    if (queries.size() > 1) {
      throw new IllegalStateException(
          "Scalable push queries on work on sources that have a single writer query. "
              + "Source Name: " + sourceName + " Queries: " + queries );
    }

    final QueryId queryId = Iterables.getOnlyElement(queries);

    return engineContext
        .getQueryRegistry()
        .getPersistentQuery(queryId)
        .orElseThrow(() -> new KsqlException("Persistent query has been stopped"));
  }
}
