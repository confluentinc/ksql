/*
 * Copyright 2020 Confluent Inc.
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
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility for constructing {@link QueryId}s - separate from {@code EngineExecutor} for
 * easy access to unit testing.
 */
public final class QueryIdUtil {
  private static final Pattern VALID_QUERY_ID = Pattern.compile("[A-Za-z0-9_]+");
  private static final ReservedQueryIdsPrefixes[] PREFIXES = ReservedQueryIdsPrefixes.values();

  public enum ReservedQueryIdsPrefixes {
    INSERT("INSERTQUERY_"),
    CTAS("CTAS_"),
    CSAS("CSAS_"),
    CST("CST_");

    private final String prefix;
    ReservedQueryIdsPrefixes(final String prefix) {
      this.prefix = prefix;
    }

    @Override
    public String toString() {
      return prefix;
    }
  }

  private QueryIdUtil() {
  }

  private static void validateWithQueryId(final String queryId) {
    Arrays.stream(PREFIXES).forEach(prefixId -> {
      if (queryId.startsWith(prefixId.toString())) {
        throw new KsqlException(String.format(
            "Query IDs must not start with a reserved query ID prefix (%s). "
                + "Got '%s'.",
            Arrays.stream(PREFIXES)
                .map(ReservedQueryIdsPrefixes::toString)
                .collect(Collectors.joining(", ")),
            queryId));
      }
    });

    if (!VALID_QUERY_ID.matcher(queryId).matches()) {
      throw new IllegalArgumentException(String.format(
          "Query IDs may contain only alphanumeric characters and '_'. "
              + "Got: '%s'", queryId));
    }

  }

  /**
   * Builds a {@link QueryId} for a physical plan specification.
   *
   * @param statement the statement that requires the query ID
   * @param engineContext  the context representing the current state of the engine
   * @param idGenerator generates query ids
   * @param outputNode  the logical plan
   * @param createOrReplaceEnabled whether or not the queryID can replace an existing one
   * @return the {@link QueryId} to be used
   */
  static QueryId buildId(
      final Statement statement,
      final EngineContext engineContext,
      final QueryIdGenerator idGenerator,
      final OutputNode outputNode,
      final boolean createOrReplaceEnabled,
      final Optional<String> withQueryId) {
    if (withQueryId.isPresent()) {
      final String queryId = withQueryId.get().toUpperCase();
      validateWithQueryId(queryId);
      return new QueryId(queryId);
    }
    if (statement instanceof CreateTable && ((CreateTable) statement).isSource()) {
      // Use the CST name as part of the QueryID
      final String suffix = ((CreateTable) statement).getName().text().toUpperCase()
          + "_" + idGenerator.getNext().toUpperCase();
      return new QueryId(ReservedQueryIdsPrefixes.CST + suffix);
    }

    if (!outputNode.getSinkName().isPresent()) {
      final String prefix =
          "transient_" + outputNode.getSource().getLeftmostSourceNode().getAlias().text() + "_";
      return new QueryId(prefix + Math.abs(ThreadLocalRandom.current().nextLong()));
    }

    final KsqlStructuredDataOutputNode structured = (KsqlStructuredDataOutputNode) outputNode;
    if (!structured.createInto()) {
      return new QueryId(ReservedQueryIdsPrefixes.INSERT + idGenerator.getNext());
    }

    final SourceName sink = outputNode.getSinkName().get();
    final Set<QueryId> queriesForSink = engineContext.getQueryRegistry().getQueriesWithSink(sink);
    if (queriesForSink.size() > 1) {
      throw new KsqlException("REPLACE for sink " + sink + " is not supported because there are "
          + "multiple queries writing into it: " + queriesForSink);
    } else if (!queriesForSink.isEmpty()) {
      if (!createOrReplaceEnabled) {
        final String type = outputNode.getNodeOutputType().getKsqlType().toLowerCase();
        throw new UnsupportedOperationException(
            String.format(
                "Cannot add %s '%s': A %s with the same name already exists",
                type,
                sink.text(),
                type));
      }
      return Iterables.getOnlyElement(queriesForSink);
    }

    final String suffix = outputNode.getId().toString().toUpperCase()
        + "_" + idGenerator.getNext().toUpperCase();
    return new QueryId(
        outputNode.getNodeOutputType() == DataSourceType.KTABLE
            ? ReservedQueryIdsPrefixes.CTAS + suffix
            : ReservedQueryIdsPrefixes.CSAS + suffix
    );
  }

}
