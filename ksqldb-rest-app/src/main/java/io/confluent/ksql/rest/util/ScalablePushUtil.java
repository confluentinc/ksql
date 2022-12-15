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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class ScalablePushUtil {

  private static String STREAMS_AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
  private static String LATEST_VALUE = "latest";

  private ScalablePushUtil() {

  }

  @SuppressWarnings({"BooleanExpressionComplexity", "CyclomaticComplexity"})
  public static boolean isScalablePushQuery(
      final Statement statement,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overrides
  ) {
    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_SCALABLE_ENABLED)) {
      return false;
    }
    if (! (statement instanceof Query)) {
      return false;
    }
    final Query query = (Query) statement;
    final SourceFinder sourceFinder = new SourceFinder();
    sourceFinder.process(query.getFrom(), null);
    // It will be present if it's not a join, which we don't handle
    if (!sourceFinder.getSourceName().isPresent()) {
      return false;
    }
    // Find all of the writers to this particular source.
    final SourceName sourceName = sourceFinder.getSourceName().get();
    final Set<QueryId> upstreamQueries = ksqlEngine.getQueriesWithSink(sourceName);
    // See if the config or override have set the stream to be "latest"
    final boolean isLatest = overrides.containsKey(STREAMS_AUTO_OFFSET_RESET_CONFIG)
        ? LATEST_VALUE.equals(overrides.get(STREAMS_AUTO_OFFSET_RESET_CONFIG))
        : LATEST_VALUE.equals(ksqlConfig.getKsqlStreamConfigProp(STREAMS_AUTO_OFFSET_RESET_CONFIG)
            .orElse(null));
    // Cannot be a pull query, i.e. must be a push
    return !query.isPullQuery()
        // Group by is not supported
        && !query.getGroupBy().isPresent()
        // Windowing is not supported
        && !query.getWindow().isPresent()
        // Having clause is not supported
        && !query.getHaving().isPresent()
        // Partition by is not supported
        && !query.getPartitionBy().isPresent()
        // There must be an EMIT CHANGES clause
        && (query.getRefinement().isPresent()
            && query.getRefinement().get().getOutputRefinement() == OutputRefinement.CHANGES)
        // Must be reading from "latest"
        && isLatest
        // We only handle a single sink source at the moment from a CTAS/CSAS
        && upstreamQueries.size() == 1;
  }

  /**
   * Finds the source in the from clause
   */
  private static final class SourceFinder extends AstVisitor<Void, Void> {

    private SourceName sourceName;

    @Override
    protected Void visitAliasedRelation(final AliasedRelation node, final Void context) {
      process(node.getRelation(), context);
      return null;
    }

    @Override
    protected Void visitTable(final Table node, final Void context) {
      sourceName = node.getName();
      return null;
    }

    public Optional<SourceName> getSourceName() {
      return Optional.ofNullable(sourceName);
    }
  }
}
