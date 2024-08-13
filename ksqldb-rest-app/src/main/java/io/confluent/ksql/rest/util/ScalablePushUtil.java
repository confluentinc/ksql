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

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public final class ScalablePushUtil {

  private static String LATEST_VALUE = "latest";

  private ScalablePushUtil() {

  }

  @SuppressWarnings({"BooleanExpressionComplexity", "CyclomaticComplexity"})
  public static boolean isScalablePushQuery(
      final Statement statement,
      final KsqlExecutionContext ksqlEngine,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overrides
  ) {
    if (!isPushV2Enabled(ksqlConfig, overrides)) {
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
    final boolean isLatest = isLatest(ksqlConfig, overrides);
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
        && upstreamQueries.size() == 1
        // ROWPARTITION and ROWOFFSET are not currently supported in SPQs
        && !containsDisallowedColumns(query);
  }

  private static boolean containsDisallowedColumns(final Query query) {
    return containsDisallowedColumnsInWhereClause(query)
        || containsDisallowedColumnsInSelectClause(query);
  }

  // this code is a duplicate of what's in PullQueryValidator, but this is intended as
  // we'll split the isDisallowedInPullOrScalableQueries boolean soon anyways
  private static boolean containsDisallowedColumnsInWhereClause(
      final Query query
  ) {

    final Optional<Expression> whereClause = query.getWhere();
    if (!whereClause.isPresent()) {
      return false;
    }

    return ColumnExtractor.extractColumns(whereClause.get())
        .stream()
        .map(ColumnReferenceExp::getColumnName)
        .anyMatch(SystemColumns::isDisallowedInPullOrScalablePushQueries);
  }

  private static boolean containsDisallowedColumnsInSelectClause(
      final Query query
  ) {
    return query.getSelect().getSelectItems()
        .stream()
        .filter(col -> col instanceof SingleColumn) //filter out select *
        .map(SingleColumn.class::cast)
        .map(SingleColumn::getExpression)
        .map(ColumnExtractor::extractColumns)
        .flatMap(Collection::stream)
        .map(ColumnReferenceExp::getColumnName)
        .anyMatch(SystemColumns::isDisallowedInPullOrScalablePushQueries);
  }

  private static boolean isPushV2Enabled(
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overrides
  ) {
    if (overrides.containsKey(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED)) {
      return Boolean.TRUE.equals(overrides.get(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED));
    } else {
      return ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED);
    }
  }

  private static boolean isLatest(
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overrides
  ) {
    if (overrides.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      return LATEST_VALUE.equals(overrides.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    } else if (overrides.containsKey(
        KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      return LATEST_VALUE.equals(
          overrides.get(KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    } else if (ksqlConfig.getKsqlStreamConfigProp(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).isPresent()) {
      return LATEST_VALUE.equals(
          ksqlConfig.getKsqlStreamConfigProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).orElse(null));
    } else {
      // Implicitly assume latest since this is the default for push queries in ksqlDB.
      return true;
    }
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
