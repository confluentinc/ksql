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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PlanNode {

  private final PlanNodeId id;
  private final DataSourceType nodeOutputType;
  private final Optional<SourceName> sourceName;

  protected PlanNode(
      final PlanNodeId id,
      final DataSourceType nodeOutputType,
      final Optional<SourceName> sourceName
  ) {
    this.id = requireNonNull(id, "id");
    this.nodeOutputType = requireNonNull(nodeOutputType, "nodeOutputType");
    this.sourceName = requireNonNull(sourceName, "sourceName");
  }

  public final PlanNodeId getId() {
    return id;
  }

  public final DataSourceType getNodeOutputType() {
    return nodeOutputType;
  }

  public abstract LogicalSchema getSchema();

  public abstract List<PlanNode> getSources();

  public DataSourceNode getLeftmostSourceNode() {
    return Iterables.getOnlyElement(getSourceNodes().limit(1).collect(Collectors.toList()));
  }

  public Stream<DataSourceNode> getSourceNodes() {
    if (this instanceof DataSourceNode) {
      return Stream.of((DataSourceNode) this);
    }

    return getSources().stream()
        .flatMap(PlanNode::getSourceNodes);
  }

  protected abstract int getPartitions(KafkaTopicClient kafkaTopicClient);

  public abstract SchemaKStream<?> buildStream(PlanBuildContext buildContext);

  Optional<SourceName> getSourceName() {
    return sourceName;
  }

  /**
   * Call to resolve an {@link io.confluent.ksql.parser.tree.AllColumns} instance into a
   * corresponding set of columns.
   *
   * @param sourceName the name of the source
   * @return the list of columns.
   */
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName
  ) {
    return getSources().stream()
        .filter(s -> !sourceName.isPresent()
            || !s.getSourceName().isPresent()
            || sourceName.equals(s.getSourceName()))
        .flatMap(s -> s.resolveSelectStar(sourceName));
  }

  /**
   * Called to resolve the supplied {@code expression} into an expression that matches the nodes
   * schema.
   *
   * <p>{@link AggregateNode} and {@link FlatMapNode} replace UDAFs and UDTFs with synthetic column
   * names. Where a select is a UDAF or UDTF this method will return the appropriate synthetic
   * {@link io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp}
   *
   * @param idx the index of the select within the projection.
   * @param expression the expression to resolve.
   * @return the resolved expression.
   */
  public Expression resolveSelect(final int idx, final Expression expression) {
    return expression;
  }

  /**
   * Called to validate that columns referenced in the query are valid, i.e. they are known.
   *
   * @param requiredColumns the set of required columns.
   * @return any unknown columns.
   */
  protected Set<ColumnReferenceExp> validateColumns(final RequiredColumns requiredColumns) {
    return Iterables.getOnlyElement(getSources()).validateColumns(requiredColumns);
  }

  /**
   * Called to check that the query's projection contains the required key columns.
   *
   * <p>Only called for persistent queries.
   *
   * @param sinkName the name of the sink being created by the persistent query.
   * @param projection the query's projection.
   * @throws KsqlException if any key columns are missing from the projection.
   */
  void validateKeyPresent(final SourceName sinkName, final Projection projection) {
    getSources().forEach(s -> s.validateKeyPresent(sinkName, projection));
  }

  static void throwKeysNotIncludedError(
      final SourceName sinkName,
      final String type,
      final List<? extends Expression> requiredKeys
  ) {
    throwKeysNotIncludedError(sinkName, type, requiredKeys, true, false);
  }

  static void throwKeysNotIncludedError(
      final SourceName sinkName,
      final String type,
      final List<? extends Expression> requiredKeys,
      final boolean requireAll,
      final boolean synthetic
  ) {
    final String keyPostfix = requireAll && requiredKeys.size() > 1 ? "s" : "";
    final String types = type + (requiredKeys.size() == 1 ? "" : "s");

    final String joined = (requireAll ? GrammaticalJoiner.and() : GrammaticalJoiner.or())
        .join(requiredKeys);

    final String additional1 = synthetic
        ? "See " + DocumentationLinks.SYNTHETIC_JOIN_KEY_DOC_URL + "."
        : "";

    final String additional2 = synthetic
        ? System.lineSeparator()
        + Iterables.getOnlyElement(requiredKeys) + " was added as a synthetic key column "
        + "because the join criteria did not match any source column. "
        + "This expression must be included in the projection and may be aliased. "
        : "";

    throw new KsqlException("Key" + keyPostfix + " missing from projection (ie, SELECT). "
        + additional1
        + System.lineSeparator()
        + "The query used to build " + sinkName + " must include the " + types + " " + joined
        + " in its projection (eg, SELECT "
        + (requireAll
              ? GrammaticalJoiner.comma().join(requiredKeys)
              : requiredKeys.stream().findFirst().get())
        + "...)." + additional2
    );
  }

  @SuppressWarnings("UnstableApiUsage")
  static Stream<ColumnName> orderColumns(
      final List<Column> columns,
      final LogicalSchema schema
  ) {
    // When doing a `select *` key columns should be at the front of the column list
    // but are added at the back during processing for performance reasons. Furthermore,
    // the keys should be selected in the same order as they appear in the source.
    // Switch them around here:
    final ImmutableMap<ColumnName, Column> columnsByName = Maps.uniqueIndex(columns, Column::name);
    final Stream<Column> keys = schema.key().stream()
        .map(key -> columnsByName.get(key.name()))
        .filter(Objects::nonNull);

    final Stream<Column> windowBounds = columns.stream()
        .filter(c -> SystemColumns.isWindowBound(c.name()));

    final Stream<Column> values = columns.stream()
        .filter(c -> !SystemColumns.isWindowBound(c.name()))
        .filter(c -> !SystemColumns.isPseudoColumn(c.name()))
        .filter(c -> !schema.isKeyColumn(c.name()));

    return Streams.concat(keys, windowBounds, values).map(Column::name);
  }
}
