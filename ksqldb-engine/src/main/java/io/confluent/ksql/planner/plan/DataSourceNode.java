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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKSourceFactory;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
public class DataSourceNode extends PlanNode {

  private static final String SOURCE_OP_NAME = "Source";

  private final DataSource dataSource;
  private final SchemaKStreamFactory schemaKStreamFactory;
  private final LogicalSchema schema;
  private final boolean isWindowed;

  public DataSourceNode(
      final PlanNodeId id,
      final DataSource dataSource,
      final SourceName alias,
      final boolean isWindowed
  ) {
    this(id, dataSource, alias, SchemaKSourceFactory::buildSource, isWindowed);
  }

  DataSourceNode(
      final PlanNodeId id,
      final DataSource dataSource,
      final SourceName alias,
      final SchemaKStreamFactory schemaKStreamFactory,
      final boolean isWindowed
  ) {
    super(id, dataSource.getDataSourceType(), Optional.of(alias));
    this.dataSource = requireNonNull(dataSource, "dataSource");
    this.schema = buildSchema(dataSource);
    this.schemaKStreamFactory = requireNonNull(schemaKStreamFactory, "schemaKStreamFactory");
    this.isWindowed = isWindowed;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public SourceName getAlias() {
    return getSourceName().orElseThrow(IllegalStateException::new);
  }

  public DataSourceType getDataSourceType() {
    return dataSource.getDataSourceType();
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    final String topicName = dataSource.getKsqlTopic().getKafkaTopicName();

    return kafkaTopicClient.describeTopic(topicName)
        .partitions()
        .size();
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of();
  }

  public boolean isWindowed() {
    return isWindowed;
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
    final Stacker contextStacker = buildContext.buildNodeContext(getId().toString());
    return schemaKStreamFactory.create(
        buildContext,
        dataSource,
        contextStacker.push(SOURCE_OP_NAME)
    );
  }

  @Override
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName
  ) {
    if (sourceName.isPresent() && !sourceName.equals(getSourceName())) {
      throw new IllegalArgumentException("Expected alias of " + getAlias()
          + ", but was " + sourceName.get());
    }

    // Note: the 'value' columns include the key columns at this point:
    return orderColumns(getSchema().value(), getSchema());
  }

  @Override
  void validateKeyPresent(final SourceName sinkName, final Projection projection) {
    if (getSchema().key().isEmpty()) {
      // No key column.
      return;
    }

    final List<Column> keys = getSchema().key();

    for (final Column keyCol : keys) {
      final ColumnName keyName = keyCol.name();
      if (!projection.containsExpression(new QualifiedColumnReferenceExp(getAlias(), keyName))
          && !projection.containsExpression(new UnqualifiedColumnReferenceExp(keyName))
      ) {
        throwKeysNotIncludedError(
            sinkName,
            "key column",
            keys.stream()
                .map(Column::name)
                .map(UnqualifiedColumnReferenceExp::new)
                .collect(Collectors.toList())
        );
      }
    }
  }

  @Override
  public Set<ColumnReferenceExp> validateColumns(final RequiredColumns requiredColumns) {
    return requiredColumns.get().stream()
        .filter(this::nonKnownColumn)
        .collect(Collectors.toSet());
  }

  private boolean nonKnownColumn(final ColumnReferenceExp columnRef) {
    if (columnRef.maybeQualifier().isPresent()
        && !columnRef.maybeQualifier().get().equals(getAlias())
    ) {
      return true;
    }

    final ColumnName columnName = columnRef.getColumnName();

    if (SystemColumns.isPseudoColumn(columnName)) {
      return false;
    }

    if (SystemColumns.isWindowBound(columnName)) {
      return !dataSource.getKsqlTopic().getKeyFormat().isWindowed();
    }

    return !dataSource.getSchema()
        .findColumn(columnName)
        .isPresent();
  }

  private static LogicalSchema buildSchema(
      final DataSource dataSource
  ) {
    // DataSourceNode copies implicit and key fields into the value schema
    // It users a KS valueMapper to add the key fields
    // and a KS transformValues to add the implicit fields
    return dataSource.getSchema()
        .withPseudoAndKeyColsInValue(
            dataSource.getKsqlTopic().getKeyFormat().isWindowed());
  }

  @Immutable
  interface SchemaKStreamFactory {

    SchemaKStream<?> create(
        PlanBuildContext buildContext,
        DataSource dataSource,
        QueryContext.Stacker contextStacker
    );
  }
}
