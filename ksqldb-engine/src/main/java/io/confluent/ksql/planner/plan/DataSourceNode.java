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
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKSourceFactory;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
public class DataSourceNode extends PlanNode {

  private static final String SOURCE_OP_NAME = "Source";

  private final DataSource dataSource;
  private final KeyField keyField;
  private final SchemaKStreamFactory schemaKStreamFactory;

  public DataSourceNode(
      final PlanNodeId id,
      final DataSource dataSource,
      final SourceName alias
  ) {
    this(id, dataSource, alias, SchemaKSourceFactory::buildSource);
  }

  DataSourceNode(
      final PlanNodeId id,
      final DataSource dataSource,
      final SourceName alias,
      final SchemaKStreamFactory schemaKStreamFactory
  ) {
    super(id, dataSource.getDataSourceType(), buildSchema(dataSource), Optional.of(alias));
    this.dataSource = requireNonNull(dataSource, "dataSource");

    this.keyField = dataSource.getKeyField()
        .validateKeyExistsIn(getSchema());

    this.schemaKStreamFactory = requireNonNull(schemaKStreamFactory, "schemaKStreamFactory");
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
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

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitDataSourceNode(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final Stacker contextStacker = builder.buildNodeContext(getId().toString());
    return schemaKStreamFactory.create(
        builder,
        dataSource,
        contextStacker.push(SOURCE_OP_NAME),
        keyField
    );
  }

  @Override
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName, final boolean valueOnly
  ) {
    if (sourceName.isPresent() && !sourceName.equals(getSourceName())) {
      throw new IllegalArgumentException("Expected alias of " + getAlias()
          + ", but was " + sourceName.get());
    }

    return valueOnly
        ? getSchema().withoutPseudoAndKeyColsInValue().value().stream().map(Column::name)
        : orderColumns(getSchema().value(), getSchema());
  }

  private static LogicalSchema buildSchema(final DataSource dataSource) {
    // DataSourceNode copies implicit and key fields into the value schema
    // It users a KS valueMapper to add the key fields
    // and a KS transformValues to add the implicit fields
    return dataSource.getSchema()
        .withPseudoAndKeyColsInValue(dataSource.getKsqlTopic().getKeyFormat().isWindowed());
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Stream<ColumnName> orderColumns(
      final List<Column> columns,
      final LogicalSchema schema
  ) {
    // When doing a `select *` key columns should be at the front of the column list
    // but are added at the back during processing for performance reasons.
    // Switch them around here:
    final Stream<Column> keys = columns.stream()
        .filter(c -> schema.isKeyColumn(c.name()));

    final Stream<Column> windowBounds = columns.stream()
        .filter(c -> SystemColumns.isWindowBound(c.name()));

    final Stream<Column> values = columns.stream()
        .filter(c -> !SystemColumns.isWindowBound(c.name()))
        .filter(c -> !SystemColumns.isPseudoColumn(c.name()))
        .filter(c -> !schema.isKeyColumn(c.name()));

    return Streams.concat(keys, windowBounds, values).map(Column::name);
  }

  @Immutable
  interface SchemaKStreamFactory {

    SchemaKStream<?> create(
        KsqlQueryBuilder builder,
        DataSource dataSource,
        QueryContext.Stacker contextStacker,
        KeyField keyField
    );
  }
}
