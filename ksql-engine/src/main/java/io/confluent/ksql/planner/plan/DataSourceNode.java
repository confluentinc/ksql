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
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

@Immutable
public class DataSourceNode extends PlanNode {

  private static final String SOURCE_OP_NAME = "source";
  private static final String REDUCE_OP_NAME = "reduce";

  private final DataSource<?> dataSource;
  private final String alias;
  private final LogicalSchemaWithMetaAndKeyFields schema;
  private final KeyField keyField;
  private final SchemaKStreamFactory schemaKStreamFactory;

  public DataSourceNode(
      final PlanNodeId id,
      final DataSource<?> dataSource,
      final String alias
  ) {
    this(id, dataSource, alias, SchemaKStream::forSource);
  }

  DataSourceNode(
      final PlanNodeId id,
      final DataSource<?> dataSource,
      final String alias,
      final SchemaKStreamFactory schemaKStreamFactory
  ) {
    super(id, dataSource.getDataSourceType());
    this.dataSource = requireNonNull(dataSource, "dataSource");
    this.alias = requireNonNull(alias, "alias");

    // DataSourceNode copies implicit and key fields into the value schema
    // It users a KS valueMapper to add the key fields
    // and a KS transformValues to add the implicit fields
    this.schema = StreamSource.getSchemaWithMetaAndKeyFields(alias, dataSource.getSchema());

    final Optional<String> keyFieldName = dataSource.getKeyField()
        .withAlias(alias)
        .name();

    this.keyField = KeyField.of(keyFieldName, dataSource.getKeyField().legacy())
        .validateKeyExistsIn(schema.getSchema());

    this.schemaKStreamFactory = requireNonNull(schemaKStreamFactory, "schemaKStreamFactory");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema.getSchema();
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  public DataSource<?> getDataSource() {
    return dataSource;
  }

  String getAlias() {
    return alias;
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
    final SchemaKStream<?> schemaKStream = schemaKStreamFactory.create(
        builder,
        dataSource,
        schema,
        contextStacker.push(SOURCE_OP_NAME),
        timestampIndex(),
        getAutoOffsetReset(builder.getKsqlConfig().getKsqlStreamConfigProps()),
        keyField
    );
    if (getDataSourceType() == DataSourceType.KSTREAM) {
      return schemaKStream;
    }
    final Stacker reduceContextStacker = contextStacker.push(REDUCE_OP_NAME);
    return schemaKStream.toTable(
        dataSource.getKsqlTopic().getKeyFormat(),
        dataSource.getKsqlTopic().getValueFormat(),
        reduceContextStacker,
        builder);
  }

  interface SchemaKStreamFactory {
    SchemaKStream<?> create(
        KsqlQueryBuilder builder,
        DataSource<?> dataSource,
        LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
        QueryContext.Stacker contextStacker,
        int timestampIndex,
        Optional<AutoOffsetReset> offsetReset,
        KeyField keyField
    );
  }

  private int timestampIndex() {
    final LogicalSchema originalSchema = dataSource.getSchema();
    final String timestampField = dataSource.getTimestampExtractionPolicy().timestampField();
    return originalSchema.valueColumnIndex(timestampField)
        .orElse(
            originalSchema.withAlias(alias).valueColumnIndex(timestampField)
                .orElse(-1)
        );
  }

  private static Optional<Topology.AutoOffsetReset> getAutoOffsetReset(
      final Map<String, Object> props) {
    final Object offestReset = props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    if (offestReset == null) {
      return Optional.empty();
    }

    try {
      return Optional.of(AutoOffsetReset.valueOf(offestReset.toString().toUpperCase()));
    } catch (final Exception e) {
      throw new ConfigException(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          offestReset,
          "Unknown value"
      );
    }
  }
}
