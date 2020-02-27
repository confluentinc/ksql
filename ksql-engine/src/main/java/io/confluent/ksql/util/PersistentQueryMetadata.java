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

package io.confluent.ksql.util;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

/**
 * Metadata of a persistent query, e.g. {@code CREATE STREAM FOO AS SELECT * FROM BAR;}.
 */
public class PersistentQueryMetadata extends QueryMetadata {

  private final QueryId id;
  private final KsqlTopic resultTopic;
  private final SourceName sinkName;
  private final QuerySchemas schemas;
  private final PhysicalSchema resultSchema;
  private final DataSourceType dataSourceType;
  private final Optional<MaterializationProvider> materializationProvider;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public PersistentQueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final PhysicalSchema schema,
      final Set<SourceName> sourceNames,
      final SourceName sinkName,
      final String executionPlan,
      final QueryId id,
      final DataSourceType dataSourceType,
      final Optional<MaterializationProvider> materializationProvider,
      final String queryApplicationId,
      final KsqlTopic resultTopic,
      final Topology topology,
      final QuerySchemas schemas,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback,
      final long closeTimeout) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        statementString,
        kafkaStreams,
        schema.logicalSchema(),
        sourceNames,
        executionPlan,
        queryApplicationId,
        topology,
        streamsProperties,
        overriddenProperties,
        closeCallback,
        closeTimeout);

    this.id = requireNonNull(id, "id");
    this.resultTopic = requireNonNull(resultTopic, "resultTopic");
    this.sinkName = Objects.requireNonNull(sinkName, "sinkName");
    this.schemas = requireNonNull(schemas, "schemas");
    this.resultSchema = requireNonNull(schema, "schema");
    this.materializationProvider =
        requireNonNull(materializationProvider, "materializationProvider");
    this.dataSourceType = Objects.requireNonNull(dataSourceType, "dataSourceType");
  }

  private PersistentQueryMetadata(
      final PersistentQueryMetadata other,
      final Consumer<QueryMetadata> closeCallback
  ) {
    super(other, closeCallback);
    this.id = other.id;
    this.resultTopic = other.resultTopic;
    this.sinkName = other.sinkName;
    this.schemas = other.schemas;
    this.resultSchema = other.resultSchema;
    this.materializationProvider = other.materializationProvider;
    this.dataSourceType = other.dataSourceType;
  }

  public PersistentQueryMetadata copyWith(final Consumer<QueryMetadata> closeCallback) {
    return new PersistentQueryMetadata(this, closeCallback);
  }

  public DataSourceType getDataSourceType() {
    return dataSourceType;
  }

  public QueryId getQueryId() {
    return id;
  }

  public KsqlTopic getResultTopic() {
    return resultTopic;
  }

  public SourceName getSinkName() {
    return sinkName;
  }

  public Map<String, String> getSchemasDescription() {
    return schemas.getSchemasDescription();
  }

  public String getSchemasString() {
    return schemas.toString();
  }

  public PhysicalSchema getPhysicalSchema() {
    return resultSchema;
  }

  public Optional<Materialization> getMaterialization(
      final QueryId queryId,
      final QueryContext.Stacker contextStacker
  ) {
    return materializationProvider.map(builder -> builder.build(queryId, contextStacker));
  }

  @Override
  public void stop() {
    doClose(false);
  }
}
