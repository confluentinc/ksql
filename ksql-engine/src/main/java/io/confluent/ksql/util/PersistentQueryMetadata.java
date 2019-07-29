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

import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.QuerySchemas;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import java.util.Map;
import java.util.Objects;
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
  private final String sinkName;
  private final QuerySchemas schemas;
  private final PhysicalSchema resultSchema;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public PersistentQueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final PhysicalSchema schema,
      final Set<String> sourceNames,
      final String sinkName,
      final String executionPlan,
      final QueryId id,
      final DataSourceType dataSourceType,
      final String queryApplicationId,
      final KsqlTopic resultTopic,
      final Topology topology,
      final QuerySchemas schemas,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        statementString,
        kafkaStreams,
        schema.logicalSchema(),
        sourceNames,
        executionPlan,
        dataSourceType,
        queryApplicationId,
        topology,
        streamsProperties,
        overriddenProperties,
        closeCallback);

    this.id = requireNonNull(id, "id");
    this.resultTopic = requireNonNull(resultTopic, "resultTopic");
    this.sinkName = Objects.requireNonNull(sinkName, "sinkName");
    this.schemas = requireNonNull(schemas, "schemas");
    this.resultSchema = requireNonNull(schema, "schema");
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
  }

  public PersistentQueryMetadata copyWith(final Consumer<QueryMetadata> closeCallback) {
    return new PersistentQueryMetadata(this, closeCallback);
  }

  public QueryId getQueryId() {
    return id;
  }

  public KsqlTopic getResultTopic() {
    return resultTopic;
  }

  public String getSinkName() {
    return sinkName;
  }

  public Format getResultTopicFormat() {
    return resultTopic.getValueFormat().getFormat();
  }

  public String getSchemasDescription() {
    return schemas.toString();
  }

  public PhysicalSchema getPhysicalSchema() {
    return resultSchema;
  }
}
